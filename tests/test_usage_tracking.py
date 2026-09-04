import datetime as dt
from pathlib import Path

import pytest

from geo_model.data.db import get_session, init_db
from geo_model.providers.base import GeoPoint
from geo_model.providers.here_maps import HereMapsProvider
from geo_model.usage_report import current_calendar_month_start, summarize_usage


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


def _provider_with_fake_get(monkeypatch, responses):
    """responses: list of _FakeResponse, consumed in order across all calls."""
    provider = HereMapsProvider(api_key="test-key")
    calls = iter(responses)

    def fake_get(self, url, params=None, timeout=None):
        return next(calls)

    monkeypatch.setattr("requests.Session.get", fake_get)
    return provider


def test_here_provider_records_usage_on_success(monkeypatch):
    provider = _provider_with_fake_get(monkeypatch, [
        _FakeResponse(200, {"items": [{"position": {"lat": 51.5, "lng": -0.1}}]}),
    ])
    provider.geocode("some address")
    log = provider.get_usage_log()
    assert len(log) == 1
    assert log[0].call_type == "geocode"
    assert log[0].status_code == 200


def test_here_provider_records_every_retry_attempt(monkeypatch):
    # Two failing attempts (retryable status) then a success -- all three
    # should be recorded, since each one got a real response from HERE.
    provider = _provider_with_fake_get(monkeypatch, [
        _FakeResponse(503, {}),
        _FakeResponse(503, {}),
        _FakeResponse(200, {"items": []}),
    ])
    monkeypatch.setattr("time.sleep", lambda *_: None)
    provider.nearby_amenities(GeoPoint(lat=51.5, long=-0.1), "cafe", radius_miles=1, limit=5)
    log = provider.get_usage_log()
    assert [r.status_code for r in log] == [503, 503, 200]
    assert all(r.call_type == "discover" for r in log)


def test_here_provider_does_not_record_network_failure(monkeypatch):
    import requests

    provider = HereMapsProvider(api_key="test-key")

    def fake_get(self, url, params=None, timeout=None):
        raise requests.ConnectionError("blocked")

    monkeypatch.setattr("requests.Session.get", fake_get)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    result = provider.geocode("some address")
    assert result is None
    assert provider.get_usage_log() == []


def test_summarize_usage(tmp_path):
    db_path = tmp_path / "usage_test.db"
    init_db(db_path)
    now = dt.datetime.now(dt.timezone.utc)

    from geo_model.data.models import ApiUsage

    with get_session() as session:
        session.add(ApiUsage(provider="here", call_type="discover", status_code=200, run_id="r1", called_at=now))
        session.add(ApiUsage(provider="here", call_type="discover", status_code=200, run_id="r1", called_at=now))
        session.add(ApiUsage(provider="here", call_type="geocode", status_code=404, run_id="r1", called_at=now - dt.timedelta(days=40)))

    with get_session() as session:
        summary = summarize_usage(session)
        assert summary.total_calls == 3
        assert summary.by_call_type == {"discover": 2, "geocode": 1}
        assert summary.by_status == {200: 2, 404: 1}

        recent = summarize_usage(session, since=now - dt.timedelta(days=1))
        assert recent.total_calls == 2


def test_current_calendar_month_start():
    now = dt.datetime(2026, 9, 15, 12, 30, tzinfo=dt.timezone.utc)
    start = current_calendar_month_start(now)
    assert start == dt.datetime(2026, 9, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
