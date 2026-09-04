"""Reporting over the api_usage table -- pure queries, no writes. A thin
interface/adapter module (like geo_model.postcodes and
geo_model.artifact_sync), not part of the run/refresh hot path.

This is a LOCAL count of what this pipeline sent and got a response for --
useful to sanity-check against a provider's own dashboard, but not a
substitute for it: it can't see quota consumed outside this codebase (a
manual API test, another integration on the same key), and it doesn't know
the account's actual plan/limit.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from geo_model.data.models import ApiUsage


@dataclass(frozen=True)
class UsageSummary:
    since: dt.datetime | None
    total_calls: int
    by_provider: dict[str, int]
    by_call_type: dict[str, int]
    by_status: dict[int, int]
    first_call_at: dt.datetime | None
    last_call_at: dt.datetime | None


def summarize_usage(session: Session, since: dt.datetime | None = None, provider: str | None = None) -> UsageSummary:
    stmt = select(ApiUsage)
    if since is not None:
        stmt = stmt.where(ApiUsage.called_at >= since)
    if provider is not None:
        stmt = stmt.where(ApiUsage.provider == provider)
    rows = list(session.scalars(stmt))

    by_provider: dict[str, int] = {}
    by_call_type: dict[str, int] = {}
    by_status: dict[int, int] = {}
    for r in rows:
        by_provider[r.provider] = by_provider.get(r.provider, 0) + 1
        by_call_type[r.call_type] = by_call_type.get(r.call_type, 0) + 1
        by_status[r.status_code] = by_status.get(r.status_code, 0) + 1

    called_ats = [r.called_at for r in rows]
    return UsageSummary(
        since=since,
        total_calls=len(rows),
        by_provider=by_provider,
        by_call_type=by_call_type,
        by_status=by_status,
        first_call_at=min(called_ats) if called_ats else None,
        last_call_at=max(called_ats) if called_ats else None,
    )


def current_calendar_month_start(now: dt.datetime | None = None) -> dt.datetime:
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
