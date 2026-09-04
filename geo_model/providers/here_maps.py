"""HERE platform implementation of GeoProvider.

Uses HERE's CURRENT apiKey-based REST APIs -- not the legacy app_id/app_code
"demo" endpoints v2 used (places.demo.api.here.com, route.api.here.com/7.2),
which are retired/deprecated:

  - Geocoding:        https://geocode.search.hereapi.com/v1/geocode
  - Nearby amenities:  https://discover.search.hereapi.com/v1/discover
  - Driving/walking/etc routing: https://router.hereapi.com/v8/routes
  - Public transit routing:       https://transit.router.hereapi.com/v8/routes

Verified end-to-end against a live HERE account (Sep 2026): all four
endpoints, including the transit routing one, return real, sane data.

Every request that gets a response (any status code) is recorded via
get_usage_log() -- geo_model.pipeline persists these to the ApiUsage table
after each run so usage stays reconcilable against HERE's own dashboard.
"""
from __future__ import annotations

import datetime as dt
import time

import requests

from geo_model.logging_setup import get_logger
from geo_model.providers.base import AmenityResult, GeoPoint, GeoProvider, UsageRecord

logger = get_logger(__name__)

GEOCODE_URL = "https://geocode.search.hereapi.com/v1/geocode"
DISCOVER_URL = "https://discover.search.hereapi.com/v1/discover"
ROUTING_URL = "https://router.hereapi.com/v8/routes"
TRANSIT_ROUTING_URL = "https://transit.router.hereapi.com/v8/routes"

# transportMode values HERE's Routing v8 API documents for non-transit modes.
ROUTING_V8_MODES = {"car", "truck", "pedestrian", "bicycle", "scooter", "taxi", "bus"}

_RETRIES = 3
_RETRY_BACKOFF_SECONDS = 2
_TIMEOUT_SECONDS = 15


class HereMapsProvider(GeoProvider):
    name = "here"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("HERE_API_KEY is required to construct HereMapsProvider")
        self._api_key = api_key
        self._session = requests.Session()
        self._usage_log: list[UsageRecord] = []

    def get_usage_log(self) -> list[UsageRecord]:
        return list(self._usage_log)

    def geocode(self, address: str) -> GeoPoint | None:
        params = {"q": address, "apiKey": self._api_key, "limit": 1}
        data = self._get(GEOCODE_URL, params, context=f"geocode({address!r})", call_type="geocode")
        if data is None:
            return None
        items = data.get("items", [])
        if not items:
            logger.warning("No geocode result for address=%r", address)
            return None
        pos = items[0]["position"]
        return GeoPoint(lat=pos["lat"], long=pos["lng"])

    def nearby_amenities(
        self,
        origin: GeoPoint,
        query: str,
        radius_miles: float,
        limit: int,
    ) -> list[AmenityResult]:
        params = {
            "at": f"{origin.lat},{origin.long}",
            "q": query,
            "limit": limit,
            "apiKey": self._api_key,
        }
        context = f"discover(q={query!r}, at={origin.lat},{origin.long})"
        data = self._get(DISCOVER_URL, params, context=context, call_type="discover")
        if data is None:
            return []

        results: list[AmenityResult] = []
        radius_m = radius_miles * 1609.34
        for item in data.get("items", []):
            pos = item.get("position")
            if not pos:
                continue
            distance_m = item.get("distance")
            if distance_m is not None and distance_m > radius_m:
                continue
            results.append(
                AmenityResult(
                    title=item.get("title", ""),
                    address=(item.get("address") or {}).get("label"),
                    lat=pos["lat"],
                    long=pos["lng"],
                    distance_m=distance_m,
                )
            )
        logger.debug("%s -> %d results (radius filter %.0fm)", context, len(results), radius_m)
        return results

    def travel_time_minutes(
        self,
        origin: GeoPoint,
        destination: GeoPoint,
        mode: str,
    ) -> float | None:
        if mode == "publicTransport":
            return self._transit_travel_time(origin, destination)
        if mode not in ROUTING_V8_MODES:
            raise ValueError(f"Unsupported HERE routing mode: {mode!r}")

        params = {
            "transportMode": mode,
            "origin": f"{origin.lat},{origin.long}",
            "destination": f"{destination.lat},{destination.long}",
            "return": "summary",
            "apiKey": self._api_key,
        }
        context = f"route(mode={mode}, {origin.lat},{origin.long} -> {destination.lat},{destination.long})"
        data = self._get(ROUTING_URL, params, context=context, call_type=f"route:{mode}")
        if data is None:
            return None
        return self._minutes_from_routing_v8(data, context)

    def _transit_travel_time(self, origin: GeoPoint, destination: GeoPoint) -> float | None:
        params = {
            "origin": f"{origin.lat},{origin.long}",
            "destination": f"{destination.lat},{destination.long}",
            "apiKey": self._api_key,
        }
        context = f"transit_route({origin.lat},{origin.long} -> {destination.lat},{destination.long})"
        data = self._get(TRANSIT_ROUTING_URL, params, context=context, call_type="transit_route")
        if data is None:
            return None
        try:
            sections = data["routes"][0]["sections"]
            start = sections[0]["departure"]["time"]
            end = sections[-1]["arrival"]["time"]
        except (KeyError, IndexError):
            logger.warning("%s: unexpected transit response shape, no duration extracted", context)
            return None
        try:
            start_dt = dt.datetime.fromisoformat(start)
            end_dt = dt.datetime.fromisoformat(end)
        except ValueError:
            logger.warning("%s: could not parse departure/arrival timestamps", context)
            return None
        return (end_dt - start_dt).total_seconds() / 60.0

    @staticmethod
    def _minutes_from_routing_v8(data: dict, context: str) -> float | None:
        try:
            duration_s = data["routes"][0]["sections"][0]["summary"]["duration"]
        except (KeyError, IndexError):
            logger.warning("%s: unexpected routing response shape, no duration extracted", context)
            return None
        return duration_s / 60.0

    def _get(self, url: str, params: dict, context: str, call_type: str) -> dict | None:
        for attempt in range(1, _RETRIES + 1):
            start = time.monotonic()
            try:
                resp = self._session.get(url, params=params, timeout=_TIMEOUT_SECONDS)
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.info(
                    "%s -> status=%d elapsed_ms=%.0f attempt=%d/%d",
                    context, resp.status_code, elapsed_ms, attempt, _RETRIES,
                )
                # A response -- any status code -- means HERE received and
                # (per their billing model) most likely counted this
                # request. A request that raises below (never got a
                # response) does NOT get recorded -- see UsageRecord.
                self._usage_log.append(
                    UsageRecord(call_type=call_type, status_code=resp.status_code, called_at=dt.datetime.now(dt.timezone.utc))
                )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < _RETRIES:
                    time.sleep(_RETRY_BACKOFF_SECONDS * attempt)
                    continue
                logger.error("%s failed: status=%d body=%s", context, resp.status_code, resp.text[:500])
                return None
            except requests.RequestException as e:
                logger.warning("%s errored on attempt %d/%d: %s", context, attempt, _RETRIES, e)
                if attempt < _RETRIES:
                    time.sleep(_RETRY_BACKOFF_SECONDS * attempt)
                else:
                    logger.error("%s failed after %d attempts: %s", context, _RETRIES, e)
                    return None
        return None
