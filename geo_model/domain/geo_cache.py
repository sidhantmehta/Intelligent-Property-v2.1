"""Cache-freshness policy: pure decision logic for what "Refresh Geo Data"
needs to (re)fetch. Takes/returns plain data -- no DB session, no HTTP --
so it's unit-testable without a database or network.

geo_model.pipeline is responsible for reading the current cache state out
of SQLite into the shape this module expects, calling these functions, and
then acting on the result (fetching + upserting via a provider).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class CacheKey:
    outcode: str
    category_key: str


def is_stale(fetched_at: dt.datetime | None, staleness_days: int, now: dt.datetime) -> bool:
    """A missing entry (fetched_at is None) is always considered stale."""
    if fetched_at is None:
        return True
    age = now - fetched_at
    return age > dt.timedelta(days=staleness_days)


def keys_needing_refresh(
    outcodes: list[str],
    category_keys: list[str],
    last_fetched_at: dict[CacheKey, dt.datetime],
    staleness_days: int,
    now: dt.datetime,
    force: bool = False,
) -> list[CacheKey]:
    """Every (outcode, category) pair that is missing from the cache, or
    older than ``staleness_days``. If ``force`` is True, every pair is
    included regardless of freshness -- this is what the explicit
    "Refresh Geo Data" action passes; a plain "Run Model" never sets it,
    so a run only ever fetches pairs that are fully missing (see
    keys_missing_only below), never force-refreshing merely-stale ones.
    """
    keys = [CacheKey(o, c) for o in outcodes for c in category_keys]
    if force:
        return keys
    return [k for k in keys if is_stale(last_fetched_at.get(k), staleness_days, now)]


def keys_missing_only(
    outcodes: list[str],
    category_keys: list[str],
    last_fetched_at: dict[CacheKey, dt.datetime],
) -> list[CacheKey]:
    """Pairs with no cache entry at all. Used by run_model(), which should
    never silently re-hit the API for data that's merely stale -- only the
    dedicated refresh action does that."""
    keys = [CacheKey(o, c) for o in outcodes for c in category_keys]
    return [k for k in keys if k not in last_fetched_at]
