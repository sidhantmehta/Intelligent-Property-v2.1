"""Seeds the ``outcodes`` reference table (outcode -> centroid lat/long).

This is a one-off maintenance operation, not part of the run/refresh
pipeline's hot path, so -- unlike geo_model.pipeline -- this module both
fetches (from postcodes.io, a free public UK postcode-lookup API) and
writes to the DB itself rather than being split fetch/write across layers.

v2's own `connector_scraper_data/debug_postcode_outcodes_with_longlat.csv`
is NOT used as a source here: it was checked and contains only one real
data row (a debug/test fixture, not real reference data).
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import Outcode
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

POSTCODES_IO_OUTCODE_URL = "https://api.postcodes.io/outcodes/{}"
_TIMEOUT_SECONDS = 15
_MAX_WORKERS = 10


def load_outcode_list(outcodes_file: Path) -> list[str]:
    """v2's connector_scraper_data/outcodes.txt -- a JSON array of
    {"code": int, "outcode": str} -- still a valid full UK outcode list."""
    with open(outcodes_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [entry["outcode"] for entry in raw]


def _fetch_one(session: requests.Session, outcode: str) -> tuple[str, tuple[float, float] | None]:
    try:
        resp = session.get(POSTCODES_IO_OUTCODE_URL.format(outcode), timeout=_TIMEOUT_SECONDS)
    except requests.RequestException as e:
        logger.error("postcodes.io lookup failed for outcode=%s: %s", outcode, e)
        return outcode, None

    if resp.status_code == 404:
        logger.warning("postcodes.io has no data for outcode=%s", outcode)
        return outcode, None
    if resp.status_code != 200:
        logger.error("postcodes.io lookup failed for outcode=%s: status=%d body=%s", outcode, resp.status_code, resp.text[:300])
        return outcode, None

    data = resp.json()["result"]
    return outcode, (data["latitude"], data["longitude"])


def fetch_outcode_centroids(outcodes: list[str]) -> dict[str, tuple[float, float]]:
    """Looks up (lat, long) for each outcode via postcodes.io's per-outcode
    GET endpoint -- postcodes.io has no bulk lookup for outcodes (only for
    full postcodes), so this is one request per outcode, parallelized with
    a small thread pool. Outcodes postcodes.io doesn't recognise are simply
    absent from the result (logged, not raised)."""
    session = requests.Session()
    result: dict[str, tuple[float, float]] = {}

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, session, o): o for o in outcodes}
        for i, future in enumerate(as_completed(futures), start=1):
            outcode, centroid = future.result()
            if centroid is not None:
                result[outcode] = centroid
            if i % 100 == 0 or i == len(outcodes):
                logger.info("postcodes.io: resolved %d/%d outcodes so far (%d looked up)", len(result), len(outcodes), i)

    return result


def seed_outcodes_table(session: Session, outcodes_file: Path, outcode_filter: list[str] | None = None) -> int:
    """Fetches centroids for every outcode in ``outcodes_file`` (or, if
    ``outcode_filter`` is given, just that subset of it) and upserts them
    into the outcodes table. Returns the number seeded."""
    outcode_list = load_outcode_list(outcodes_file)
    if outcode_filter:
        wanted = set(outcode_filter)
        outcode_list = [o for o in outcode_list if o in wanted]
    logger.info("Seeding outcodes table from %d outcodes in %s", len(outcode_list), outcodes_file)
    centroids = fetch_outcode_centroids(outcode_list)

    for outcode, (lat, long_) in centroids.items():
        stmt = sqlite_insert(Outcode).values(outcode=outcode, lat=lat, long=long_)
        stmt = stmt.on_conflict_do_update(index_elements=["outcode"], set_={"lat": lat, "long": long_})
        session.execute(stmt)

    logger.info("Seeded %d/%d outcodes (%d not resolved by postcodes.io)", len(centroids), len(outcode_list), len(outcode_list) - len(centroids))
    return len(centroids)
