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
import time
from pathlib import Path

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import Outcode
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

POSTCODES_IO_BULK_OUTCODES_URL = "https://api.postcodes.io/outcodes"
_BATCH_SIZE = 100
_TIMEOUT_SECONDS = 15


def load_outcode_list(outcodes_file: Path) -> list[str]:
    """v2's connector_scraper_data/outcodes.txt -- a JSON array of
    {"code": int, "outcode": str} -- still a valid full UK outcode list."""
    with open(outcodes_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [entry["outcode"] for entry in raw]


def fetch_outcode_centroids(outcodes: list[str]) -> dict[str, tuple[float, float]]:
    """Looks up (lat, long) for each outcode via postcodes.io's bulk
    /outcodes endpoint, batched. Outcodes postcodes.io doesn't recognise
    are simply absent from the result (logged, not raised)."""
    session = requests.Session()
    result: dict[str, tuple[float, float]] = {}

    for i in range(0, len(outcodes), _BATCH_SIZE):
        batch = outcodes[i : i + _BATCH_SIZE]
        try:
            resp = session.post(
                POSTCODES_IO_BULK_OUTCODES_URL,
                json={"outcodes": batch},
                timeout=_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            logger.error("postcodes.io batch %d-%d failed: %s", i, i + len(batch), e)
            continue

        if resp.status_code != 200:
            logger.error("postcodes.io batch %d-%d failed: status=%d body=%s", i, i + len(batch), resp.status_code, resp.text[:300])
            continue

        for entry in resp.json().get("result", []):
            query = entry.get("query")
            data = entry.get("result")
            if data is None:
                logger.warning("postcodes.io has no data for outcode=%r", query)
                continue
            result[data["outcode"]] = (data["latitude"], data["longitude"])

        logger.info("postcodes.io: resolved %d/%d outcodes so far", len(result), i + len(batch))
        time.sleep(0.2)  # be a polite citizen of a free public API

    return result


def seed_outcodes_table(session: Session, outcodes_file: Path) -> int:
    """Fetches centroids for every outcode in ``outcodes_file`` and
    upserts them into the outcodes table. Returns the number seeded."""
    outcode_list = load_outcode_list(outcodes_file)
    logger.info("Seeding outcodes table from %d outcodes in %s", len(outcode_list), outcodes_file)
    centroids = fetch_outcode_centroids(outcode_list)

    for outcode, (lat, long_) in centroids.items():
        stmt = sqlite_insert(Outcode).values(outcode=outcode, lat=lat, long=long_)
        stmt = stmt.on_conflict_do_update(index_elements=["outcode"], set_={"lat": lat, "long": long_})
        session.execute(stmt)

    logger.info("Seeded %d/%d outcodes (%d not resolved by postcodes.io)", len(centroids), len(outcode_list), len(outcode_list) - len(centroids))
    return len(centroids)
