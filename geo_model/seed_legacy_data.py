"""One-off import of v2's reference_data/*.txt (real HERE Places results
scraped in Feb 2019, UK-wide, ~700k rows total) into the new amenities
cache, so a first "Run Model" doesn't have to fetch everything from HERE
cold.

Imported rows are flagged ``is_seed=True`` and given a fixed, deliberately
old ``fetched_at`` (matching the data's actual vintage) so
geo_model.domain.geo_cache treats them as *present* (run_model won't
re-fetch them) but *stale* (refresh_geo_data will re-validate/replace them
the first time it's run for that outcode/category) -- exactly the "already
have data, but it should eventually be double-checked" status this data
actually has.

v2's files mix many HERE Places categories per file (each file's `cat=`
request apparently returned neighbouring categories too, not just its
namesake), so this only imports the categories that map unambiguously onto
v3's config.yaml amenity categories; everything else in the file is
skipped. Categories with no legacy source at all (gyms, supermarkets) are
simply left for the first real run_model()/refresh_geo_data() to fetch.
"""
from __future__ import annotations

import ast
import datetime as dt
from pathlib import Path

import pandas as pd
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import Amenity, Outcode
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

SEED_FETCHED_AT = dt.datetime(2019, 2, 4, tzinfo=dt.timezone.utc)
PROVIDER = "here"

# legacy filename -> {legacy HERE category label -> v3 category_key}
# Verified against the actual distinct category values present in each
# file (not guessed) -- see the categories NOT listed here (e.g. every
# generic "shop"/"business-services"/etc label) are intentionally skipped.
FILE_CATEGORY_MAP: dict[str, dict[str, str]] = {
    "eat-drink.txt": {
        "coffee-tea": "cafes",
        "restaurant": "restaurants",
        "bar-pub": "pubs_bars",
        "education-facility": "schools",
    },
    "recreation.txt": {
        "leisure-outdoor": "parks_recreation",
        "education-facility": "schools",
    },
    "public-transport.txt": {
        "public-transport": "public_transport",
        "railway-station": "railway",
    },
    "railway-station.txt": {
        "railway-station": "railway",
    },
    "airport.txt": {
        "airport": "airport",
    },
}


def _parse_position(position: str) -> tuple[float, float] | None:
    try:
        lat, long_ = ast.literal_eval(position)
        return float(lat), float(long_)
    except (ValueError, SyntaxError, TypeError):
        return None


def _load_file_rows(path: Path, category_map: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", index_col=0, dtype=str)
    df = df[df["category"].isin(category_map.keys())]
    if df.empty:
        return df
    df["category_key"] = df["category"].map(category_map)
    parsed = df["position"].map(_parse_position)
    df = df[parsed.notna()]
    df["lat"] = [p[0] for p in parsed if p is not None]
    df["long"] = [p[1] for p in parsed if p is not None]
    df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")
    return df


def seed_amenities_from_legacy_data(session: Session, reference_data_dir: Path) -> dict[str, int]:
    known_outcodes = {row.outcode for row in session.query(Outcode.outcode)}
    if not known_outcodes:
        logger.warning("outcodes table is empty -- seed rows for unknown outcodes will still be skipped. Run geo_model.postcodes seeding first.")

    counts: dict[str, int] = {}
    for filename, category_map in FILE_CATEGORY_MAP.items():
        path = reference_data_dir / filename
        if not path.exists():
            logger.warning("Legacy seed file not found, skipping: %s", path)
            continue

        df = _load_file_rows(path, category_map)
        df = df[df["postcode"].isin(known_outcodes)] if known_outcodes else df
        logger.info("Seeding from %s: %d usable rows across categories %s", filename, len(df), sorted(set(category_map.values())))

        rows = [
            {
                "outcode": r["postcode"],
                "provider": PROVIDER,
                "category_key": r["category_key"],
                "title": r["title"],
                "address": r.get("address"),
                "lat": r["lat"],
                "long": r["long"],
                "distance_m": r["distance_m"] if pd.notna(r["distance_m"]) else None,
                "fetched_at": SEED_FETCHED_AT,
                "is_seed": True,
            }
            for _, r in df.iterrows()
        ]

        inserted = 0
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            stmt = sqlite_insert(Amenity).values(batch).on_conflict_do_nothing(
                index_elements=["outcode", "provider", "category_key", "title", "lat", "long"]
            )
            result = session.execute(stmt)
            inserted += result.rowcount or 0
        session.commit()

        for category_key in set(category_map.values()):
            counts[category_key] = counts.get(category_key, 0) + int((df["category_key"] == category_key).sum())
        logger.info("Seeded %d new rows from %s (some may have been duplicates already present)", inserted, filename)

    return counts
