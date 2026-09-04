"""Shared logic for importing a curated school register CSV (private
schools, grammar schools, and any future one) into its own table and
geocoding it once via postcodes.io.

Every register follows the same shape: name, address, postcode, plus a
handful of register-specific attribute columns that are copied through
verbatim. That commonality lives here so each register's own module
(geo_model.private_schools, geo_model.grammar_schools) is just its CSV
column list and a call into import_school_register() -- not a second copy
of the geocode-then-upsert logic.

One-off maintenance operations, like geo_model.postcodes: they both fetch
(postcodes.io) and write rather than being split across layers, since
they're not part of the run/refresh pipeline's hot path.
"""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path
from typing import Callable

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.logging_setup import get_logger
from geo_model.postcodes import fetch_postcode_centroids

logger = get_logger(__name__)


def load_register_csv(path: Path, field_converters: dict[str, Callable] | None = None) -> list[dict]:
    """Reads a school-register CSV. ``field_converters`` maps a column name
    to a function applied to non-empty values (e.g. {"size": int})."""
    field_converters = field_converters or {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        for field, convert in field_converters.items():
            row[field] = convert(row[field]) if row.get(field) else None
    return rows


def import_school_register(
    session: Session,
    csv_path: Path,
    model_cls,
    extra_fields: list[str],
    field_converters: dict[str, Callable] | None = None,
) -> dict:
    """Loads ``csv_path`` (name/address/postcode + whatever ``extra_fields``
    names), geocodes every distinct postcode once via postcodes.io, and
    upserts into ``model_cls`` keyed on (name, postcode). ``extra_fields``
    must match column names on both the CSV and the model 1:1."""
    schools = load_register_csv(csv_path, field_converters)
    logger.info("Loaded %d rows from %s for %s", len(schools), csv_path, model_cls.__tablename__)

    postcodes = sorted({s["postcode"] for s in schools if s.get("postcode")})
    centroids = fetch_postcode_centroids(postcodes)
    logger.info("Geocoded %d/%d distinct postcodes", len(centroids), len(postcodes))

    now = dt.datetime.now(dt.timezone.utc)
    geocoded_count = 0
    for s in schools:
        centroid = centroids.get(s["postcode"])
        if centroid is not None:
            geocoded_count += 1
        lat, long_ = centroid if centroid else (None, None)

        values = {
            "name": s["name"],
            "address": s["address"],
            "postcode": s["postcode"],
            "lat": lat,
            "long": long_,
            "geocoded_at": now if centroid else None,
            **{field: s.get(field) or None for field in extra_fields},
        }
        stmt = sqlite_insert(model_cls).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["name", "postcode"],
            set_={k: v for k, v in values.items() if k not in ("name", "postcode")},
        )
        session.execute(stmt)

    not_geocoded = len(schools) - geocoded_count
    if not_geocoded:
        logger.warning("%d/%d rows could not be geocoded (postcode not resolved)", not_geocoded, len(schools))
    logger.info("Imported %d rows into %s (%d geocoded)", len(schools), model_cls.__tablename__, geocoded_count)
    return {"total": len(schools), "geocoded": geocoded_count, "not_geocoded": not_geocoded}
