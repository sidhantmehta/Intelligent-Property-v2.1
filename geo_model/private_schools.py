"""Imports the private-schools register (reference_data/private_schools_
greater_london.csv) into the private_schools table, geocoding each school's
postcode once via postcodes.io (free, precise to the postcode -- no HERE
calls needed since we already have a real postcode, not just a free-text
address to resolve).

This is a one-off maintenance operation, like geo_model.postcodes: it both
fetches and writes rather than being split across layers, since it isn't
part of the run/refresh pipeline's hot path.

The CSV was produced once from a scraped source spreadsheet (variable-
length blocks per school, delimited by Start/End markers, with blank
fields sometimes causing the next label to be misread as this field's
value -- see the school register's own notes for the parsing logic) --
this module just consumes the already-cleaned CSV.
"""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import PrivateSchool
from geo_model.logging_setup import get_logger
from geo_model.postcodes import fetch_postcode_centroids

logger = get_logger(__name__)


def load_schools_csv(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        row["size"] = int(row["size"]) if row.get("size") else None
    return rows


def import_private_schools(session: Session, csv_path: Path) -> dict:
    schools = load_schools_csv(csv_path)
    logger.info("Loaded %d schools from %s", len(schools), csv_path)

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

        stmt = sqlite_insert(PrivateSchool).values(
            name=s["name"],
            address=s["address"],
            postcode=s["postcode"],
            lat=lat,
            long=long_,
            phone=s.get("phone") or None,
            gender_profile=s.get("gender_profile") or None,
            size=s.get("size"),
            day_boarding_type=s.get("day_boarding_type") or None,
            religious_affiliation=s.get("religious_affiliation") or None,
            geocoded_at=now if centroid else None,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["name", "postcode"],
            set_={
                "address": s["address"],
                "lat": lat,
                "long": long_,
                "phone": s.get("phone") or None,
                "gender_profile": s.get("gender_profile") or None,
                "size": s.get("size"),
                "day_boarding_type": s.get("day_boarding_type") or None,
                "religious_affiliation": s.get("religious_affiliation") or None,
                "geocoded_at": now if centroid else None,
            },
        )
        session.execute(stmt)

    not_geocoded = len(schools) - geocoded_count
    if not_geocoded:
        logger.warning("%d/%d schools could not be geocoded (postcode not resolved)", not_geocoded, len(schools))
    logger.info("Imported %d schools (%d geocoded)", len(schools), geocoded_count)
    return {"total": len(schools), "geocoded": geocoded_count, "not_geocoded": not_geocoded}
