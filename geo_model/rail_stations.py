"""Ingests National Rail station locations from NaPTAN (National Public
Transport Access Nodes) -- the free, no-signup UK government dataset of
every public transport access point in England, Scotland and Wales.

This is the geography half of the "nearest station" feature; NaPTAN has
no fare data and no CRS codes (its own identifier, ``atco_code``, is a
NaPTAN-specific code, e.g. "9100PADTON") -- once the fares feed is wired
up (see the project's ATOC/RSP fares plan), its own station reference
list gets matched to these rows by name/location, not a shared code.

Like postcodes.py/price_data.py, this both fetches and writes to the DB
itself rather than being split fetch/write across layers -- a one-off
national download, not a per-item request routed through a provider.
"""
from __future__ import annotations

import csv
import urllib.request
from pathlib import Path

from sqlalchemy import delete
from sqlalchemy.orm import Session

from geo_model.data.models import RailStation
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

NAPTAN_RAIL_STATIONS_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"
_TIMEOUT_SECONDS = 300
_USER_AGENT = "Intelligent-Property-v2.1 (research use, see repo README)"

# NaPTAN's StopType for a National Rail station's own access-node entry
# (as opposed to "RSE" station entrances, "PLT" platforms, or the many
# bus/coach/ferry/metro types the same national file also carries).
_RAIL_STOP_TYPE = "RLY"


def download_naptan_csv(dest: Path) -> Path:
    """Downloads the full national NaPTAN access-nodes CSV (~100MB, every
    stop type) to ``dest``. Separate from ingest_rail_stations() so a
    caller can download once and re-run ingestion against the same file."""
    req = urllib.request.Request(NAPTAN_RAIL_STATIONS_URL, headers={"User-Agent": _USER_AGENT})
    logger.info("Downloading NaPTAN access-nodes CSV to %s", dest)
    with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp, open(dest, "wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    return dest


def ingest_rail_stations(session: Session, csv_path: Path) -> dict:
    """Reads the NaPTAN CSV at ``csv_path`` (already downloaded -- see
    download_naptan_csv()), keeps only active National Rail station
    entries (StopType == "RLY", Status == "active"), and upserts them
    into rail_stations. No outcode/sector scoping -- the whole GB station
    list is a small, fixed dataset (~2,700 rows), not worth filtering."""
    total = 0
    kept = 0
    # A large interchange (Clapham Junction, Victoria, London Bridge...)
    # gets more than one NaPTAN "RLY" row -- one per access point/operator
    # area, all sharing the same CommonName. Left as separate rows, a
    # sector near one of these would see its "5 closest stations" filled
    # with 5 near-duplicates of the same station instead of 5 real
    # alternatives -- so dedupe by name here, averaging lat/long across an
    # interchange's access points into one representative point.
    by_name: dict[str, list[tuple[str, float, float]]] = {}

    with open(csv_path, encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            if row.get("StopType") != _RAIL_STOP_TYPE or row.get("Status") != "active":
                continue
            name = (row.get("CommonName") or "").strip()
            lat_raw, long_raw = (row.get("Latitude") or "").strip(), (row.get("Longitude") or "").strip()
            if not name or not lat_raw or not long_raw:
                continue
            try:
                lat, long_ = float(lat_raw), float(long_raw)
            except ValueError:
                continue
            by_name.setdefault(name, []).append((row["ATCOCode"], lat, long_))
            kept += 1

    batch = []
    for name, entries in by_name.items():
        representative_atco = min(code for code, _, _ in entries)  # stable across re-ingests
        avg_lat = sum(lat for _, lat, _ in entries) / len(entries)
        avg_long = sum(long_ for _, _, long_ in entries) / len(entries)
        batch.append(dict(atco_code=representative_atco, name=name, lat=avg_lat, long=avg_long))

    # Delete-then-insert rather than upsert: which atco_code ends up
    # "representative" for a deduped name can shift between re-ingests
    # (if NaPTAN adds/removes an access-point row), so upserting by
    # atco_code alone would leave a stale row behind under the code that
    # was representative last time.
    session.execute(delete(RailStation))
    for i in range(0, len(batch), 2000):
        session.execute(RailStation.__table__.insert(), batch[i:i + 2000])
    session.commit()

    logger.info(
        "NaPTAN ingest: %d rows scanned, %d active rail access-node rows, deduped to %d distinct stations",
        total, kept, len(batch),
    )
    return {"scanned": total, "active_rows": kept, "stations": len(batch)}
