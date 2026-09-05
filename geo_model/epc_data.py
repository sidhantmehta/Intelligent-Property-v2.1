"""Ingests EPC (Energy Performance Certificate) domestic floor-area data
-- the source behind postcode-sector £-per-m² pricing. See
geo_model.domain.floor_area for what's done with this once ingested.

Unlike Price Paid Data and the UK HPI, this source needs a free API key
(EPC_API_KEY) from https://get-energy-performance-data.communities.gov.uk/
-- register there, then set EPC_API_KEY in .env. The bulk "full load"
download (GET /api/files/domestic/csv) is a ~8GB zip of one
certificates-<year>.csv per year, covering every domestic EPC/DEC ever
lodged in England & Wales; there's also a much bigger recommendations-
<year>.csv per year in the same zip (retrofit recommendations, not
floor area) which this module never reads. Real column names/values
confirmed directly against a downloaded file rather than assumed --
see the property-type mapping below.

Like postcodes.py and price_data.py, this both fetches and writes to
the DB itself rather than being split fetch/write across layers.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path

from sqlalchemy import update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import EpcCertificate, PostcodeSector
from geo_model.logging_setup import get_logger
from geo_model.price_data import outcode_of, sector_of

logger = get_logger(__name__)

EPC_FULL_LOAD_CSV_URL = "https://api.get-energy-performance-data.communities.gov.uk/api/files/domestic/csv"
_TIMEOUT_SECONDS = 300
_USER_AGENT = "Intelligent-Property-v2.1 (research use, see repo README)"

# EPC's property_type/built_form -> our D/S/T/F scheme (matches Land
# Registry's PricePaidTransaction.property_type categories). "Park home"
# has no equivalent and is dropped, same as PPD's "O" (other) category
# being outside PROPERTY_TYPES. A blank/unrecognised built_form (e.g. a
# House with no built_form on record) is also dropped rather than guessed.
_FLAT_TYPES = {"Flat", "Maisonette"}
_BUILT_FORM_TO_TYPE = {
    "Detached": "D",
    "Semi-Detached": "S",
    "End-Terrace": "T",
    "Mid-Terrace": "T",
    "Enclosed End-Terrace": "T",
    "Enclosed Mid-Terrace": "T",
}

# Only these entries in the zip carry floor area; `recommendations-*.csv`
# (much larger, retrofit suggestions) are never opened.
_CERTIFICATE_YEARS = range(2008, 2027)


def _property_type_of(property_type: str, built_form: str) -> str | None:
    if property_type in _FLAT_TYPES:
        return "F"
    return _BUILT_FORM_TO_TYPE.get(built_form)


def _dwelling_key(uprn: str, address: str, postcode: str) -> str | None:
    uprn = (uprn or "").strip()
    if uprn:
        return "uprn:" + uprn
    address = (address or "").strip().upper()
    if not address:
        return None
    return "addr:" + address + "|" + postcode


def download_full_load_csv(dest: Path) -> Path:
    """Downloads the current domestic full-load CSV zip (~8GB) to
    ``dest``. Separate from ingest_epc_data() so a caller can download
    once and re-run ingestion against the same file while iterating."""
    req = urllib.request.Request(
        EPC_FULL_LOAD_CSV_URL,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
    )
    api_key = os.environ.get("EPC_API_KEY")
    if not api_key:
        raise RuntimeError("EPC_API_KEY is not set -- see .env.example")
    req.add_header("Authorization", f"Bearer {api_key}")
    logger.info("Downloading EPC domestic full-load CSV to %s", dest)
    with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp, open(dest, "wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    return dest


def ingest_epc_data(session: Session, outcodes: set[str], zip_path: Path) -> dict:
    """Reads certificates-<year>.csv entries out of the full-load zip at
    ``zip_path`` (already downloaded -- see download_full_load_csv()),
    keeps rows whose outcode is in ``outcodes``, and upserts one row per
    dwelling into epc_certificates -- deduped by dwelling_key, keeping
    whichever record has the latest lodgement_date if the same dwelling
    was assessed more than once (EPC_CERTIFICATES has no natural primary
    key across the whole file the way PPD's transaction_id does).

    Also derives each sector's Royal Mail post town (e.g. "Gerrards
    Cross") from the certificates' ``posttown`` column -- the most common
    value among that sector's dwellings -- and writes it straight to
    postcode_sectors.town. This is computed transiently from the CSV scan
    rather than stored per-dwelling: nothing else needs post town at
    dwelling grain, so persisting it on every one of ~6M epc_certificates
    rows would be pure overhead."""
    per_year: dict[int, dict] = {}
    total_matched = 0
    # Newest-first within a dwelling: the batched upsert below applies
    # "last write wins" per flush, so processing rows oldest-year-first
    # and using a plain REPLACE-style upsert would let an OLDER cert
    # overwrite a newer one if they land in different flush batches out
    # of order. Instead track the best (latest lodgement_date) row per
    # dwelling_key in memory across the whole scan, then write once.
    best_by_dwelling: dict[str, dict] = {}
    # Parallel to best_by_dwelling, keyed the same way -- kept separate
    # since post_town isn't an EpcCertificate column (see docstring above).
    town_by_dwelling: dict[str, str] = {}

    with zipfile.ZipFile(zip_path) as zf:
        names = {n for n in zf.namelist() if n.startswith("certificates-")}
        for year in _CERTIFICATE_YEARS:
            name = f"certificates-{year}.csv"
            if name not in names:
                continue
            year_total = 0
            year_matched = 0
            with zf.open(name) as raw:
                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))
                for row in reader:
                    year_total += 1
                    postcode = (row.get("postcode") or "").strip()
                    if not postcode:
                        continue
                    outcode = outcode_of(postcode)
                    if outcode not in outcodes:
                        continue
                    sector = sector_of(postcode)
                    if sector is None:
                        continue
                    ptype = _property_type_of(row.get("property_type", ""), row.get("built_form", ""))
                    if ptype is None:
                        continue
                    floor_area_raw = (row.get("total_floor_area") or "").strip()
                    if not floor_area_raw:
                        continue
                    try:
                        floor_area = float(floor_area_raw)
                    except ValueError:
                        continue
                    if floor_area <= 0:
                        continue
                    key = _dwelling_key(row.get("uprn", ""), row.get("address", ""), postcode)
                    if key is None:
                        continue
                    lodgement_date = dt.datetime.strptime(row["lodgement_date"][:10], "%Y-%m-%d").date()

                    existing = best_by_dwelling.get(key)
                    if existing is not None and existing["lodgement_date"] >= lodgement_date:
                        continue
                    best_by_dwelling[key] = dict(
                        dwelling_key=key,
                        postcode=postcode,
                        sector=sector,
                        outcode=outcode,
                        property_type=ptype,
                        total_floor_area_m2=floor_area,
                        lodgement_date=lodgement_date,
                    )
                    post_town = (row.get("posttown") or "").strip()
                    if post_town:
                        town_by_dwelling[key] = post_town
                    else:
                        town_by_dwelling.pop(key, None)
                    year_matched += 1
            logger.info("EPC %s: %d rows scanned, %d matched our scope", name, year_total, year_matched)
            per_year[year] = {"scanned": year_total, "matched": year_matched}
            total_matched += year_matched

    batch = list(best_by_dwelling.values())
    for i in range(0, len(batch), 5000):
        chunk = batch[i:i + 5000]
        stmt = sqlite_insert(EpcCertificate)
        stmt = stmt.on_conflict_do_update(
            index_elements=["dwelling_key"],
            set_={
                "total_floor_area_m2": stmt.excluded.total_floor_area_m2,
                "lodgement_date": stmt.excluded.lodgement_date,
            },
        )
        session.execute(stmt, chunk)
    session.commit()

    sector_town_votes: dict[str, Counter] = {}
    for dwelling_key, cert in best_by_dwelling.items():
        town = town_by_dwelling.get(dwelling_key)
        if not town:
            continue
        sector_town_votes.setdefault(cert["sector"], Counter())[town] += 1
    dominant_town_by_sector = {
        sector: votes.most_common(1)[0][0] for sector, votes in sector_town_votes.items()
    }
    for sector, town in dominant_town_by_sector.items():
        session.execute(update(PostcodeSector).where(PostcodeSector.sector == sector).values(town=town))
    session.commit()

    logger.info("EPC ingest: %d unique dwellings in scope out of %d scanned rows", len(best_by_dwelling), total_matched)
    logger.info("EPC ingest: derived post town for %d sectors", len(dominant_town_by_sector))
    return {
        "per_year": per_year,
        "total_scanned_matched": total_matched,
        "unique_dwellings": len(best_by_dwelling),
        "sectors_with_town": len(dominant_town_by_sector),
    }
