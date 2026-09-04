"""Ingests HM Land Registry Price Paid Data (raw sale transactions) and the
UK House Price Index (monthly per-local-authority index, for scaling old
sales to today's-equivalent value) -- the two public, free, no-API-key
data sources behind postcode-sector pricing. See
geo_model.domain.pricing for what's actually done with this once ingested.

Like postcodes.py, this is a one-off maintenance operation that both
fetches and writes to the DB itself, rather than being split fetch/write
across layers -- there's no per-sector/per-transaction request to route
through a provider abstraction the way HERE calls are.

Bulk CSV sources (both confirmed reachable and stable, no API key):
- Price Paid Data: one CSV per year, all of England & Wales, no header row.
  https://s3.eu-west-1.amazonaws.com/prod1.publicdata.landregistry.gov.uk/pp-<year>.csv
- UK HPI: one CSV covering every local authority back to 2004, refreshed
  monthly (~35MB, has a header row).
  https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-<yyyy-mm>.csv
  The exact "latest month" filename isn't predictable in advance (it's
  named after the month it covers, published a couple of months later) --
  HPI_FULL_FILE_URL is resolved by scraping the current download-page
  link rather than guessing the filename.
"""
from __future__ import annotations

import csv
import datetime as dt
import re
import urllib.request

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import HpiIndex, PricePaidTransaction
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

PPD_URL_TMPL = "https://s3.eu-west-1.amazonaws.com/prod1.publicdata.landregistry.gov.uk/pp-{}.csv"
HPI_DOWNLOADS_PAGE_TMPL = "https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads-{}"
_TIMEOUT_SECONDS = 120
_USER_AGENT = "Intelligent-Property-v2.1 (research use, see repo README)"

PPD_COLUMNS = [
    "transaction_id", "price", "date", "postcode", "property_type", "old_new",
    "duration", "paon", "saon", "street", "locality", "town", "district", "county",
    "ppd_category", "record_status",
]

# Local authority districts only (England unitary/non-met/met/London-borough
# GSS code prefixes) -- excludes counties, regions, and country-level rows,
# which the same file also carries and which don't match
# PricePaidTransaction.district. See module docstring's AreaCode survey.
_LOCAL_AUTHORITY_PREFIXES = ("E06", "E07", "E08", "E09")

# HM Land Registry district renamed between when some old sale rows were
# recorded and the current UK-HPI file's naming (2021 Northamptonshire
# reorganisation into unitary authorities) -- the only PPD->HPI district
# name mismatch found across our whole scope (see chat history / the
# original cross-check). Extend this if a future re-fetch finds another.
_DISTRICT_ALIASES = {
    "SOUTH NORTHAMPTONSHIRE": "WEST NORTHAMPTONSHIRE",
}


def sector_of(postcode: str) -> str | None:
    """"SW11 1AA" -> "SW11 1". None for a postcode with no space or empty incode."""
    parts = postcode.strip().split(" ")
    if len(parts) != 2 or not parts[1]:
        return None
    return parts[0] + " " + parts[1][0]


def outcode_of(postcode: str) -> str:
    return postcode.strip().split(" ")[0]


def _fetch_lines(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
        for line in resp:
            yield line.decode("utf-8", errors="replace")


def ingest_price_paid_data(session: Session, outcodes: set[str], years: list[int]) -> dict:
    """Downloads each year's PPD CSV and upserts rows whose outcode is in
    ``outcodes``. Returns per-year and total counts."""
    per_year = {}
    total_matched = 0
    for year in years:
        url = PPD_URL_TMPL.format(year)
        logger.info("Fetching Price Paid Data for %d from %s", year, url)
        year_total = 0
        year_matched = 0
        batch: list[dict] = []
        reader = csv.reader(_fetch_lines(url))

        def flush(batch):
            if not batch:
                return
            stmt = sqlite_insert(PricePaidTransaction)
            stmt = stmt.on_conflict_do_update(
                index_elements=["transaction_id"],
                set_={"price": stmt.excluded.price, "date": stmt.excluded.date},
            )
            session.execute(stmt, batch)

        for row in reader:
            year_total += 1
            if len(row) < 16:
                continue
            postcode = row[3].strip()
            outcode = outcode_of(postcode)
            if outcode not in outcodes:
                continue
            sector = sector_of(postcode)
            if sector is None:
                continue
            batch.append(dict(
                transaction_id=row[0].strip("{}"),
                price=int(row[1]),
                date=dt.datetime.strptime(row[2][:10], "%Y-%m-%d").date(),
                postcode=postcode,
                sector=sector,
                outcode=outcode,
                property_type=row[4],
                old_new=row[5],
                duration=row[6],
                district=row[12].upper(),
                ppd_category=row[14],
            ))
            year_matched += 1
            if len(batch) >= 5000:
                flush(batch)
                batch = []
        flush(batch)
        session.commit()
        logger.info("Price Paid Data %d: %d rows scanned, %d matched our scope", year, year_total, year_matched)
        per_year[year] = {"scanned": year_total, "matched": year_matched}
        total_matched += year_matched

    return {"per_year": per_year, "total_matched": total_matched}


def _resolve_hpi_full_file_url() -> str:
    """The UK-HPI-full-file's URL is named after the month it covers and
    published a couple of months later, so it isn't predictable -- try
    the current and previous two months' download pages and scrape the
    actual CSV link off whichever resolves first."""
    today = dt.date.today()
    month = dt.date(today.year, today.month, 1)
    for _ in range(4):
        slug = month.strftime("%B-%Y").lower()
        page_url = HPI_DOWNLOADS_PAGE_TMPL.format(slug)
        try:
            req = urllib.request.Request(page_url, headers={"User-Agent": _USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            m = re.search(r'href="(https://publicdata\.landregistry\.gov\.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-[^"]+\.csv)', html)
            if m:
                return m.group(1)
        except Exception as e:
            logger.info("HPI downloads page %s not found/parseable yet (%s), trying previous month", page_url, e)
        month = (month.replace(day=1) - dt.timedelta(days=1)).replace(day=1)
    raise RuntimeError("Could not resolve the current UK-HPI-full-file URL after trying 4 months back")


def ingest_hpi_index(session: Session) -> dict:
    """Downloads the current UK-HPI-full-file and upserts every local
    authority district row (see _LOCAL_AUTHORITY_PREFIXES) into
    hpi_index. Districts are stored upper-cased to match
    PricePaidTransaction.district; the one known PPD/HPI naming mismatch
    is aliased via _DISTRICT_ALIASES."""
    url = _resolve_hpi_full_file_url()
    logger.info("Fetching UK HPI full file from %s", url)
    reader = csv.DictReader(_fetch_lines(url))
    total = 0
    matched = 0
    batch: list[dict] = []

    def _f(row, key):
        v = row.get(key)
        return float(v) if v not in (None, "") else None

    def flush(batch):
        if not batch:
            return
        stmt = sqlite_insert(HpiIndex)
        stmt = stmt.on_conflict_do_update(
            index_elements=["district", "month"],
            set_={
                "index_all": stmt.excluded.index_all,
                "index_detached": stmt.excluded.index_detached,
                "index_semi": stmt.excluded.index_semi,
                "index_terraced": stmt.excluded.index_terraced,
                "index_flat": stmt.excluded.index_flat,
            },
        )
        session.execute(stmt, batch)

    for row in reader:
        total += 1
        if row["AreaCode"][:3] not in _LOCAL_AUTHORITY_PREFIXES:
            continue
        district = row["RegionName"].upper()
        district = _DISTRICT_ALIASES.get(district, district)
        month = dt.datetime.strptime(row["Date"], "%d/%m/%Y").date().replace(day=1)
        batch.append(dict(
            district=district,
            month=month,
            index_all=_f(row, "Index"),
            index_detached=_f(row, "DetachedIndex"),
            index_semi=_f(row, "SemiDetachedIndex"),
            index_terraced=_f(row, "TerracedIndex"),
            index_flat=_f(row, "FlatIndex"),
        ))
        matched += 1
        if len(batch) >= 5000:
            flush(batch)
            batch = []
    flush(batch)
    session.commit()
    logger.info("UK HPI: %d rows scanned, %d local-authority rows upserted", total, matched)
    return {"scanned": total, "matched": matched}
