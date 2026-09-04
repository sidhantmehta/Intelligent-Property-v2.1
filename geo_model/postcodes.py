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
from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.data.models import Outcode, PostcodeSector, PricePaidTransaction
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

POSTCODES_IO_OUTCODE_URL = "https://api.postcodes.io/outcodes/{}"
POSTCODES_IO_BULK_POSTCODES_URL = "https://api.postcodes.io/postcodes"
_TIMEOUT_SECONDS = 15
_MAX_WORKERS = 10
_POSTCODE_BATCH_SIZE = 100


def load_outcode_list(outcodes_file: Path) -> list[str]:
    """v2's connector_scraper_data/outcodes.txt -- a JSON array of
    {"code": int, "outcode": str} -- still a valid full UK outcode list."""
    with open(outcodes_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [entry["outcode"] for entry in raw]


def _fetch_outcode_result(session: requests.Session, outcode: str) -> dict | None:
    try:
        resp = session.get(POSTCODES_IO_OUTCODE_URL.format(outcode), timeout=_TIMEOUT_SECONDS)
    except requests.RequestException as e:
        logger.error("postcodes.io lookup failed for outcode=%s: %s", outcode, e)
        return None

    if resp.status_code == 404:
        logger.warning("postcodes.io has no data for outcode=%s", outcode)
        return None
    if resp.status_code != 200:
        logger.error("postcodes.io lookup failed for outcode=%s: status=%d body=%s", outcode, resp.status_code, resp.text[:300])
        return None

    return resp.json()["result"]


def _fetch_one(session: requests.Session, outcode: str) -> tuple[str, tuple[float, float] | None]:
    data = _fetch_outcode_result(session, outcode)
    if data is None:
        return outcode, None
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


# The 33 London boroughs (32 boroughs + the City of London), split into the
# 12-plus-City "Inner London" statistical area and the remaining "Outer"
# boroughs -- fixed, well-known lists, not derived from any API. Matched
# against postcodes.io's admin_district names (which is why "City of
# London" appears here rather than "Corporation of London" or similar).
INNER_LONDON_BOROUGHS = {
    "Camden", "Greenwich", "Hackney", "Hammersmith and Fulham", "Islington",
    "Kensington and Chelsea", "Lambeth", "Lewisham", "Southwark",
    "Tower Hamlets", "Wandsworth", "Westminster", "City of London",
}
OUTER_LONDON_BOROUGHS = {
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
    "Croydon", "Ealing", "Enfield", "Haringey", "Harrow", "Havering",
    "Hillingdon", "Hounslow", "Kingston upon Thames", "Merton", "Newham",
    "Redbridge", "Richmond upon Thames", "Sutton", "Waltham Forest",
}
LONDON_BOROUGHS = INNER_LONDON_BOROUGHS | OUTER_LONDON_BOROUGHS

# Home Counties districts -> their traditional/ceremonial county, for the
# map's county-level labels. Built from the districts that actually appear
# across the 681-outcode London + Home Counties scope (see
# connector_scraper_data/outcodes_london_and_home_counties.txt) -- covers
# Surrey, Kent, Essex, Hertfordshire, Buckinghamshire, Berkshire,
# Bedfordshire, and the bit of East/West Sussex and Hampshire the scope
# reaches into. A district not listed here (a scope change, or postcodes.io
# returning an unexpected name) just gets no county label -- borough/group
# classification doesn't depend on this map at all.
DISTRICT_TO_COUNTY = {
    # Surrey
    "Elmbridge": "Surrey", "Epsom and Ewell": "Surrey", "Guildford": "Surrey",
    "Mole Valley": "Surrey", "Reigate and Banstead": "Surrey", "Runnymede": "Surrey",
    "Spelthorne": "Surrey", "Surrey Heath": "Surrey", "Tandridge": "Surrey",
    "Waverley": "Surrey", "Woking": "Surrey",
    # Kent
    "Ashford": "Kent", "Canterbury": "Kent", "Dartford": "Kent", "Dover": "Kent",
    "Folkestone and Hythe": "Kent", "Gravesham": "Kent", "Maidstone": "Kent",
    "Sevenoaks": "Kent", "Swale": "Kent", "Thanet": "Kent",
    "Tonbridge and Malling": "Kent", "Tunbridge Wells": "Kent",
    "Medway": "Kent",
    # Essex
    "Basildon": "Essex", "Braintree": "Essex", "Brentwood": "Essex",
    "Castle Point": "Essex", "Chelmsford": "Essex", "Colchester": "Essex",
    "Epping Forest": "Essex", "Harlow": "Essex", "Maldon": "Essex",
    "Rochford": "Essex", "Tendring": "Essex", "Uttlesford": "Essex",
    "Southend-on-Sea": "Essex", "Thurrock": "Essex",
    # Hertfordshire
    "Broxbourne": "Hertfordshire", "Dacorum": "Hertfordshire",
    "East Hertfordshire": "Hertfordshire", "Hertsmere": "Hertfordshire",
    "North Hertfordshire": "Hertfordshire", "St Albans": "Hertfordshire",
    "Stevenage": "Hertfordshire", "Three Rivers": "Hertfordshire",
    "Watford": "Hertfordshire", "Welwyn Hatfield": "Hertfordshire",
    # Buckinghamshire
    "Buckinghamshire": "Buckinghamshire", "Chiltern": "Buckinghamshire",
    "South Bucks": "Buckinghamshire", "Wycombe": "Buckinghamshire",
    "Aylesbury Vale": "Buckinghamshire", "Milton Keynes": "Buckinghamshire",
    # Berkshire (no county council since 1998, but still the ceremonial county)
    "Bracknell Forest": "Berkshire", "Reading": "Berkshire",
    "Slough": "Berkshire", "West Berkshire": "Berkshire",
    "Windsor and Maidenhead": "Berkshire", "Wokingham": "Berkshire",
    # Bedfordshire
    "Bedford": "Bedfordshire", "Central Bedfordshire": "Bedfordshire",
    "Luton": "Bedfordshire",
    # West Sussex
    "Crawley": "West Sussex", "Horsham": "West Sussex", "Mid Sussex": "West Sussex",
    "Worthing": "West Sussex", "Arun": "West Sussex", "Adur": "West Sussex",
    "Chichester": "West Sussex", "Brighton and Hove": "West Sussex",
    # East Sussex
    "Lewes": "East Sussex", "Eastbourne": "East Sussex", "Rother": "East Sussex",
    "Wealden": "East Sussex", "Hastings": "East Sussex",
    # Hampshire (edge of scope)
    "Rushmoor": "Hampshire", "East Hampshire": "Hampshire", "Hart": "Hampshire",
    "Basingstoke and Deane": "Hampshire",
    # Oxfordshire (edge of scope)
    "Vale of White Horse": "Oxfordshire", "South Oxfordshire": "Oxfordshire",
    # Suffolk (edge of scope)
    "Babergh": "Suffolk",
    "Sevenoaks District": "Kent",
}


def borough_geo_group(admin_districts: list[str]) -> tuple[str | None, str | None]:
    """Classifies an outcode's postcodes.io admin_district list into
    (borough, geo_group). An outcode can straddle more than one district
    (e.g. SW1A: Wandsworth + Westminster) -- picks the first that's a
    recognised London borough if any is, else just the first district
    (good enough: districts within one outcode are always adjacent)."""
    if not admin_districts:
        return None, None
    london = next((d for d in admin_districts if d in LONDON_BOROUGHS), None)
    borough = london or admin_districts[0]
    if borough in INNER_LONDON_BOROUGHS:
        geo_group = "Inner London"
    elif borough in OUTER_LONDON_BOROUGHS:
        geo_group = "Greater London"
    else:
        geo_group = "Home Counties"
    return borough, geo_group


def fetch_outcode_admin_areas(outcodes: list[str]) -> dict[str, tuple[str | None, str | None]]:
    """Looks up (borough, geo_group) for each outcode via postcodes.io's
    per-outcode endpoint's admin_district field. Same one-request-per-
    outcode shape as fetch_outcode_centroids (postcodes.io has no bulk
    /outcodes endpoint)."""
    session = requests.Session()
    result: dict[str, tuple[str | None, str | None]] = {}

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_outcode_result, session, o): o for o in outcodes}
        for i, future in enumerate(as_completed(futures), start=1):
            outcode = futures[future]
            data = future.result()
            if data is not None:
                result[outcode] = borough_geo_group(data.get("admin_district") or [])
            if i % 100 == 0 or i == len(outcodes):
                logger.info("postcodes.io: resolved admin areas for %d/%d outcodes so far", i, len(outcodes))

    return result


def backfill_outcode_areas(session: Session, outcode_filter: list[str] | None = None) -> int:
    """Populates borough/region/geo_group for outcodes already in the
    table (region is set to the DISTRICT_TO_COUNTY mapping's county for
    Home Counties boroughs, or "Greater London" for London ones -- see
    module docstring lists above). Returns the number updated."""
    query = select(Outcode)
    if outcode_filter:
        query = query.where(Outcode.outcode.in_(outcode_filter))
    outcodes = [o.outcode for o in session.scalars(query)]
    logger.info("Backfilling borough/geo_group for %d outcodes", len(outcodes))
    areas = fetch_outcode_admin_areas(outcodes)

    updated = 0
    for outcode, (borough, geo_group) in areas.items():
        if geo_group == "Home Counties":
            region = DISTRICT_TO_COUNTY.get(borough)
        elif geo_group in ("Inner London", "Greater London"):
            region = "Greater London"
        else:
            region = None
        session.execute(
            update(Outcode).where(Outcode.outcode == outcode).values(borough=borough, region=region, geo_group=geo_group)
        )
        updated += 1

    logger.info("Backfilled %d/%d outcodes (%d not resolved by postcodes.io)", updated, len(outcodes), len(outcodes) - updated)
    return updated


def fetch_postcode_centroids(postcodes: list[str]) -> dict[str, tuple[float, float]]:
    """Looks up (lat, long) for full UK postcodes (e.g. "SW6 5PA"), NOT
    outcodes -- postcodes.io has a real bulk endpoint for this one (unlike
    /outcodes, verified with a live call before relying on it), so this is
    batched POSTs rather than one request per postcode."""
    session = requests.Session()
    result: dict[str, tuple[float, float]] = {}

    for i in range(0, len(postcodes), _POSTCODE_BATCH_SIZE):
        batch = postcodes[i : i + _POSTCODE_BATCH_SIZE]
        try:
            resp = session.post(POSTCODES_IO_BULK_POSTCODES_URL, json={"postcodes": batch}, timeout=_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            logger.error("postcodes.io bulk postcode batch %d-%d failed: %s", i, i + len(batch), e)
            continue
        if resp.status_code != 200:
            logger.error("postcodes.io bulk postcode batch %d-%d failed: status=%d body=%s", i, i + len(batch), resp.status_code, resp.text[:300])
            continue

        for entry in resp.json().get("result", []):
            data = entry.get("result")
            if data is None:
                logger.warning("postcodes.io has no data for postcode=%r", entry.get("query"))
                continue
            # Key by the query we sent (not postcodes.io's normalized
            # `data["postcode"]`) so the caller can look results up by the
            # exact postcode string it has on file, whitespace and all.
            result[entry["query"]] = (data["latitude"], data["longitude"])

        logger.info("postcodes.io: resolved %d/%d postcodes so far", len(result), i + len(batch))

    return result


# How many real postcodes to sample per sector when computing its centroid.
# Postcode sectors are small (typically a few hundred metres to ~1km
# across), so averaging a handful of real addresses' coordinates is plenty
# -- no need to bulk-lookup every one of the (often hundreds of) distinct
# postcodes a busy sector has in price_paid_transactions.
_SECTOR_CENTROID_SAMPLE_SIZE = 8


def compute_sector_centroids(session: Session, sample_size: int = _SECTOR_CENTROID_SAMPLE_SIZE) -> int:
    """Derives a lat/long centroid for every postcode sector that has at
    least one row in price_paid_transactions, by bulk-looking-up a sample
    of that sector's real postcodes via postcodes.io and averaging them.
    Upserts into postcode_sectors. Returns the number of sectors seeded."""
    rows = session.execute(
        select(PricePaidTransaction.sector, PricePaidTransaction.outcode, PricePaidTransaction.postcode).distinct()
    ).all()

    by_sector: dict[str, dict] = {}
    for sector, outcode, postcode in rows:
        entry = by_sector.setdefault(sector, {"outcode": outcode, "postcodes": []})
        if len(entry["postcodes"]) < sample_size:
            entry["postcodes"].append(postcode)

    all_sample_postcodes = sorted({pc for entry in by_sector.values() for pc in entry["postcodes"]})
    logger.info(
        "Computing centroids for %d sectors from a sample of %d real postcodes",
        len(by_sector), len(all_sample_postcodes),
    )
    centroids = fetch_postcode_centroids(all_sample_postcodes)

    seeded = 0
    for sector, entry in by_sector.items():
        points = [centroids[pc] for pc in entry["postcodes"] if pc in centroids]
        if not points:
            logger.warning("No resolved postcodes for sector=%s (%d sampled) -- skipping", sector, len(entry["postcodes"]))
            continue
        lat = sum(p[0] for p in points) / len(points)
        long_ = sum(p[1] for p in points) / len(points)
        stmt = sqlite_insert(PostcodeSector).values(
            sector=sector, outcode=entry["outcode"], lat=lat, long=long_, postcode_count=len(points),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["sector"],
            set_={"lat": stmt.excluded.lat, "long": stmt.excluded.long, "postcode_count": stmt.excluded.postcode_count},
        )
        session.execute(stmt)
        seeded += 1

    logger.info("Seeded %d/%d sector centroids (%d unresolved)", seeded, len(by_sector), len(by_sector) - seeded)
    return seeded


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
