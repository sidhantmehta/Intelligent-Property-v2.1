"""Authenticates against the National Rail Data Portal (NRDP,
opendata.nationalrail.co.uk) and ingests the ATOC/RSP Fares static feed --
the free, CC-licensed, canonical UK rail fares dataset, updated ~3x/year
(Jan/May/Sept). This is what geo_model.domain.station_fares needs real
NLC/CRS/flow/fare data from.

NRDP is being retired in 2026 (the "Darwin Evolution" project) in favour
of the Rail Data Marketplace (raildata.org.uk) -- by explicit user
decision this module targets NRDP now and defers that migration; if NRDP
stops working, re-registering on RDM and swapping the auth/download
endpoints here is the fix, not a sign this module is broken.

The feed itself is a ~47MB zip of fixed-width mainframe-format files named
RJFAF<sequence>.<EXT> (sequence increments each release, e.g. RJFAF882 one
month, RJFAF883 the next -- callers must glob for the extension, never
hardcode a sequence number). Byte offsets below are taken directly from
RSP's own RSPS5045 "Fares and Associated Data Feed Interface Specification"
(version P-02-03) and verified against a real downloaded feed before being
trusted -- positions in that spec are 1-indexed inclusive, so every offset
here is spec_position - 1, matching Python's 0-indexed half-open slicing.

Like epc_data.py/price_data.py/rail_stations.py, this both fetches and
writes to the DB -- the exception to the project's data/domain/pipeline
layering, per those modules' own docstrings.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Iterator

from sqlalchemy import delete

from geo_model.data.models import FareLocation, FareTicketType
from geo_model.domain.station_fares import FareLocationRecord, FareRecord, FlowRecord
from geo_model.logging_setup import get_logger

logger = get_logger(__name__)

NRDP_AUTH_URL = "https://opendata.nationalrail.co.uk/authenticate"
NRDP_FARES_FEED_URL = "https://opendata.nationalrail.co.uk/api/staticfeeds/2.0/fares"
_TIMEOUT_SECONDS = 300
_USER_AGENT = "Intelligent-Property-v2.1 (research use, see repo README)"


def authenticate() -> str:
    """Reads NRDP_USERNAME/NRDP_PASSWORD from the environment (see
    .env.example), POSTs to the NRDP auth endpoint, and returns the auth
    token to use as the X-Auth-Token header on the feed download.
    Credentials are never logged or included in any exception message."""
    username = os.environ.get("NRDP_USERNAME")
    password = os.environ.get("NRDP_PASSWORD")
    if not username or not password:
        raise RuntimeError("NRDP_USERNAME/NRDP_PASSWORD are not set -- see .env.example")

    body = urllib.parse.urlencode({"username": username, "password": password}).encode("ascii")
    req = urllib.request.Request(
        NRDP_AUTH_URL, data=body, method="POST",
        headers={"User-Agent": _USER_AGENT, "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"NRDP authentication failed: HTTP {e.code}") from None
    token = payload.get("token")
    if not token:
        raise RuntimeError("NRDP authentication response had no token")
    logger.info("Authenticated against NRDP as %s", username)
    return token


def download_fares_feed(token: str, dest_zip: Path) -> Path:
    """Downloads the current fares feed zip to ``dest_zip`` using the
    token from authenticate()."""
    req = urllib.request.Request(
        NRDP_FARES_FEED_URL, headers={"User-Agent": _USER_AGENT, "X-Auth-Token": token},
    )
    logger.info("Downloading NRDP fares feed to %s", dest_zip)
    with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp, open(dest_zip, "wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    return dest_zip


def extract_fares_zip(zip_path: Path, dest_dir: Path) -> dict[str, Path]:
    """Extracts the feed zip and returns the paths to the three files this
    project reads: LOC (locations), TTY (ticket types), FFL (flows+fares).
    The sequence number in the filename (RJFAF<seq>.<EXT>) changes every
    release, so this globs for the extension rather than assuming a name."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)
    result = {}
    for ext in ("LOC", "TTY", "FFL"):
        matches = list(dest_dir.glob(f"*.{ext}"))
        if not matches:
            raise RuntimeError(f"No .{ext} file found in extracted fares feed at {dest_dir}")
        if len(matches) > 1:
            raise RuntimeError(f"Multiple .{ext} files found in {dest_dir}: {matches}")
        result[ext] = matches[0]
    return result


def parse_locations(loc_path: Path) -> Iterator[FareLocationRecord]:
    """Parses the LOC file's Location records (RSPS5045 section 4.19.2).
    Rows with a non-blank CRS code are real physical stations. Rows
    without one are mostly PlusBus zones/county/area codes that can never
    be a rail journey endpoint -- except the fifteen "LONDON ZONES x-y"
    locations (e.g. NLC 0035 = "LONDON ZONES 1-6"), which have no CRS
    (they aren't a station) but ARE real destinations: the Travelcard-
    inclusive season ticket a commuter from outside the zonal boundary
    actually buys is priced to one of these, not to the individual
    terminus -- found by hand while investigating a real fare mismatch
    (Gerrards Cross's "London Terminals"-cluster weekly, ~£87, undershot
    the real advertised ~£115 weekly; the difference is exactly this
    zones-1-6 Travelcard add-on). So both kinds are kept; everything else
    blank-CRS is dropped. Header lines (RSP's own file-metadata banner)
    start with '/' and are skipped; data lines start with an update
    marker ('R' in a full refresh) followed by RECORD_TYPE='L' at
    position 2."""
    with open(loc_path, encoding="latin-1") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line or line.startswith("/") or len(line) < 75:
                continue
            if line[1] != "L":
                continue
            nlc = line[36:40]
            description = line[40:56].strip()
            crs_code = line[56:59].strip()
            fare_group_nlc = line[69:75].strip()
            if not crs_code and not description.startswith("LONDON ZONES"):
                continue
            yield FareLocationRecord(nlc=nlc, description=description, crs_code=crs_code, fare_group_nlc=fare_group_nlc or nlc)


def parse_ticket_types(tty_path: Path) -> Iterator[dict]:
    """Parses the TTY file's Ticket Types records (RSPS5045 section
    4.6.2). No RECORD_TYPE marker exists in this file (unlike LOC/FFL) --
    header/comment lines start with '/' and are skipped, everything else
    is a data row."""
    with open(tty_path, encoding="latin-1") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line or line.startswith("/") or len(line) < 45:
                continue
            yield dict(
                ticket_code=line[1:4].strip(),
                description=line[28:43].strip(),
                tkt_class=line[43],
                tkt_type=line[44],
            )


def parse_flows_and_fares(
    ffl_path: Path, relevant_nlcs: set[str]
) -> tuple[list[FlowRecord], list[FareRecord]]:
    """Single pass over the FFL file's interleaved Flow ('F' at position 2)
    and Fare ('T' at position 2) records (RSPS5045 sections 4.1.2/4.1.3).
    The full feed is ~8.3M lines nationwide; this project only ever needs
    flows connecting a bounded set of stations (our sectors' nearest
    stations, and the reference points' nearest stations) to each other,
    so a flow is kept only if EITHER its origin or destination NLC is in
    ``relevant_nlcs`` (callers should pass the union of both sides' NLC +
    fare-group sets) -- keeping fare rows for a flow_id kept from the flow
    pass, and dropping the flow (and its fares) otherwise. Only adult
    fares are ever relevant here, but that filter is left to
    geo_model.domain.station_fares.find_flow_id rather than duplicated."""
    flows: list[FlowRecord] = []
    kept_flow_ids: set[str] = set()
    fares: list[FareRecord] = []

    with open(ffl_path, encoding="latin-1") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if len(line) < 2:
                continue
            record_type = line[1]
            # Flow records (49 chars) and Fare records (20-22 chars) are
            # very different lengths -- a single shared minimum-length
            # check would silently discard every Fare record (a real bug
            # caught by testing against the downloaded feed: Fare lines
            # are only ~22 chars, well under a Flow-sized threshold).
            if record_type == "F":
                if len(line) < 49:
                    continue
                origin_nlc = line[2:6]
                dest_nlc = line[6:10]
                if origin_nlc not in relevant_nlcs and dest_nlc not in relevant_nlcs:
                    continue
                flow_id = line[42:49]
                flows.append(FlowRecord(
                    flow_id=flow_id, origin_nlc=origin_nlc, dest_nlc=dest_nlc,
                    status_code=line[15:18], direction=line[19],
                ))
                kept_flow_ids.add(flow_id)
            elif record_type == "T":
                if len(line) < 20:
                    continue
                flow_id = line[2:9]
                if flow_id not in kept_flow_ids:
                    continue
                try:
                    fare_pence = int(line[12:20])
                except ValueError:
                    continue
                fares.append(FareRecord(flow_id=flow_id, ticket_code=line[9:12].strip(), fare_pence=fare_pence))

    logger.info(
        "Parsed FFL: %d relevant flows, %d fares (relevant_nlcs=%d)",
        len(flows), len(fares), len(relevant_nlcs),
    )
    return flows, fares


def ingest_fares(session, loc_path: Path, tty_path: Path) -> dict:
    """Parses the LOC/TTY files and replaces fare_locations/
    fare_ticket_types in full -- small, fixed-size national reference
    tables (~28,700 and ~4,100 rows), always a full replace rather than a
    scoped recompute since there's no sector/outcode split that would make
    sense for them. Run whenever a new fares feed release is downloaded
    (see module docstring for the ~3x/year cadence); re-run
    geo_model.pipeline.match_stations_to_fares() right after, since a
    changed fare_locations can change which stations match."""
    locations = list(parse_locations(loc_path))
    ticket_types = [t for t in parse_ticket_types(tty_path) if t["ticket_code"]]
    logger.info("Parsed %d fare locations, %d ticket types", len(locations), len(ticket_types))

    # Both files carry one record per date-validity period, not one per
    # NLC/ticket_code -- a location or ticket type whose details changed
    # over time (or that's simply listed with a future-dated renewal
    # record) appears more than once with the same key. We don't track
    # date ranges (FareLocationRecord/parse_ticket_types don't parse
    # START_DATE/END_DATE), so this keeps one row per key -- first
    # occurrence wins, which is fine since the fields that matter for
    # matching (description/CRS/fare_group, ticket class/type) were
    # observed identical across a key's repeats in the real feed.
    loc_by_nlc: dict[str, FareLocationRecord] = {}
    for l in locations:
        loc_by_nlc.setdefault(l.nlc, l)
    tty_by_code: dict[str, dict] = {}
    for t in ticket_types:
        tty_by_code.setdefault(t["ticket_code"], t)

    session.execute(delete(FareLocation))
    loc_rows = [dict(nlc=l.nlc, description=l.description, crs_code=l.crs_code, fare_group_nlc=l.fare_group_nlc) for l in loc_by_nlc.values()]
    for i in range(0, len(loc_rows), 5000):
        session.execute(FareLocation.__table__.insert(), loc_rows[i:i + 5000])

    session.execute(delete(FareTicketType))
    tty_rows = list(tty_by_code.values())
    for i in range(0, len(tty_rows), 2000):
        session.execute(FareTicketType.__table__.insert(), tty_rows[i:i + 2000])
    session.commit()

    return {"fare_locations": len(loc_rows), "fare_ticket_types": len(tty_rows)}
