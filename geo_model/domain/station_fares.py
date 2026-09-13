"""Bridges RailStation (NaPTAN, atco_code-keyed) to the ATOC/RSP fares
feed's location/flow/fare data (NLC-keyed) and derives ticket prices from
it. Pure functions only, like geo_model.domain.address_match/stations --
no DB or HTTP; geo_model.pipeline wires this to the data layer.

The two station datasets share no common code (NaPTAN carries no CRS/NLC
at all -- confirmed by inspecting every column it exports), so the join is
by name, same "no shared ID" situation address_match.py solves for
EPC<->Price Paid Data. It's harder here because the fares feed's own
station name field (LOC file's DESCRIPTION) is only 16 characters, so
longer names are hand-abbreviated inconsistently -- sometimes by dropping
vowels ("Paddington" -> "PDDNGTN"), sometimes by dropping whole syllables
("Islington" -> "ISLTN"), with no single deterministic rule. Validated
empirically (scratchpad script, not committed) against the real feed
before writing this: exact matching after expanding common abbreviations
(STREET->ST, JUNCTION->JN, etc.) resolves ~85% of the stations this
project actually uses; a second, deliberately conservative fuzzy pass
(vowel-dropped "consonant skeleton" comparison, high similarity threshold,
tightened further for short/single-word names after real short names were
found to collide nationally -- e.g. "Chelmsford" spuriously resembling
"Chelsfield" at a loose threshold) resolves most of the remainder, taking
total coverage to ~92% of used stations and leaving zero postcode sectors
with no fare-priceable station among their nearest five. Anything left
unmatched is reported as such, never guessed at (see match_stations_to_
fare_locations's docstring).
"""
from __future__ import annotations

import difflib
import re
from collections import defaultdict
from dataclasses import dataclass

__all__ = [
    "FareLocationRecord",
    "StationFareMatchResult",
    "FlowRecord",
    "FareRecord",
    "normalize_station_name",
    "consonant_skeleton",
    "match_stations_to_fare_locations",
    "find_flow_id",
    "build_flow_index",
    "find_flow_id_indexed",
    "monthly_from_weekly",
    "extract_ticket_fare",
    "extract_fares_for_flow",
    "select_monthly_season",
    "ANYTIME_DAY_RETURN_CODE",
    "WEEKLY_SEASON_CODE",
    "TRAVELCARD_WEEKLY_SEASON_CODE",
    "LONDON_ZONES_1_TO_6_DESCRIPTION",
    "MONTHLY_SEASON_MULTIPLIER",
    "MONTHLY_BASIS_ZONES_TRAVELCARD",
    "MONTHLY_BASIS_STATION_OR_CLUSTER",
    "ADULT_STATUS_CODE",
]

# UK-wide regulated fixed multiplier: Monthly season = Weekly season x
# 3.84 (Annual = x40), applied identically by every train operator.
# Verified independently (not derivable from the fares feed -- standard
# class has no direct Monthly ticket-type code at all, only First Class
# does: 'MFB' = "MONTHLY 1ST"). This is why SectorStationFare.
# monthly_is_computed is always True.
MONTHLY_SEASON_MULTIPLIER = 3.84

ANYTIME_DAY_RETURN_CODE = "SDR"  # standard class
WEEKLY_SEASON_CODE = "7DS"  # standard class, rail-only (no Travelcard)
# Weekly season INCLUDING a Zones 1-6 Travelcard, standard class -- the
# product a commuter from outside the zonal boundary actually buys, and
# what a fare-comparison site (Trainline etc.) shows by default. Priced
# to the special "LONDON ZONES 1-6" location (no CRS -- see
# geo_model.fares_data.parse_locations), not to the individual station or
# cluster, so it never shows up on the same flow as WEEKLY_SEASON_CODE.
# Real example that surfaced this: Gerrards Cross's rail-only weekly to
# the "London Terminals" cluster is ~£87, but the advertised commuter
# price is ~£115 -- the gap is exactly this Travelcard add-on.
TRAVELCARD_WEEKLY_SEASON_CODE = "7TS"
LONDON_ZONES_1_TO_6_DESCRIPTION = "LONDON ZONES 1-6"
ADULT_STATUS_CODE = "000"

# The LOC file's 16-char DESCRIPTION abbreviates whole words by hand
# ("STREET" -> "ST", "JUNCTION" -> "JN"); expanding common ones on our
# side before comparing recovers a big chunk of exact matches for free.
_ABBREV = {
    "STREET": "ST",
    "ROAD": "RD",
    "JUNCTION": "JN",
    "INTERNATIONAL": "INTL",
    "UNDER": "U",
    "PARKWAY": "PKWY",
    "CENTRAL": "CTRL",
    "GREEN": "GRN",
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "AND": "",
}

# Fuzzy-match similarity cutoffs (difflib ratio, 0-1). Multi-token
# skeletons are long/distinctive enough that a looser cutoff is safe;
# short single-token skeletons collide nationally far more easily (real
# example found during validation: "Chelmsford" and "Chelsfield" reach
# 0.80 similarity as skeletons despite being ~200 miles apart), so those
# need a near-exact match to be accepted at all.
_FUZZY_CUTOFF_MULTI_TOKEN = 0.85
_FUZZY_CUTOFF_SINGLE_TOKEN = 0.95
# A top match isn't trusted unless it clearly beats the runner-up by this
# margin -- otherwise two similarly-plausible stations are indistinguishable
# and the match is reported ambiguous rather than guessed.
_FUZZY_AMBIGUITY_MARGIN = 0.05


def normalize_station_name(name: str) -> str:
    """"Ashton-under-Lyne Rail Station" -> "ASHTON U LYNE"; drops
    parenthetical disambiguators ("Reedham (Surrey)" -> "REEDHAM") since
    the fares feed's abbreviated description never carries them."""
    n = name.upper().strip()
    n = re.sub(r"\([^)]*\)", "", n)
    n = re.sub(r"\bRAIL STATION\b", "", n)
    n = re.sub(r"\bSTATION\b", "", n)
    n = re.sub(r"[^\sA-Z0-9]", " ", n)  # hyphens/&/apostrophes -> space, not deleted
    tokens = [t for t in n.split() if t]
    tokens = [_ABBREV.get(t, t) for t in tokens]
    return " ".join(t for t in tokens if t)


def consonant_skeleton(name: str) -> str:
    """Collapses a normalized name to "first letter of each word plus its
    consonants" (vowels after the first letter dropped), the same shape
    the fares feed's hand-abbreviated descriptions tend toward
    ("Paddington" -> "PDDNGTN", "Harlington" -> "HRLNGTN"). Not a full
    replication of the feed's abbreviation style (some entries drop
    further letters beyond vowels, e.g. "Islington" -> "ISLTN") -- close
    enough to feed into a fuzzy-similarity comparison, not meant to be
    compared for exact equality on its own."""
    out = []
    for word in name.split():
        if not word:
            continue
        out.append(word[0] + re.sub(r"[AEIOU]", "", word[1:]))
    return " ".join(out)


@dataclass(frozen=True)
class FareLocationRecord:
    nlc: str
    description: str
    crs_code: str
    fare_group_nlc: str


@dataclass(frozen=True)
class StationFareMatchResult:
    station_atco_code: str
    station_name: str
    nlc: str | None
    crs_code: str | None
    fare_group_nlc: str | None
    confidence: str  # "exact" | "fuzzy" | "unmatched"
    match_note: str | None


def match_stations_to_fare_locations(
    stations: list, fare_locations: list[FareLocationRecord]
) -> list[StationFareMatchResult]:
    """Returns one result per station in ``stations`` (each needs
    .atco_code and .name -- geo_model.domain.stations.StationRecord
    satisfies this), never dropping one: a station with no safe match
    comes back with confidence="unmatched" and nlc=None rather than being
    omitted, so it stays visible for troubleshooting instead of silently
    vanishing from downstream fare lookups."""
    fare_by_norm: dict[str, FareLocationRecord] = {}
    norm_collisions: dict[str, set[str]] = defaultdict(set)
    for loc in fare_locations:
        key = normalize_station_name(loc.description)
        norm_collisions[key].add(loc.nlc)
        fare_by_norm.setdefault(key, loc)

    skeleton_index: dict[str, list[FareLocationRecord]] = defaultdict(list)
    for key, loc in fare_by_norm.items():
        skeleton_index[consonant_skeleton(key)].append(loc)
    all_skeletons = list(skeleton_index.keys())

    results: list[StationFareMatchResult] = []
    for station in stations:
        key = normalize_station_name(station.name)
        exact = fare_by_norm.get(key)
        if exact is not None:
            note = None
            if len(norm_collisions[key]) > 1:
                note = f"multiple fare locations share this normalized name: {sorted(norm_collisions[key])}, used {exact.nlc}"
            results.append(StationFareMatchResult(
                station_atco_code=station.atco_code, station_name=station.name,
                nlc=exact.nlc, crs_code=exact.crs_code, fare_group_nlc=exact.fare_group_nlc,
                confidence="exact", match_note=note,
            ))
            continue

        sk = consonant_skeleton(key)
        multi_token = " " in sk
        cutoff = _FUZZY_CUTOFF_MULTI_TOKEN if multi_token else _FUZZY_CUTOFF_SINGLE_TOKEN
        best = difflib.get_close_matches(sk, all_skeletons, n=2, cutoff=cutoff)
        if not best:
            results.append(StationFareMatchResult(
                station_atco_code=station.atco_code, station_name=station.name,
                nlc=None, crs_code=None, fare_group_nlc=None,
                confidence="unmatched", match_note="no fare-location name reached the fuzzy-match similarity threshold",
            ))
            continue

        top = best[0]
        top_ratio = difflib.SequenceMatcher(None, sk, top).ratio()
        second_ratio = difflib.SequenceMatcher(None, sk, best[1]).ratio() if len(best) > 1 else 0.0
        candidates = skeleton_index[top]
        distinct_nlcs = {c.nlc for c in candidates}
        if len(distinct_nlcs) > 1 or (second_ratio > 0 and top_ratio - second_ratio < _FUZZY_AMBIGUITY_MARGIN):
            results.append(StationFareMatchResult(
                station_atco_code=station.atco_code, station_name=station.name,
                nlc=None, crs_code=None, fare_group_nlc=None,
                confidence="unmatched",
                match_note=f"ambiguous fuzzy match: skeleton {sk!r} nearly ties between {top!r} and {best[1]!r}" if len(best) > 1 else f"ambiguous fuzzy match: skeleton {top!r} maps to multiple distinct NLCs {sorted(distinct_nlcs)}",
            ))
            continue

        winner = candidates[0]
        results.append(StationFareMatchResult(
            station_atco_code=station.atco_code, station_name=station.name,
            nlc=winner.nlc, crs_code=winner.crs_code, fare_group_nlc=winner.fare_group_nlc,
            confidence="fuzzy", match_note=f"consonant-skeleton match: {sk!r} ~ {top!r} (ratio={top_ratio:.2f})",
        ))

    return results


@dataclass(frozen=True)
class FlowRecord:
    flow_id: str
    origin_nlc: str
    dest_nlc: str
    status_code: str
    direction: str  # "S" single-direction, "R" reversible


@dataclass(frozen=True)
class FareRecord:
    flow_id: str
    ticket_code: str
    fare_pence: int


def find_flow_id(origin_nlcs: set[str], dest_nlcs: set[str], flows: list[FlowRecord]) -> str | None:
    """``origin_nlcs``/``dest_nlcs`` should each contain a station's own
    NLC plus its fare-group/cluster NLC -- a station whose fares are only
    priced to a shared cluster (e.g. Victoria/Cannon Street tickets from
    outer stations are priced to "London Terminals" NLC 1072, not to the
    individual terminus -- confirmed against the real feed) has no flow
    under its own NLC at all. Only adult (status_code '000') flows are in
    scope. A reversible ('R') flow prices either direction; a single-
    direction ('S') flow must match origin->dest order exactly. Returns
    the first match -- callers should already have narrowed ``flows`` to
    ones plausibly relevant (see geo_model.fares_data.parse_flows_and_
    fares's origin/destination filtering) so more than one true match
    would itself indicate a data question worth surfacing, not silently
    picking one."""
    for f in flows:
        if f.status_code != ADULT_STATUS_CODE:
            continue
        if f.origin_nlc in origin_nlcs and f.dest_nlc in dest_nlcs:
            return f.flow_id
        if f.direction == "R" and f.origin_nlc in dest_nlcs and f.dest_nlc in origin_nlcs:
            return f.flow_id
    return None


def build_flow_index(flows: list[FlowRecord]) -> dict[tuple[str, str], str]:
    """Precomputes an (origin_nlc, dest_nlc) -> flow_id lookup so
    find_flow_id_indexed() can answer each query in O(1) instead of
    scanning every flow -- needed at pipeline scale, where thousands of
    (station, reference point) pairs would otherwise each scan the same
    tens-of-thousands-strong flow list linearly (find_flow_id is fine for
    a handful of ad-hoc lookups, e.g. in tests, but not for that). Only
    adult flows are indexed; a reversible flow is indexed both ways, a
    single-direction one only in its recorded direction. ``setdefault``
    keeps the first flow seen for a given direction if more than one
    technically matches (rare, and no way to prefer one over another
    without more information than a flow_id gives)."""
    index: dict[tuple[str, str], str] = {}
    for f in flows:
        if f.status_code != ADULT_STATUS_CODE:
            continue
        index.setdefault((f.origin_nlc, f.dest_nlc), f.flow_id)
        if f.direction == "R":
            index.setdefault((f.dest_nlc, f.origin_nlc), f.flow_id)
    return index


def find_flow_id_indexed(origin_nlcs: set[str], dest_nlcs: set[str], flow_index: dict[tuple[str, str], str]) -> str | None:
    """Same semantics as find_flow_id, against a prebuilt build_flow_index()
    result instead of a raw flow list."""
    for o in origin_nlcs:
        for d in dest_nlcs:
            flow_id = flow_index.get((o, d))
            if flow_id is not None:
                return flow_id
    return None


def monthly_from_weekly(weekly_fare_pence: int) -> int:
    """UK-wide regulated fixed multiplier -- see MONTHLY_SEASON_MULTIPLIER."""
    return round(weekly_fare_pence * MONTHLY_SEASON_MULTIPLIER)


def extract_ticket_fare(fares_for_flow: list[FareRecord], ticket_code: str) -> int | None:
    return next((f.fare_pence for f in fares_for_flow if f.ticket_code == ticket_code), None)


def extract_fares_for_flow(fares_for_flow: list[FareRecord]) -> tuple[int | None, int | None]:
    """Returns (anytime_day_return_pence, monthly_season_pence) for one
    flow's fare rows, using only that flow's own rail-only Weekly (7DS)
    for the monthly figure -- a simple convenience for a single
    station/cluster-direct flow. Real sector_station_fares computation
    should prefer select_monthly_season() instead, which also checks the
    Zones 1-6 Travelcard flow (see that function's docstring for why)."""
    sdr = extract_ticket_fare(fares_for_flow, ANYTIME_DAY_RETURN_CODE)
    weekly = extract_ticket_fare(fares_for_flow, WEEKLY_SEASON_CODE)
    monthly = monthly_from_weekly(weekly) if weekly is not None else None
    return sdr, monthly


# SectorStationFare.monthly_fare_basis values -- see select_monthly_season.
MONTHLY_BASIS_ZONES_TRAVELCARD = "zones_1_6_travelcard"
MONTHLY_BASIS_STATION_OR_CLUSTER = "station_or_cluster"


def select_monthly_season(
    station_fares_for_flow: list[FareRecord],
    zones_fares_for_flow: list[FareRecord] | None,
) -> tuple[int | None, str]:
    """Prefers the Zones 1-6 Travelcard-inclusive Weekly (7TS, priced to
    the special "LONDON ZONES 1-6" location -- see LONDON_ZONES_1_TO_6_
    DESCRIPTION) when a flow to it exists: that's the product a real
    commuter from outside the zonal boundary buys, and what a fare-
    comparison site shows by default -- confirmed against a real mismatch
    (see TRAVELCARD_WEEKLY_SEASON_CODE's docstring). Falls back to the
    station/cluster-direct rail-only Weekly (7DS) otherwise, e.g. for a
    station already inside the TfL zones (observed: Charlton, zone 4)
    that has no Zones 1-6 flow at all -- its own rail fare already covers
    zonal travel, no add-on needed.

    Returns (monthly_pence_or_None, basis); ``basis`` is surfaced on
    SectorStationFare.monthly_fare_basis so the frontend can label which
    kind of price it's showing rather than imply they're interchangeable."""
    if zones_fares_for_flow:
        weekly = extract_ticket_fare(zones_fares_for_flow, TRAVELCARD_WEEKLY_SEASON_CODE)
        if weekly is not None:
            return monthly_from_weekly(weekly), MONTHLY_BASIS_ZONES_TRAVELCARD
    weekly = extract_ticket_fare(station_fares_for_flow, WEEKLY_SEASON_CODE)
    if weekly is not None:
        return monthly_from_weekly(weekly), MONTHLY_BASIS_STATION_OR_CLUSTER
    return None, MONTHLY_BASIS_STATION_OR_CLUSTER
