"""Joins an EPC certificate to the Price Paid Data sale of the *same
dwelling* without a shared ID -- Price Paid Data carries no UPRN, so this
matches on postcode + a normalized house number (+ a normalized flat/unit
id for flats, since a postcode's house number alone doesn't disambiguate a
block of flats).

This is deliberately conservative: a case this module can't disambiguate
with reasonable confidence is returned as "ambiguous" rather than guessed
at, so a bad guess never silently pollutes a price. Real example that
motivated the "postcode alone isn't enough" design: two EPC certificates
in postcode NW2 3QB are "40, FLAT 4, SHOOT UP HILL" and "44 SHOOT-UP
HILL" -- different buildings sharing one postcode, so the house number is
load-bearing, not a formality.

Pure functions only, like geo_model.domain.pricing/floor_area -- no DB
session. geo_model.pipeline wires this to the data layer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = [
    "EpcRecord",
    "PpdRecord",
    "MatchedPair",
    "normalize_house_number",
    "normalize_flat_id",
    "match_records",
]

# A leading number (with an optional single trailing letter, e.g. "94A")
# at the start of a string -- the comparable "house number" token.
_HOUSE_NUMBER_RE = re.compile(r"^\s*(\d+[A-Z]?)\b")
# Flat/unit qualifier words to strip before looking for a bare number --
# PPD's SAON and EPC's address1 both use a mix of these.
_FLAT_WORD_RE = re.compile(r"\b(FLAT|APARTMENT|APT|UNIT|MAISONETTE|ROOM)S?\b\.?\s*", re.IGNORECASE)
_FLOOR_WORD_RE = re.compile(r"\b(GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|TOP|BASEMENT)\s+FLOOR\b", re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+[A-Z]?")
# A "clean" flat reference is just an optional flat-word plus a bare
# number, e.g. "FLAT 4", "APARTMENT 12B", or "4" alone -- anything else
# (a floor-word instead of a number, extra punctuation, an unrecognized
# qualifier) still normalizes to a flat id but is flagged fuzzy rather
# than exact, since the text needed real interpretation to get there.
_CLEAN_FLAT_RE = re.compile(r"^(FLAT|APARTMENT|APT|UNIT|MAISONETTE|ROOM)?\.?\s*\d+[A-Z]?$", re.IGNORECASE)


def normalize_house_number(text: str | None) -> str | None:
    """"40" -> "40"; "94A" -> "94A"; "The Cottage" -> None (a named house
    has no number to match on -- out of scope for this join)."""
    if not text:
        return None
    m = _HOUSE_NUMBER_RE.match(text.strip().upper())
    return m.group(1) if m else None


def normalize_flat_id(text: str | None) -> str | None:
    """"FLAT 4" / "APARTMENT 4" / "4" -> "4"; "GROUND FLOOR FLAT" ->
    "GROUND" (no number given, but the floor is still a distinguishing
    token); "" / None -> None (not flat-like at all)."""
    if not text:
        return None
    text = text.strip().upper()
    stripped = _FLAT_WORD_RE.sub("", text).strip()
    m = _NUMBER_RE.search(stripped)
    if m:
        return m.group(0)
    fm = _FLOOR_WORD_RE.search(text)
    if fm:
        return fm.group(1)
    if stripped and len(stripped) <= 3:
        return stripped
    return None


@dataclass(frozen=True)
class EpcRecord:
    dwelling_key: str
    postcode: str
    sector: str
    outcode: str
    property_type: str  # D/S/T/F
    total_floor_area_m2: float
    lodgement_date: object  # datetime.date
    address1: str | None
    address2: str | None


@dataclass(frozen=True)
class PpdRecord:
    transaction_id: str
    postcode: str
    property_type: str  # D/S/T/F/O
    price: int
    date: object  # datetime.date
    district: str
    paon: str | None
    saon: str | None
    street: str | None


@dataclass(frozen=True)
class MatchedPair:
    dwelling_key: str
    transaction_id: str
    sector: str
    outcode: str
    property_type: str  # from the EPC side -- floor area is intrinsically tied to it
    total_floor_area_m2: float
    sale_price: int
    sale_date: object
    district: str
    lodgement_date: object
    confidence: str  # "house_match" | "flat_match_exact" | "flat_match_fuzzy" | "ambiguous"
    match_note: str | None


def _epc_house_number(rec: EpcRecord) -> str | None:
    # When a dwelling has a flat qualifier, EPC puts it in address1 and
    # the house-number+street in address2 (see module docstring example);
    # for a plain house, address1 itself is the house-number+street and
    # address2 is something else (locality) or blank.
    return normalize_house_number(rec.address2) or normalize_house_number(rec.address1)


def _epc_flat_id(rec: EpcRecord) -> str | None:
    return normalize_flat_id(rec.address1)


def match_records(epc_records: list[EpcRecord], ppd_records: list[PpdRecord]) -> list[MatchedPair]:
    """Matches within one postcode's worth of records at a time -- callers
    should pre-partition by postcode (matching across postcodes is never
    correct) and call this per partition, or pass one postcode's records
    at a time. Every PPD sale for a matched dwelling is kept as its own
    MatchedPair (not collapsed to the latest) so the caller can apply the
    same recency-weighted-median-over-a-window approach already used for
    SectorPrice, rather than throwing away real sales history."""
    ppd_by_house: dict[str, list[PpdRecord]] = {}
    for p in ppd_records:
        hn = normalize_house_number(p.paon)
        if hn is None:
            continue  # named house, no number -- out of scope for this join
        ppd_by_house.setdefault(hn, []).append(p)

    epc_by_house: dict[str, list[EpcRecord]] = {}
    for e in epc_records:
        hn = _epc_house_number(e)
        if hn is None:
            continue
        epc_by_house.setdefault(hn, []).append(e)

    results: list[MatchedPair] = []
    for house_number, epc_group in epc_by_house.items():
        ppd_group = ppd_by_house.get(house_number)
        if not ppd_group:
            continue  # dwelling never sold in our ingested Price Paid window

        distinct_ppd_saons = {normalize_flat_id(p.saon) for p in ppd_group}
        if len(epc_group) == 1 and distinct_ppd_saons in ({None}, set()):
            # One EPC dwelling at this house number, and every PPD sale
            # here has no flat qualifier either -- a house, not a flat.
            epc_rec = epc_group[0]
            for ppd_rec in ppd_group:
                results.append(_finalize(epc_rec, ppd_rec, "house_match", None))
            continue

        if len(epc_group) == 1 and distinct_ppd_saons != {None} and distinct_ppd_saons != set():
            # EPC only knows one dwelling here, but Price Paid Data shows
            # this house number has been subdivided (multiple SAONs) --
            # we can't tell which unit the EPC certificate is for.
            epc_rec = epc_group[0]
            for ppd_rec in ppd_group:
                results.append(_finalize(
                    epc_rec, ppd_rec, "ambiguous",
                    "PPD shows multiple flats/units at this house number but EPC has only one dwelling here",
                ))
            continue

        # Multiple EPC dwellings at this house number -- a block of flats.
        # Disambiguate by flat id.
        epc_by_flat: dict[str | None, list[EpcRecord]] = {}
        for e in epc_group:
            epc_by_flat.setdefault(_epc_flat_id(e), []).append(e)

        for ppd_rec in ppd_group:
            flat_id = normalize_flat_id(ppd_rec.saon)
            candidates = epc_by_flat.get(flat_id, []) if flat_id is not None else []
            if flat_id is None:
                # PPD sale has no flat qualifier, but EPC shows several
                # units at this house number -- can't tell which one sold.
                for e in epc_group:
                    results.append(_finalize(e, ppd_rec, "ambiguous", "Multiple EPC dwellings at this house number, but the sale has no flat/unit qualifier to pick one"))
                continue
            if len(candidates) == 1:
                clean = ppd_rec.saon is not None and bool(_CLEAN_FLAT_RE.match(ppd_rec.saon.strip()))
                confidence = "flat_match_exact" if clean else "flat_match_fuzzy"
                results.append(_finalize(candidates[0], ppd_rec, confidence, None))
            elif len(candidates) > 1:
                for e in candidates:
                    results.append(_finalize(e, ppd_rec, "ambiguous", "More than one EPC dwelling normalized to the same flat id"))
            else:
                for e in epc_group:
                    results.append(_finalize(e, ppd_rec, "ambiguous", "Sale's flat id didn't match any EPC dwelling at this house number"))

    return results


def _finalize(epc: EpcRecord, ppd: PpdRecord, confidence: str, note: str | None) -> MatchedPair:
    # A property-type disagreement between the two independently-recorded
    # sides is itself a red flag that the "same house number" match may
    # have crossed to the wrong building (e.g. a flat conversion PPD
    # recorded before/after the EPC's snapshot) -- downgrade rather than
    # silently trust it.
    if confidence != "ambiguous" and ppd.property_type in ("D", "S", "T", "F") and ppd.property_type != epc.property_type:
        confidence = "ambiguous"
        note = f"Property type disagreement: EPC={epc.property_type} PPD={ppd.property_type}"
    return MatchedPair(
        dwelling_key=epc.dwelling_key,
        transaction_id=ppd.transaction_id,
        sector=epc.sector,
        outcode=epc.outcode,
        property_type=epc.property_type,
        total_floor_area_m2=epc.total_floor_area_m2,
        sale_price=ppd.price,
        sale_date=ppd.date,
        district=ppd.district,
        lodgement_date=epc.lodgement_date,
        confidence=confidence,
        match_note=note,
    )
