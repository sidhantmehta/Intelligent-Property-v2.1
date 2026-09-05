"""Aggregates MatchedPropertySale rows (a real EPC floor area paired with
its own Price Paid Data sale, from geo_model.domain.address_match) into a
median price-per-m2 per (sector, property_type) -- and, where there's
enough data, a breakdown by 100m2 size band.

This replaces dividing two independently-computed medians (SectorPrice's
median_price / SectorFloorArea's median_floor_area_m2) with a genuine
per-dwelling ratio, aggregated the same way SectorPrice already is:
HPI-adjusted to today, recency-weighted, sector median with an
outcode-level fallback for sparse cells. Reuses geo_model.domain.pricing's
adjust_to_today/weighted_median directly rather than re-deriving them, so
the two price methodologies can never quietly drift apart.

Pure functions only -- no DB session; geo_model.pipeline wires this to
the data layer, same layering as pricing.py/floor_area.py.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from geo_model.domain.pricing import RECENCY_HALF_LIFE_DAYS, adjust_to_today, weighted_median

__all__ = [
    "MatchedSale",
    "PricePerSqmEstimate",
    "USABLE_CONFIDENCE",
    "MIN_MATCHED",
    "MIN_MATCHED_FOR_BIN",
    "SIZE_BIN_WIDTH_M2",
    "size_bin_of",
    "estimate_price_per_sqm",
]

# Only these confidence tiers ever feed an estimate -- "ambiguous" rows
# exist in the DB for troubleshooting (see address_match.py) but are
# never aggregated into a number someone might rely on.
USABLE_CONFIDENCE = ("house_match", "flat_match_exact", "flat_match_fuzzy")

# Mirrors geo_model.domain.pricing's MIN_TRANSACTIONS -- same sparse-cell
# reasoning, applied to matched pairs instead of raw transactions.
MIN_MATCHED = 5
# A size-bin breakdown is additional, optional detail on top of the
# overall sector+type figure -- only shown where a bin clears its own
# (same) minimum; a sparse bin is simply omitted, not backed off to the
# outcode, since "the outcode's 300-400m2 detached houses" is already a
# stretch for a figure meant to describe one sector.
MIN_MATCHED_FOR_BIN = 5
SIZE_BIN_WIDTH_M2 = 100

PROPERTY_TYPES = ("D", "S", "T", "F")


@dataclass(frozen=True)
class MatchedSale:
    sector: str
    outcode: str
    property_type: str
    total_floor_area_m2: float
    sale_price: int
    sale_date: dt.date
    district: str
    confidence: str


@dataclass
class PricePerSqmEstimate:
    key: str  # sector code, or outcode code when this is a fallback estimate
    property_type: str
    size_bin_m2: int | None  # None = the overall (not size-broken-out) figure
    median_price_per_sqm: float | None
    matched_count: int
    grain: str  # "sector" | "outcode" | "none"


def size_bin_of(total_floor_area_m2: float) -> int:
    """110.0 -> 100; 249.9 -> 200. Bins are labelled by their lower bound."""
    return int(total_floor_area_m2 // SIZE_BIN_WIDTH_M2) * SIZE_BIN_WIDTH_M2


def _recency_weight(sale_date: dt.date, as_of: dt.date) -> float:
    days_ago = max(0, (as_of - sale_date).days)
    return 0.5 ** (days_ago / RECENCY_HALF_LIFE_DAYS)


def _weighted_prices_per_sqm(
    sales: list[MatchedSale],
    hpi_by_district_month: dict[tuple[str, dt.date], dict[str, float | None]],
    as_of_month: dt.date,
) -> list[tuple[float, float]]:
    out = []
    for s in sales:
        if s.total_floor_area_m2 <= 0:
            continue
        adjusted = adjust_to_today(s.sale_price, s.sale_date, s.district, s.property_type, hpi_by_district_month, as_of_month)
        if adjusted is None:
            continue
        out.append((adjusted / s.total_floor_area_m2, _recency_weight(s.sale_date, as_of_month.replace(day=28))))
    return out


def estimate_price_per_sqm(
    sales: list[MatchedSale],
    hpi_by_district_month: dict[tuple[str, dt.date], dict[str, float | None]],
    as_of_month: dt.date,
) -> list[PricePerSqmEstimate]:
    """One overall PricePerSqmEstimate (size_bin_m2=None) per (sector,
    property_type) present in ``sales``, with the outcode-level fallback
    for sparse cells -- plus zero or more size-bin estimates (sector grain
    only, no fallback) where a bin clears MIN_MATCHED_FOR_BIN. ``sales``
    should already be pre-filtered to confidence in USABLE_CONFIDENCE and
    the primary time window by the caller, same convention as
    pricing.estimate_sector_prices."""
    by_sector_type: dict[tuple[str, str], list[MatchedSale]] = {}
    by_outcode_type: dict[tuple[str, str], list[MatchedSale]] = {}
    by_sector_type_bin: dict[tuple[str, str, int], list[MatchedSale]] = {}
    sector_to_outcode: dict[str, str] = {}
    for s in sales:
        by_sector_type.setdefault((s.sector, s.property_type), []).append(s)
        by_outcode_type.setdefault((s.outcode, s.property_type), []).append(s)
        by_sector_type_bin.setdefault((s.sector, s.property_type, size_bin_of(s.total_floor_area_m2)), []).append(s)
        sector_to_outcode[s.sector] = s.outcode

    estimates: list[PricePerSqmEstimate] = []
    for sector, outcode in sector_to_outcode.items():
        for ptype in PROPERTY_TYPES:
            sector_sales = by_sector_type.get((sector, ptype), [])
            if len(sector_sales) >= MIN_MATCHED:
                grain, used = "sector", sector_sales
            else:
                outcode_sales = by_outcode_type.get((outcode, ptype), [])
                if outcode_sales:
                    grain, used = "outcode", outcode_sales
                else:
                    estimates.append(PricePerSqmEstimate(
                        key=sector, property_type=ptype, size_bin_m2=None,
                        median_price_per_sqm=None, matched_count=0, grain="none",
                    ))
                    continue

            weighted = _weighted_prices_per_sqm(used, hpi_by_district_month, as_of_month)
            median = weighted_median(weighted)
            estimates.append(PricePerSqmEstimate(
                key=sector, property_type=ptype, size_bin_m2=None,
                median_price_per_sqm=median, matched_count=len(used),
                grain=grain if median is not None else "none",
            ))

            for (bin_sector, bin_ptype, bin_lo), bin_sales in by_sector_type_bin.items():
                if bin_sector != sector or bin_ptype != ptype or len(bin_sales) < MIN_MATCHED_FOR_BIN:
                    continue
                bin_weighted = _weighted_prices_per_sqm(bin_sales, hpi_by_district_month, as_of_month)
                bin_median = weighted_median(bin_weighted)
                if bin_median is None:
                    continue
                estimates.append(PricePerSqmEstimate(
                    key=sector, property_type=ptype, size_bin_m2=bin_lo,
                    median_price_per_sqm=bin_median, matched_count=len(bin_sales), grain="sector",
                ))

    return estimates
