"""Pure price-estimation logic: HPI-adjusting a sale to today's-equivalent
value, recency-weighting, and a sector-level median with an outcode-level
fallback for sparse cells. Like geo_model.domain.scoring, this takes/
returns plain dataclasses only -- no DB session -- per the architecture's
layering rule (geo_model.pipeline is the only place this gets wired to the
data layer).

WHY PRICE/TRAVEL TIME ARE SCORED AT POSTCODE-SECTOR GRAIN BUT AMENITIES
STAY AT OUTCODE GRAIN: real Price Paid Data pulled for this project's
681-outcode scope showed price varying enormously within one outcode even
for the same property type (one real example: Battersea's SW11 1 sector,
terraced houses only, last 2 years -- prices ranged GBP340k to GBP3.65M).
Amenity counts within a radius-bin scheme don't have that same
within-outcode spread (the bins are already coarser than a sector), and
scoring them per-sector would cost ~3-4x more HERE Discover calls for
numbers that would rarely differ from the parent outcode's -- so amenities
stay outcode-level and get reused by every sector inside that outcode
(see geo_model.pipeline.run_model), while price and travel time -- which
DO vary enough to matter -- are computed per sector.

WINDOW/BACKOFF CHOICE, from the same real-data check: at sector grain,
segmented by property type, a 2-3 year window gives a healthy sample
(median 28-54 transactions per (sector,type) cell) with only ~5-19% of
cells sparse (under 5 transactions) -- detached homes are consistently
the sparsest type, since dense sectors simply have few of them. For any
(sector,type) cell under the minimum, this module backs off to that
type's outcode-level aggregate (same window) rather than showing a
low-confidence number from a handful of sales.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

__all__ = [
    "Transaction",
    "PriceEstimate",
    "adjust_to_today",
    "weighted_median",
    "estimate_sector_prices",
]

# Primary aggregation window and sparse-cell threshold -- see module
# docstring for the real-data analysis behind both numbers.
PRIMARY_WINDOW_DAYS = 365 * 3
MIN_TRANSACTIONS = 5
# Recency half-life for the weighted median: a sale this many days old
# carries half the weight of one from today. 365 days means a 3-year-old
# sale (the edge of the primary window) carries ~1/8 the weight of a
# fresh one -- recent sales dominate the median without hard-excluding
# older ones that still passed the window cut.
RECENCY_HALF_LIFE_DAYS = 365.0

PROPERTY_TYPES = ("D", "S", "T", "F")
_INDEX_COLUMN_BY_TYPE = {
    "D": "index_detached", "S": "index_semi", "T": "index_terraced", "F": "index_flat",
}


@dataclass(frozen=True)
class Transaction:
    outcode: str
    sector: str
    property_type: str
    price: int
    date: dt.date
    district: str
    old_new: str  # "Y" | "N"
    ppd_category: str  # "A" | "B"


@dataclass
class PriceEstimate:
    key: str  # sector code, or outcode code when this is a fallback estimate
    property_type: str
    median_price: float | None
    transaction_count: int
    grain: str  # "sector" | "outcode" | "none"


def adjust_to_today(
    price: int,
    sale_date: dt.date,
    district: str,
    property_type: str,
    hpi_by_district_month: dict[tuple[str, dt.date], dict[str, float | None]],
    as_of_month: dt.date,
) -> float | None:
    """Scales ``price`` from ``sale_date`` to an as-of-``as_of_month``
    equivalent using the ratio of that district's HPI index at the two
    months (type-specific index if present, else the district's overall
    index). None if neither index is available for that district/month --
    the caller should drop the transaction from the estimate rather than
    use an un-adjusted price, since an old un-adjusted price is exactly
    the staleness problem this module exists to avoid."""
    sale_month = sale_date.replace(day=1)
    sale_row = hpi_by_district_month.get((district, sale_month))
    as_of_row = hpi_by_district_month.get((district, as_of_month))
    if not sale_row or not as_of_row:
        return None

    col = _INDEX_COLUMN_BY_TYPE.get(property_type)
    sale_index = (sale_row.get(col) if col else None) or sale_row.get("index_all")
    as_of_index = (as_of_row.get(col) if col else None) or as_of_row.get("index_all")
    if not sale_index or not as_of_index:
        return None

    return price * (as_of_index / sale_index)


def weighted_median(values_with_weights: list[tuple[float, float]]) -> float | None:
    """Standard weighted median: sort by value, walk cumulative weight,
    return the value where it first reaches half the total weight."""
    if not values_with_weights:
        return None
    ordered = sorted(values_with_weights, key=lambda vw: vw[0])
    total = sum(w for _, w in ordered)
    if total <= 0:
        return None
    half = total / 2
    cumulative = 0.0
    for value, weight in ordered:
        cumulative += weight
        if cumulative >= half:
            return value
    return ordered[-1][0]


def _recency_weight(sale_date: dt.date, as_of: dt.date) -> float:
    days_ago = max(0, (as_of - sale_date).days)
    return 0.5 ** (days_ago / RECENCY_HALF_LIFE_DAYS)


def _comparable(transactions: list[Transaction]) -> list[Transaction]:
    """The "clean" comparable-sale filter: standard sales only (excludes
    repossessions/BTL-panel entries under ppd_category B, which are
    noisier), and resales only (excludes new-build, which carries a
    builder premium over general market value)."""
    return [t for t in transactions if t.ppd_category == "A" and t.old_new == "N"]


def _adjusted_weighted_prices(
    transactions: list[Transaction],
    hpi_by_district_month: dict[tuple[str, dt.date], dict[str, float | None]],
    as_of_month: dt.date,
) -> list[tuple[float, float]]:
    out = []
    for t in transactions:
        adjusted = adjust_to_today(t.price, t.date, t.district, t.property_type, hpi_by_district_month, as_of_month)
        if adjusted is None:
            continue
        out.append((adjusted, _recency_weight(t.date, as_of_month.replace(day=28))))
    return out


def estimate_sector_prices(
    transactions: list[Transaction],
    hpi_by_district_month: dict[tuple[str, dt.date], dict[str, float | None]],
    as_of_month: dt.date,
    now: dt.date,
) -> list[PriceEstimate]:
    """Computes one PriceEstimate per (sector, property_type) present in
    ``transactions``, backing off to the parent outcode's estimate (same
    window/filters, just pooled across every sector in that outcode) when
    a sector's own cell has fewer than MIN_TRANSACTIONS. ``transactions``
    should already be pre-filtered to the primary window by the caller
    (kept out of this function so it stays a pure aggregation step,
    testable independent of "what is today").
    """
    comparable = _comparable(transactions)

    by_sector_type: dict[tuple[str, str], list[Transaction]] = {}
    by_outcode_type: dict[tuple[str, str], list[Transaction]] = {}
    sector_to_outcode: dict[str, str] = {}
    for t in comparable:
        by_sector_type.setdefault((t.sector, t.property_type), []).append(t)
        by_outcode_type.setdefault((t.outcode, t.property_type), []).append(t)
        sector_to_outcode[t.sector] = t.outcode

    estimates: list[PriceEstimate] = []
    for sector, outcode in sector_to_outcode.items():
        for ptype in PROPERTY_TYPES:
            sector_txns = by_sector_type.get((sector, ptype), [])
            if len(sector_txns) >= MIN_TRANSACTIONS:
                grain, used = "sector", sector_txns
            else:
                outcode_txns = by_outcode_type.get((outcode, ptype), [])
                if len(outcode_txns) >= MIN_TRANSACTIONS:
                    grain, used = "outcode", outcode_txns
                elif outcode_txns:
                    grain, used = "outcode", outcode_txns  # below minimum but it's what we have
                else:
                    estimates.append(PriceEstimate(key=sector, property_type=ptype, median_price=None, transaction_count=0, grain="none"))
                    continue

            weighted = _adjusted_weighted_prices(used, hpi_by_district_month, as_of_month)
            median = weighted_median(weighted)
            estimates.append(PriceEstimate(
                key=sector, property_type=ptype, median_price=median,
                transaction_count=len(used), grain=grain if median is not None else "none",
            ))

    return estimates
