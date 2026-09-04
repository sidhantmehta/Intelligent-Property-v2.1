"""Pure floor-area estimation logic: median total floor area per postcode
sector + property type, produced from EPC certificates. Deliberately
mirrors geo_model.domain.pricing's sector/outcode backoff shape (same
sparse-cell problem, same fix), but WITHOUT pricing's HPI time-adjustment
or recency weighting -- a dwelling's floor area doesn't change with the
market, so any certificate on file for it is as good as any other. The
one certificate-quality step this module does need is amortised earlier,
by the caller: EPC certificates are re-lodged periodically (efficiency
improvements, remortgages, sales), so geo_model.epc_data dedupes to one
certificate per dwelling (by UPRN, falling back to address) before
records ever reach this module -- otherwise a frequently-reassessed
dwelling would be overweighted in the median the same way an
unweighted price median would double-count a flipped property.
"""
from __future__ import annotations

from dataclasses import dataclass

__all__ = ["FloorAreaRecord", "FloorAreaEstimate", "estimate_sector_floor_areas"]

# Same minimum as pricing.MIN_TRANSACTIONS -- below this many certificates
# in a sector+type cell, back off to the parent outcode's pooled figure
# rather than show a low-confidence number.
MIN_CERTIFICATES = 5
PROPERTY_TYPES = ("D", "S", "T", "F")


@dataclass(frozen=True)
class FloorAreaRecord:
    outcode: str
    sector: str
    property_type: str
    total_floor_area_m2: float


@dataclass
class FloorAreaEstimate:
    key: str  # sector code, or outcode code when this is a fallback estimate
    property_type: str
    median_floor_area_m2: float | None
    certificate_count: int
    grain: str  # "sector" | "outcode" | "none"


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def estimate_sector_floor_areas(records: list[FloorAreaRecord]) -> list[FloorAreaEstimate]:
    """One FloorAreaEstimate per (sector, property_type) present in
    ``records``, backing off to the parent outcode's pooled figure (same
    window/filters, just pooled across every sector in that outcode) when
    a sector's own cell has fewer than MIN_CERTIFICATES."""
    by_sector_type: dict[tuple[str, str], list[float]] = {}
    by_outcode_type: dict[tuple[str, str], list[float]] = {}
    sector_to_outcode: dict[str, str] = {}
    for r in records:
        by_sector_type.setdefault((r.sector, r.property_type), []).append(r.total_floor_area_m2)
        by_outcode_type.setdefault((r.outcode, r.property_type), []).append(r.total_floor_area_m2)
        sector_to_outcode[r.sector] = r.outcode

    estimates: list[FloorAreaEstimate] = []
    for sector, outcode in sector_to_outcode.items():
        for ptype in PROPERTY_TYPES:
            sector_vals = by_sector_type.get((sector, ptype), [])
            if len(sector_vals) >= MIN_CERTIFICATES:
                grain, used = "sector", sector_vals
            else:
                outcode_vals = by_outcode_type.get((outcode, ptype), [])
                if outcode_vals:
                    grain, used = "outcode", outcode_vals
                else:
                    estimates.append(FloorAreaEstimate(
                        key=sector, property_type=ptype, median_floor_area_m2=None,
                        certificate_count=0, grain="none",
                    ))
                    continue
            estimates.append(FloorAreaEstimate(
                key=sector, property_type=ptype,
                median_floor_area_m2=_median(used), certificate_count=len(used), grain=grain,
            ))
    return estimates
