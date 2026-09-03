"""Pure scoring logic: haversine distance, radius-bin weighting, and
per-category + travel-time aggregation into a per-outcode total_score.

Deliberately takes/returns plain dataclasses only -- no DB session, no HTTP
call -- so the whole thing is unit-testable without a database or network,
per the architecture's layering rule (geo_model.pipeline is the only place
that wires this together with the data/provider layers).

Every amenity category AND every travel-time reference point is scored and
normalized independently (0-1 across the outcodes being scored) and only
then blended into total_score by its configured weight. That's a deliberate
difference from v2's matching_engine_module, which blended categories into
one combined number *before* normalizing -- which threw away the
per-category breakdown v3 needs to show what's weak/strong for a given
outcode. Keeping every category's own raw/normalized score is what makes
that breakdown possible.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    h = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 3958.8 * 2 * math.asin(math.sqrt(h))


def radius_weight(distance_miles: float, bins_miles: list[float]) -> float:
    """1/bin_upper for the bin ``distance_miles`` falls into (closer =
    higher weight); 0.0 if beyond the outermost bin. E.g. with bins
    [0,1,3,5,10]: 0.6 miles -> 1/1, 2 miles -> 1/3, 12 miles -> 0.0.
    Same shape as v2's matching_engine_module radius weighting."""
    for i in range(1, len(bins_miles)):
        if distance_miles <= bins_miles[i]:
            return 1.0 / bins_miles[i]
    return 0.0


@dataclass(frozen=True)
class AmenityRecord:
    outcode: str
    category_key: str
    distance_miles: float


@dataclass(frozen=True)
class TravelTimeRecord:
    outcode: str
    reference_point_name: str
    minutes: float


@dataclass(frozen=True)
class CategoryWeight:
    key: str
    weight: float


@dataclass
class CategoryScore:
    category_key: str
    raw_score: float
    normalized_score: float
    weight_applied: float


@dataclass
class OutcodeScore:
    outcode: str
    total_score: float
    categories: list[CategoryScore] = field(default_factory=list)


_TRAVEL_TIME_PREFIX = "travel_time::"


def travel_time_category_key(reference_point_name: str) -> str:
    """Travel time to a reference point is scored/stored/displayed exactly
    like an amenity category (its own row, its own weight, its own column
    in the results table) -- this namespaced key is how the two are told
    apart without needing a second code path anywhere downstream."""
    return f"{_TRAVEL_TIME_PREFIX}{reference_point_name}"


def is_travel_time_category(category_key: str) -> bool:
    return category_key.startswith(_TRAVEL_TIME_PREFIX)


def _min_max_normalize(values: dict[str, float], higher_is_better: bool) -> dict[str, float]:
    if not values:
        return {}
    lo, hi = min(values.values()), max(values.values())
    if math.isclose(hi, lo):
        return {k: 1.0 for k in values}
    normalized = {k: (v - lo) / (hi - lo) for k, v in values.items()}
    if not higher_is_better:
        normalized = {k: 1.0 - v for k, v in normalized.items()}
    return normalized


def score_outcodes(
    outcodes: list[str],
    amenities: list[AmenityRecord],
    category_weights: list[CategoryWeight],
    radius_bins_miles: list[float],
    travel_times: list[TravelTimeRecord],
    reference_weights: list[CategoryWeight],
) -> list[OutcodeScore]:
    """Compute a total_score and per-category breakdown for every outcode.

    ``category_weights`` covers amenity categories; ``reference_weights``
    covers travel-time reference points (keyed by reference point name --
    wrapped internally via travel_time_category_key). An outcode with no
    amenities in a category, or no travel-time record for a reference
    point, still gets a row for it with raw_score 0 / minutes treated as
    "no data", so every outcode has a value for every column.
    """
    # --- amenity categories: raw score per (outcode, category) ---
    raw_by_category: dict[str, dict[str, float]] = {cw.key: {o: 0.0 for o in outcodes} for cw in category_weights}
    for a in amenities:
        if a.category_key not in raw_by_category or a.outcode not in raw_by_category[a.category_key]:
            continue
        raw_by_category[a.category_key][a.outcode] += radius_weight(a.distance_miles, radius_bins_miles)

    normalized_by_category: dict[str, dict[str, float]] = {
        key: _min_max_normalize(raw, higher_is_better=True) for key, raw in raw_by_category.items()
    }

    # --- travel time reference points: raw = minutes (missing -> None) ---
    minutes_by_ref: dict[str, dict[str, float | None]] = {
        rw.key: {o: None for o in outcodes} for rw in reference_weights
    }
    for t in travel_times:
        if t.reference_point_name not in minutes_by_ref or t.outcode not in minutes_by_ref[t.reference_point_name]:
            continue
        minutes_by_ref[t.reference_point_name][t.outcode] = t.minutes

    normalized_by_ref: dict[str, dict[str, float]] = {}
    for rw in reference_weights:
        known_minutes = {o: m for o, m in minutes_by_ref[rw.key].items() if m is not None}
        normalized_known = _min_max_normalize(known_minutes, higher_is_better=False)
        # Outcodes with no travel-time record get the worst score (0.0)
        # rather than being silently excluded from the blend.
        normalized_by_ref[rw.key] = {o: normalized_known.get(o, 0.0) for o in outcodes}

    # --- blend into total_score per outcode, keeping every category row ---
    total_weight = sum(cw.weight for cw in category_weights) + sum(rw.weight for rw in reference_weights)
    results: list[OutcodeScore] = []
    for outcode in outcodes:
        categories: list[CategoryScore] = []
        weighted_sum = 0.0
        for cw in category_weights:
            normalized = normalized_by_category[cw.key][outcode]
            categories.append(
                CategoryScore(
                    category_key=cw.key,
                    raw_score=raw_by_category[cw.key][outcode],
                    normalized_score=normalized,
                    weight_applied=cw.weight,
                )
            )
            weighted_sum += normalized * cw.weight
        for rw in reference_weights:
            normalized = normalized_by_ref[rw.key][outcode]
            raw_minutes = minutes_by_ref[rw.key][outcode]
            categories.append(
                CategoryScore(
                    category_key=travel_time_category_key(rw.key),
                    raw_score=raw_minutes if raw_minutes is not None else -1.0,
                    normalized_score=normalized,
                    weight_applied=rw.weight,
                )
            )
            weighted_sum += normalized * rw.weight

        total_score = weighted_sum / total_weight if total_weight > 0 else 0.0
        results.append(OutcodeScore(outcode=outcode, total_score=total_score, categories=categories))

    return results
