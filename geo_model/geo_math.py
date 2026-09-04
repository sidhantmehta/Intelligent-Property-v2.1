"""Pure geographic math with zero dependencies on any other geo_model
module -- lives outside both providers/ and domain/ specifically so either
can import it without creating a layering cycle (providers must not depend
on domain; domain must not depend on providers).
"""
from __future__ import annotations

import math


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    h = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 3958.8 * 2 * math.asin(math.sqrt(h))
