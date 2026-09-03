#!/usr/bin/env python3
"""Seeds geo_model.db with a small, clearly-illustrative set of central
London outcodes and runs the model against them using a synthetic
provider (no real HERE/postcodes.io calls -- this sandbox's network
policy blocks both), so the Artifact frontend has something real to
render on first publish. Every doc this produces is exported with
is_example=True and the frontend shows an "Example data" banner because
of it -- this is NOT a substitute for a real refresh-geo-data/run-model
pass once HERE/postcodes.io are reachable.

Centroids below are approximate (general knowledge, not postcodes.io --
which is unreachable from here) and only good enough for illustration.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from geo_model import pipeline  # noqa: E402
from geo_model.config import load_model_config  # noqa: E402
from geo_model.data.db import get_session  # noqa: E402
from geo_model.data.models import Outcode  # noqa: E402
from geo_model.providers.base import AmenityResult, GeoPoint, GeoProvider  # noqa: E402

# Approximate central-London centroids (illustration only).
EXAMPLE_OUTCODES = {
    "EC4A": (51.5158, -0.1075),  # City / Fleet Street -- confirmed real centroid
    "SE1": (51.5045, -0.0865),   # Southwark / Bankside
    "E1": (51.5154, -0.0708),    # Whitechapel
    "E14": (51.5054, -0.0235),   # Canary Wharf
    "N1": (51.5362, -0.1033),    # Islington
    "W1": (51.5142, -0.1494),    # Marylebone / Mayfair
    "SW1E": (51.4965, -0.1447),  # Victoria
    "NW1": (51.5290, -0.1450),   # Regent's Park / Camden
}
CENTER = "EC4A"


class ExampleProvider(GeoProvider):
    """Deterministic, illustration-only: amenity density and travel time
    both scale with distance from CENTER, so central outcodes plausibly
    score higher -- same pattern real data would show, without claiming
    to BE real data."""

    name = "example"

    def geocode(self, address):
        return GeoPoint(lat=51.5072, long=-0.1276)  # central London, good enough for illustration

    def nearby_amenities(self, origin, query, radius_miles, limit):
        cx, cy = EXAMPLE_OUTCODES[CENTER]
        dist = ((origin.lat - cx) ** 2 + (origin.long - cy) ** 2) ** 0.5
        density = max(1, int(6 - dist * 40))
        seed = abs(hash((round(origin.lat, 4), round(origin.long, 4), query))) % 1000
        count = max(1, min(limit, density + seed % 3))
        return [
            AmenityResult(
                title=f"Example {query} #{i + 1}",
                address=None,
                lat=origin.lat + 0.001 * i,
                long=origin.long + 0.001 * i,
                distance_m=200.0 + i * 300 + (seed % 400),
            )
            for i in range(count)
        ]

    def travel_time_minutes(self, origin, destination, mode):
        cx, cy = EXAMPLE_OUTCODES[CENTER]
        dist = ((origin.lat - cx) ** 2 + (origin.long - cy) ** 2) ** 0.5
        return round(8 + dist * 250, 1)


def main() -> None:
    pipeline.ensure_db_ready()
    with get_session() as session:
        for outcode, (lat, long_) in EXAMPLE_OUTCODES.items():
            existing = session.get(Outcode, outcode)
            if existing:
                existing.lat, existing.long = lat, long_
            else:
                session.add(Outcode(outcode=outcode, lat=lat, long=long_))

    pipeline.build_provider = lambda config: ExampleProvider()
    config = load_model_config()
    result = pipeline.run_model(config, outcode_filter=list(EXAMPLE_OUTCODES.keys()))
    print(result)


if __name__ == "__main__":
    main()
