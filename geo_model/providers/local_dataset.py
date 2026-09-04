"""A GeoProvider backed by a fixed, pre-geocoded local dataset instead of
an external API -- e.g. the private schools register, which has its own
real, permanent coordinates rather than being discovered per outcode the
way HERE's Discover results are.

Implements the same nearby_amenities() contract as HereMapsProvider (scan,
filter by radius, sort by distance, cap to limit) so geo_model.pipeline and
geo_model.domain.scoring treat a local dataset exactly like an API-backed
one -- no special-casing anywhere above this layer. geocode() and
travel_time_minutes() are intentionally unsupported: a local dataset
provider is only ever assigned to amenity categories, never used for
reference-point geocoding or travel time.
"""
from __future__ import annotations

from geo_model.geo_math import haversine_miles
from geo_model.providers.base import AmenityResult, GeoPoint, GeoProvider


class LocalDatasetProvider(GeoProvider):
    name = "local_dataset"

    def __init__(self, datasets: dict[str, list[dict]]):
        """``datasets``: dataset key -> list of plain dicts, each with at
        least title/lat/long/address. geo_model.pipeline builds this from
        whatever local tables are relevant (currently just PrivateSchool)."""
        self._datasets = datasets

    def geocode(self, address: str) -> GeoPoint | None:
        raise NotImplementedError("LocalDatasetProvider does not geocode addresses -- assign it to amenity categories only")

    def travel_time_minutes(self, origin: GeoPoint, destination: GeoPoint, mode: str) -> float | None:
        raise NotImplementedError("LocalDatasetProvider does not compute travel times -- assign it to amenity categories only")

    def nearby_amenities(
        self,
        origin: GeoPoint,
        query: str,
        radius_miles: float,
        limit: int,
    ) -> list[AmenityResult]:
        """``query`` selects which dataset to scan (e.g. "private_schools"),
        mirroring how HERE's `query` selects what to search for."""
        entries = self._datasets.get(query, [])
        results = []
        for entry in entries:
            distance_miles = haversine_miles(origin.lat, origin.long, entry["lat"], entry["long"])
            if distance_miles > radius_miles:
                continue
            results.append((distance_miles, entry))

        results.sort(key=lambda pair: pair[0])
        return [
            AmenityResult(
                title=entry["title"],
                address=entry.get("address"),
                lat=entry["lat"],
                long=entry["long"],
                distance_m=distance_miles * 1609.34,
            )
            for distance_miles, entry in results[:limit]
        ]
