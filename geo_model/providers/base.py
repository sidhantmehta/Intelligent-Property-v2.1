"""Provider abstraction -- the only layer allowed to make outbound HTTP
calls. Nothing above this layer (geo_model.domain, geo_model.pipeline)
knows provider-specific detail: endpoints, auth, category taxonomies. That
keeps adding a second provider (Google, TfL, OS Places, OpenRouteService)
a matter of writing one new class here, with zero changes to scoring or
caching logic.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class GeoPoint:
    lat: float
    long: float


@dataclass(frozen=True)
class AmenityResult:
    title: str
    address: str | None
    lat: float
    long: float
    distance_m: float | None


class GeoProvider(ABC):
    """A geo/places/routing data source."""

    name: str

    @abstractmethod
    def geocode(self, address: str) -> GeoPoint | None:
        """Resolve a free-text address to a lat/long, or None if it can't
        be resolved."""

    @abstractmethod
    def nearby_amenities(
        self,
        origin: GeoPoint,
        query: str,
        radius_miles: float,
        limit: int,
    ) -> list[AmenityResult]:
        """Search for places matching ``query`` near ``origin``, ordered
        by distance, within ``radius_miles``, capped to ``limit`` results.
        Returns an empty list (never raises) on a request that succeeds
        but finds nothing; raises on a genuine request failure so the
        caller (geo_model.domain.geo_cache) can log/retry/skip."""

    @abstractmethod
    def travel_time_minutes(
        self,
        origin: GeoPoint,
        destination: GeoPoint,
        mode: str,
    ) -> float | None:
        """Travel time in minutes from origin to destination for the given
        mode, or None if no route could be found."""
