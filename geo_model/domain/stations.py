"""Finds the N closest rail stations to each postcode sector's centroid --
pure geometry, no DB session (geo_model.pipeline wires this to the data
layer, same layering as pricing.py/floor_area.py).

Kept as "N closest," not just the nearest, because a homebuyer comparing
commute cost cares about alternatives: a station a few minutes further
away can sit in a cheaper fare zone or on a different operator's route.
See geo_model.rail_stations for where the station list comes from
(NaPTAN) and the project's fares-feed plan for what eventually prices
each of these.
"""
from __future__ import annotations

from dataclasses import dataclass

from geo_model.geo_math import haversine_miles

__all__ = [
    "StationRecord",
    "SectorCentroid",
    "SectorStationMatch",
    "NEAREST_STATION_COUNT",
    "nearest_stations",
]

# How many candidate stations to keep per sector. Chosen as "enough to
# compare a couple of realistic alternatives" without ballooning the
# sector_stations table -- not a distance radius, since a radius returns
# zero for sparse rural sectors and dozens for dense London ones.
NEAREST_STATION_COUNT = 5


@dataclass(frozen=True)
class StationRecord:
    atco_code: str
    name: str
    lat: float
    long: float


@dataclass(frozen=True)
class SectorCentroid:
    sector: str
    lat: float
    long: float


@dataclass(frozen=True)
class SectorStationMatch:
    sector: str
    station_atco_code: str
    station_name: str
    distance_miles: float
    rank: int  # 1 = closest


def nearest_stations(
    sectors: list[SectorCentroid],
    stations: list[StationRecord],
    n: int = NEAREST_STATION_COUNT,
) -> list[SectorStationMatch]:
    """One SectorStationMatch per (sector, station) for each sector's `n`
    closest stations, ranked 1 (closest) to `n`. O(sectors x stations)
    haversine calls -- at GB-station scale (~2,700) and our sector scope
    (~2,400) that's a few million cheap calls, not worth a spatial index."""
    if not stations:
        return []
    results: list[SectorStationMatch] = []
    for sector in sectors:
        with_distance = [
            (haversine_miles(sector.lat, sector.long, st.lat, st.long), st) for st in stations
        ]
        with_distance.sort(key=lambda pair: pair[0])
        for rank, (distance, station) in enumerate(with_distance[:n], start=1):
            results.append(SectorStationMatch(
                sector=sector.sector, station_atco_code=station.atco_code,
                station_name=station.name, distance_miles=distance, rank=rank,
            ))
    return results
