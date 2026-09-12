from geo_model.domain import stations


def _sector(sector, lat, long):
    return stations.SectorCentroid(sector=sector, lat=lat, long=long)


def _station(code, name, lat, long):
    return stations.StationRecord(atco_code=code, name=name, lat=lat, long=long)


def test_ranks_by_distance_closest_first():
    sector = _sector("SW1A 1", 51.5010, -0.1416)  # near Victoria
    far = _station("9100FAR", "Far Away", 55.0, -3.0)  # Scotland-ish
    near = _station("9100VIC", "Victoria", 51.4952, -0.1441)
    mid = _station("9100PIM", "Pimlico-ish", 51.49, -0.13)
    result = stations.nearest_stations([sector], [far, near, mid], n=3)
    assert [r.station_atco_code for r in result] == ["9100VIC", "9100PIM", "9100FAR"]
    assert [r.rank for r in result] == [1, 2, 3]
    assert result[0].distance_miles < result[1].distance_miles < result[2].distance_miles


def test_caps_at_n():
    sector = _sector("SW1A 1", 51.50, -0.14)
    many = [_station(f"9100S{i}", f"Station {i}", 51.50 + i * 0.01, -0.14) for i in range(10)]
    result = stations.nearest_stations([sector], many, n=5)
    assert len(result) == 5
    assert [r.rank for r in result] == [1, 2, 3, 4, 5]


def test_multiple_sectors_each_get_their_own_ranking():
    sector_a = _sector("SW1A 1", 51.50, -0.14)
    sector_b = _sector("EC1A 1", 51.52, -0.10)
    st_near_a = _station("9100A", "Near A", 51.501, -0.141)
    st_near_b = _station("9100B", "Near B", 51.521, -0.101)
    result = stations.nearest_stations([sector_a, sector_b], [st_near_a, st_near_b], n=1)
    by_sector = {r.sector: r.station_atco_code for r in result}
    assert by_sector == {"SW1A 1": "9100A", "EC1A 1": "9100B"}


def test_no_stations_returns_empty():
    assert stations.nearest_stations([_sector("SW1A 1", 51.5, -0.14)], [], n=5) == []
