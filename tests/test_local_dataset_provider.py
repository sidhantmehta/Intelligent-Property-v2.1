from geo_model.providers.base import GeoPoint
from geo_model.providers.local_dataset import LocalDatasetProvider


def _provider():
    return LocalDatasetProvider(
        {
            "private_schools": [
                {"title": "Near School", "address": "1 Close St", "lat": 51.500, "long": -0.100},
                {"title": "Mid School", "address": "2 Mid Rd", "lat": 51.520, "long": -0.100},
                {"title": "Far School", "address": "3 Far Ave", "lat": 51.900, "long": -0.100},
            ]
        }
    )


def test_filters_by_radius_and_sorts_by_distance():
    provider = _provider()
    origin = GeoPoint(lat=51.500, long=-0.100)
    results = provider.nearby_amenities(origin, "private_schools", radius_miles=5, limit=10)
    assert [r.title for r in results] == ["Near School", "Mid School"]
    assert results[0].distance_m < results[1].distance_m


def test_respects_limit():
    provider = _provider()
    origin = GeoPoint(lat=51.500, long=-0.100)
    results = provider.nearby_amenities(origin, "private_schools", radius_miles=50, limit=1)
    assert len(results) == 1
    assert results[0].title == "Near School"


def test_unknown_dataset_key_returns_empty():
    provider = _provider()
    origin = GeoPoint(lat=51.500, long=-0.100)
    assert provider.nearby_amenities(origin, "not_a_real_dataset", radius_miles=50, limit=10) == []


def test_geocode_and_travel_time_not_supported():
    provider = _provider()
    try:
        provider.geocode("anywhere")
        assert False, "expected NotImplementedError"
    except NotImplementedError:
        pass
    try:
        provider.travel_time_minutes(GeoPoint(0, 0), GeoPoint(1, 1), "car")
        assert False, "expected NotImplementedError"
    except NotImplementedError:
        pass


def test_no_usage_log_by_default():
    provider = _provider()
    assert provider.get_usage_log() == []
