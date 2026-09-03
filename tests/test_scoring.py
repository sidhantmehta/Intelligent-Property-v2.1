import math

from geo_model.domain import geo_cache, scoring
import datetime as dt


def test_haversine_known_distance():
    # London (Bank) to Canary Wharf, roughly 3.6 miles.
    d = scoring.haversine_miles(51.5134, -0.0890, 51.5039, -0.0186)
    assert 3.0 < d < 4.2


def test_radius_weight_buckets():
    bins = [0, 1, 3, 5, 10]
    assert scoring.radius_weight(0.5, bins) == 1.0
    assert math.isclose(scoring.radius_weight(2, bins), 1 / 3)
    assert math.isclose(scoring.radius_weight(4, bins), 1 / 5)
    assert math.isclose(scoring.radius_weight(9, bins), 1 / 10)
    assert scoring.radius_weight(11, bins) == 0.0


def test_score_outcodes_amenity_dense_outcode_scores_higher():
    amenities = [
        scoring.AmenityRecord(outcode="AA1", category_key="cafes", distance_miles=0.2),
        scoring.AmenityRecord(outcode="AA1", category_key="cafes", distance_miles=0.5),
        scoring.AmenityRecord(outcode="AA1", category_key="cafes", distance_miles=0.8),
        scoring.AmenityRecord(outcode="BB2", category_key="cafes", distance_miles=9.5),
    ]
    results = scoring.score_outcodes(
        outcodes=["AA1", "BB2"],
        amenities=amenities,
        category_weights=[scoring.CategoryWeight(key="cafes", weight=3)],
        radius_bins_miles=[0, 1, 3, 5, 10],
        travel_times=[],
        reference_weights=[],
    )
    by_outcode = {r.outcode: r for r in results}
    assert by_outcode["AA1"].total_score > by_outcode["BB2"].total_score
    assert by_outcode["AA1"].categories[0].normalized_score == 1.0
    assert by_outcode["BB2"].categories[0].normalized_score == 0.0


def test_score_outcodes_shorter_travel_time_scores_higher():
    travel_times = [
        scoring.TravelTimeRecord(outcode="AA1", reference_point_name="Office", minutes=10),
        scoring.TravelTimeRecord(outcode="BB2", reference_point_name="Office", minutes=60),
    ]
    results = scoring.score_outcodes(
        outcodes=["AA1", "BB2"],
        amenities=[],
        category_weights=[],
        radius_bins_miles=[0, 1, 3, 5, 10],
        travel_times=travel_times,
        reference_weights=[scoring.CategoryWeight(key="Office", weight=5)],
    )
    by_outcode = {r.outcode: r for r in results}
    assert by_outcode["AA1"].total_score > by_outcode["BB2"].total_score
    travel_key = scoring.travel_time_category_key("Office")
    assert scoring.is_travel_time_category(travel_key)
    aa1_travel = next(c for c in by_outcode["AA1"].categories if c.category_key == travel_key)
    assert aa1_travel.normalized_score == 1.0
    assert aa1_travel.raw_score == 10


def test_score_outcodes_missing_travel_time_scores_worst():
    travel_times = [
        scoring.TravelTimeRecord(outcode="AA1", reference_point_name="Office", minutes=10),
    ]
    results = scoring.score_outcodes(
        outcodes=["AA1", "BB2"],
        amenities=[],
        category_weights=[],
        radius_bins_miles=[0, 1, 3, 5, 10],
        travel_times=travel_times,
        reference_weights=[scoring.CategoryWeight(key="Office", weight=5)],
    )
    by_outcode = {r.outcode: r for r in results}
    travel_key = scoring.travel_time_category_key("Office")
    bb2_travel = next(c for c in by_outcode["BB2"].categories if c.category_key == travel_key)
    assert bb2_travel.normalized_score == 0.0
    assert bb2_travel.raw_score == -1.0


def test_geo_cache_missing_and_stale():
    now = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    fresh = now - dt.timedelta(days=10)
    stale = now - dt.timedelta(days=200)
    cache = {
        geo_cache.CacheKey("AA1", "cafes"): fresh,
        geo_cache.CacheKey("AA1", "schools"): stale,
    }
    missing_only = geo_cache.keys_missing_only(["AA1", "BB2"], ["cafes", "schools"], cache)
    assert geo_cache.CacheKey("BB2", "cafes") in missing_only
    assert geo_cache.CacheKey("BB2", "schools") in missing_only
    assert geo_cache.CacheKey("AA1", "cafes") not in missing_only
    assert geo_cache.CacheKey("AA1", "schools") not in missing_only  # stale, but not "missing"

    needing_refresh = geo_cache.keys_needing_refresh(
        ["AA1", "BB2"], ["cafes", "schools"], cache, staleness_days=180, now=now, force=False
    )
    assert geo_cache.CacheKey("AA1", "schools") in needing_refresh  # stale
    assert geo_cache.CacheKey("AA1", "cafes") not in needing_refresh  # fresh
    assert geo_cache.CacheKey("BB2", "cafes") in needing_refresh  # missing

    forced = geo_cache.keys_needing_refresh(
        ["AA1"], ["cafes"], cache, staleness_days=180, now=now, force=True
    )
    assert forced == [geo_cache.CacheKey("AA1", "cafes")]
