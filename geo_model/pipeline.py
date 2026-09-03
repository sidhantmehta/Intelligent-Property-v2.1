"""Orchestration ("service") layer: the only place data + provider + domain
are wired together. Everything else in geo_model is either pure logic
(geo_model.domain), pure I/O adapters (geo_model.data, geo_model.providers),
or an interface (scripts/run_model.py, the Artifact frontend indirectly).

Two entry points, matching the two frontend actions:
  - refresh_geo_data(): the explicit "Refresh Geo Data" button/command.
    Force-refreshes anything missing or older than the configured
    staleness threshold.
  - run_model(): the "Run Model" button/command. Only fetches data that's
    fully missing (never force-refreshes merely-stale data -- that's
    refresh_geo_data's job, so a run never surprises the user with a wave
    of fresh API calls) and produces a new scored run_result.

This is also the seam a future interface (e.g. a persistent backend) would
call into instead of the CLI -- it speaks only Python objects in and out,
never HTTP/JSON.
"""
from __future__ import annotations

import datetime as dt
import json

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.config import ModelConfig, load_settings
from geo_model.data.db import get_session, init_db
from geo_model.data.models import (
    Amenity,
    Outcode,
    ReferencePoint as ReferencePointRow,
    RunConfig,
    RunResult,
    RunResultCategory,
    TravelTime,
)
from geo_model.domain import geo_cache, scoring
from geo_model.logging_setup import get_logger, new_run_id, run_logger
from geo_model.providers.base import GeoPoint, GeoProvider
from geo_model.providers.here_maps import HereMapsProvider

logger = get_logger(__name__)


def build_provider(config: ModelConfig) -> GeoProvider:
    settings = load_settings()
    if config.provider == "here":
        if not settings.here_api_key:
            raise RuntimeError("HERE_API_KEY is not set (see .env.example) -- required for provider 'here'")
        return HereMapsProvider(api_key=settings.here_api_key)
    raise ValueError(f"Unknown provider: {config.provider!r}")


def ensure_db_ready() -> None:
    settings = load_settings()
    init_db(settings.db_path)


def _outcodes_in_scope(session: Session, outcode_filter: list[str] | None) -> list[Outcode]:
    stmt = select(Outcode)
    if outcode_filter:
        stmt = stmt.where(Outcode.outcode.in_(outcode_filter))
    return list(session.scalars(stmt))


def _cached_fetch_times(session: Session, category_keys: list[str]) -> dict[geo_cache.CacheKey, dt.datetime]:
    rows = session.execute(
        select(Amenity.outcode, Amenity.category_key, func.max(Amenity.fetched_at))
        .where(Amenity.category_key.in_(category_keys))
        .group_by(Amenity.outcode, Amenity.category_key)
    ).all()
    return {geo_cache.CacheKey(outcode, cat): fetched_at for outcode, cat, fetched_at in rows}


def _ensure_reference_points(session: Session, provider: GeoProvider, config: ModelConfig, log) -> dict[str, GeoPoint]:
    """Geocodes any reference point that's new or whose address changed;
    reuses cached lat/long otherwise. Reference-point geocoding is cheap
    (one call per point, not per outcode) so this always runs, on both
    run_model and refresh_geo_data."""
    points: dict[str, GeoPoint] = {}
    for rp in config.reference_points:
        existing = session.get(ReferencePointRow, rp.name)
        if existing and existing.address == rp.address and existing.lat is not None:
            points[rp.name] = GeoPoint(lat=existing.lat, long=existing.long)
            continue

        log.info("Geocoding reference point %r (%s)", rp.name, rp.address)
        geocoded = provider.geocode(rp.address)
        if geocoded is None:
            log.error("Could not geocode reference point %r (%s) -- it will be skipped", rp.name, rp.address)
            continue

        stmt = sqlite_insert(ReferencePointRow).values(
            name=rp.name, address=rp.address, lat=geocoded.lat, long=geocoded.long, geocoded_at=dt.datetime.now(dt.timezone.utc)
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"address": rp.address, "lat": geocoded.lat, "long": geocoded.long, "geocoded_at": dt.datetime.now(dt.timezone.utc)},
        )
        session.execute(stmt)
        points[rp.name] = geocoded
    return points


def _fetch_and_upsert_amenities(
    session: Session,
    provider: GeoProvider,
    config: ModelConfig,
    outcodes: list[Outcode],
    keys: list[geo_cache.CacheKey],
    log,
) -> None:
    outcode_by_code = {o.outcode: o for o in outcodes}
    for i, key in enumerate(keys, start=1):
        outcode_row = outcode_by_code.get(key.outcode)
        if outcode_row is None:
            continue
        category = config.category_by_key(key.category_key)
        origin = GeoPoint(lat=outcode_row.lat, long=outcode_row.long)

        try:
            results = provider.nearby_amenities(
                origin, category.query, config.search_radius_miles, config.max_amenities_per_category
            )
        except Exception as e:
            log.error("nearby_amenities failed for outcode=%s category=%s: %s", key.outcode, key.category_key, e)
            continue

        session.execute(
            Amenity.__table__.delete().where(
                Amenity.outcode == key.outcode, Amenity.category_key == key.category_key
            )
        )
        now = dt.datetime.now(dt.timezone.utc)
        for r in results:
            session.add(
                Amenity(
                    outcode=key.outcode,
                    provider=provider.name,
                    category_key=key.category_key,
                    title=r.title,
                    address=r.address,
                    lat=r.lat,
                    long=r.long,
                    distance_m=r.distance_m,
                    fetched_at=now,
                    is_seed=False,
                )
            )
        log.info(
            "Fetched %d/%d: outcode=%s category=%s -> %d amenities",
            i, len(keys), key.outcode, key.category_key, len(results),
        )
        if i % 25 == 0:
            session.commit()
    session.commit()


def _fetch_and_upsert_travel_times(
    session: Session,
    provider: GeoProvider,
    config: ModelConfig,
    outcodes: list[Outcode],
    reference_points: dict[str, GeoPoint],
    keys: list[tuple[str, str]],
    log,
) -> None:
    outcode_by_code = {o.outcode: o for o in outcodes}
    for i, (outcode, ref_name) in enumerate(keys, start=1):
        outcode_row = outcode_by_code.get(outcode)
        destination = reference_points.get(ref_name)
        if outcode_row is None or destination is None:
            continue
        origin = GeoPoint(lat=outcode_row.lat, long=outcode_row.long)
        try:
            minutes = provider.travel_time_minutes(origin, destination, config.travel_mode)
        except Exception as e:
            log.error("travel_time_minutes failed for outcode=%s ref=%s: %s", outcode, ref_name, e)
            continue

        stmt = sqlite_insert(TravelTime).values(
            outcode=outcode, reference_point_name=ref_name, mode=config.travel_mode,
            minutes=minutes, computed_at=dt.datetime.now(dt.timezone.utc),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["outcode", "reference_point_name", "mode"],
            set_={"minutes": minutes, "computed_at": dt.datetime.now(dt.timezone.utc)},
        )
        session.execute(stmt)
        log.info("Travel time %d/%d: outcode=%s -> %s = %s min", i, len(keys), outcode, ref_name, minutes)
        if i % 25 == 0:
            session.commit()
    session.commit()


def refresh_geo_data(config: ModelConfig, outcode_filter: list[str] | None = None) -> dict:
    """Explicit refresh: force-fetches every (outcode, category) pair
    that's missing or older than config.staleness_days, and recomputes
    every outcode<->reference-point travel time. Never called implicitly
    by run_model()."""
    ensure_db_ready()
    run_id = new_run_id()
    log = run_logger(__name__, run_id)
    log.info("refresh_geo_data started (outcode_filter=%s)", outcode_filter or "ALL")
    start = dt.datetime.now(dt.timezone.utc)

    category_keys = [c.key for c in config.amenity_categories]
    with get_session() as session:
        provider = build_provider(config)
        outcodes = _outcodes_in_scope(session, outcode_filter)
        log.info("Scope: %d outcodes x %d categories", len(outcodes), len(category_keys))

        reference_points = _ensure_reference_points(session, provider, config, log)

        cached = _cached_fetch_times(session, category_keys)
        keys = geo_cache.keys_needing_refresh(
            [o.outcode for o in outcodes], category_keys, cached, config.staleness_days,
            dt.datetime.now(dt.timezone.utc), force=True,
        )
        log.info("%d/%d (outcode, category) pairs need refresh", len(keys), len(outcodes) * len(category_keys))
        _fetch_and_upsert_amenities(session, provider, config, outcodes, keys, log)

        travel_keys = [(o.outcode, rp.name) for o in outcodes for rp in config.reference_points]
        _fetch_and_upsert_travel_times(session, provider, config, outcodes, reference_points, travel_keys, log)

    elapsed = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    log.info("refresh_geo_data complete in %.1fs", elapsed)
    return {"run_id": run_id, "outcodes": len(outcodes), "amenity_keys_refreshed": len(keys), "elapsed_seconds": elapsed}


def run_model(config: ModelConfig, outcode_filter: list[str] | None = None) -> dict:
    """Scores every in-scope outcode against the given config. Only
    fetches geo data / travel times that are fully missing from the
    cache -- use refresh_geo_data() first if you want stale data
    re-validated."""
    ensure_db_ready()
    run_id = new_run_id()
    log = run_logger(__name__, run_id)
    log.info("run_model started (outcode_filter=%s)", outcode_filter or "ALL")
    start = dt.datetime.now(dt.timezone.utc)

    category_keys = [c.key for c in config.amenity_categories]
    with get_session() as session:
        provider = build_provider(config)
        outcodes = _outcodes_in_scope(session, outcode_filter)
        if not outcodes:
            log.error("No outcodes in scope -- has the outcodes table been seeded? (see geo_model.postcodes)")
            return {"run_id": run_id, "outcodes": 0}
        log.info("Scope: %d outcodes x %d categories", len(outcodes), len(category_keys))

        reference_points = _ensure_reference_points(session, provider, config, log)

        cached = _cached_fetch_times(session, category_keys)
        missing_keys = geo_cache.keys_missing_only([o.outcode for o in outcodes], category_keys, cached)
        log.info("%d/%d (outcode, category) pairs are missing from cache and will be fetched now", len(missing_keys), len(outcodes) * len(category_keys))
        _fetch_and_upsert_amenities(session, provider, config, outcodes, missing_keys, log)

        existing_travel = {
            (tt.outcode, tt.reference_point_name)
            for tt in session.scalars(select(TravelTime).where(TravelTime.mode == config.travel_mode))
        }
        missing_travel_keys = [
            (o.outcode, rp.name) for o in outcodes for rp in config.reference_points
            if (o.outcode, rp.name) not in existing_travel
        ]
        log.info("%d outcode<->reference-point travel times are missing and will be fetched now", len(missing_travel_keys))
        _fetch_and_upsert_travel_times(session, provider, config, outcodes, reference_points, missing_travel_keys, log)

        # --- load everything needed for scoring ---
        amenity_rows = session.scalars(
            select(Amenity).where(Amenity.outcode.in_([o.outcode for o in outcodes]))
        )
        outcode_by_code = {o.outcode: o for o in outcodes}
        amenities = []
        for a in amenity_rows:
            o = outcode_by_code[a.outcode]
            distance_miles = (
                a.distance_m / 1609.34 if a.distance_m is not None
                else scoring.haversine_miles(o.lat, o.long, a.lat, a.long)
            )
            amenities.append(scoring.AmenityRecord(outcode=a.outcode, category_key=a.category_key, distance_miles=distance_miles))

        travel_rows = session.scalars(
            select(TravelTime).where(TravelTime.outcode.in_([o.outcode for o in outcodes]), TravelTime.mode == config.travel_mode)
        )
        travel_times = [
            scoring.TravelTimeRecord(outcode=t.outcode, reference_point_name=t.reference_point_name, minutes=t.minutes)
            for t in travel_rows if t.minutes is not None
        ]

        category_weights = [scoring.CategoryWeight(key=c.key, weight=c.weight) for c in config.amenity_categories]
        reference_weights = [scoring.CategoryWeight(key=rp.name, weight=rp.weight) for rp in config.reference_points]

        scores = scoring.score_outcodes(
            outcodes=[o.outcode for o in outcodes],
            amenities=amenities,
            category_weights=category_weights,
            radius_bins_miles=list(config.radius_bins_miles),
            travel_times=travel_times,
            reference_weights=reference_weights,
        )

        # --- persist run_config + run_results + run_result_categories ---
        run_config_row = RunConfig(
            id=run_id,
            weights_json=json.dumps({c.key: c.weight for c in config.amenity_categories}),
            reference_points_json=json.dumps({rp.name: {"address": rp.address, "weight": rp.weight} for rp in config.reference_points}),
            radius_bins_json=json.dumps(list(config.radius_bins_miles)),
        )
        session.add(run_config_row)
        session.flush()

        for outcode_score in scores:
            result_row = RunResult(run_config_id=run_id, outcode=outcode_score.outcode, total_score=outcode_score.total_score)
            session.add(result_row)
            session.flush()
            for cat_score in outcode_score.categories:
                session.add(
                    RunResultCategory(
                        run_result_id=result_row.id,
                        category_key=cat_score.category_key,
                        raw_score=cat_score.raw_score,
                        normalized_score=cat_score.normalized_score,
                        weight_applied=cat_score.weight_applied,
                    )
                )
        session.commit()

    elapsed = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    top = sorted(scores, key=lambda s: s.total_score, reverse=True)[:5]
    bottom = sorted(scores, key=lambda s: s.total_score)[:5]
    log.info(
        "run_model complete in %.1fs: %d outcodes scored. top=%s bottom=%s",
        elapsed, len(scores),
        [(s.outcode, round(s.total_score, 3)) for s in top],
        [(s.outcode, round(s.total_score, 3)) for s in bottom],
    )
    return {"run_id": run_id, "outcodes": len(scores), "elapsed_seconds": elapsed}
