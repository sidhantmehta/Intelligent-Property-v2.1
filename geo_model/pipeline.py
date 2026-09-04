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
from typing import Iterable

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from geo_model.config import ModelConfig, load_settings
from geo_model.data.db import get_session, init_db
from geo_model.data.models import (
    Amenity,
    ApiUsage,
    GrammarSchool,
    HpiIndex,
    Outcode,
    PostcodeSector,
    PricePaidTransaction,
    PrivateSchool,
    ReferencePoint as ReferencePointRow,
    RunConfig,
    RunResult,
    RunResultCategory,
    SectorPrice,
    SectorTravelTime,
    TravelTime,
)
from geo_model.domain import geo_cache, pricing, scoring
from geo_model.logging_setup import get_logger, new_run_id, run_logger
from geo_model.providers.base import GeoPoint, GeoProvider
from geo_model.providers.here_maps import HereMapsProvider
from geo_model.providers.local_dataset import LocalDatasetProvider

logger = get_logger(__name__)


def build_provider(config: ModelConfig) -> GeoProvider:
    """Builds the PRIMARY provider (config.provider, e.g. "here") -- the
    one used for reference-point geocoding and travel time, and the
    default for any amenity category that doesn't name a different one.
    Use _resolve_category_provider() for amenity fetching, which also
    handles categories assigned to a different provider (e.g.
    private_schools -> local_dataset)."""
    settings = load_settings()
    if config.provider == "here":
        if not settings.here_api_key:
            raise RuntimeError("HERE_API_KEY is not set (see .env.example) -- required for provider 'here'")
        return HereMapsProvider(api_key=settings.here_api_key)
    raise ValueError(f"Unknown provider: {config.provider!r}")


def _build_local_dataset_provider(session: Session) -> LocalDatasetProvider:
    private_schools = session.scalars(select(PrivateSchool).where(PrivateSchool.lat.is_not(None))).all()
    grammar_schools = session.scalars(select(GrammarSchool).where(GrammarSchool.lat.is_not(None))).all()
    datasets = {
        "private_schools": [
            {"title": s.name, "address": s.address, "lat": s.lat, "long": s.long}
            for s in private_schools
        ],
        "grammar_schools": [
            {"title": s.name, "address": s.address, "lat": s.lat, "long": s.long}
            for s in grammar_schools
        ],
    }
    return LocalDatasetProvider(datasets)


def _resolve_category_provider(
    provider_name: str,
    config: ModelConfig,
    session: Session,
    primary_provider: GeoProvider,
    provider_cache: dict[str, GeoProvider],
) -> GeoProvider:
    """Amenity categories can each name their own provider (config.py
    defaults it to the top-level one when unset). Providers are built at
    most once per run and reused across every category/outcode that uses
    them -- e.g. private_schools' LocalDatasetProvider only ever loads the
    schools table once, not once per outcode."""
    if provider_name in provider_cache:
        return provider_cache[provider_name]
    if provider_name == config.provider:
        provider_cache[provider_name] = primary_provider
    elif provider_name == "local_dataset":
        provider_cache[provider_name] = _build_local_dataset_provider(session)
    else:
        raise ValueError(f"Unknown provider: {provider_name!r}")
    return provider_cache[provider_name]


def ensure_db_ready() -> None:
    settings = load_settings()
    init_db(settings.db_path)


def _outcodes_in_scope(session: Session, outcode_filter: list[str] | None) -> list[Outcode]:
    stmt = select(Outcode)
    if outcode_filter:
        stmt = stmt.where(Outcode.outcode.in_(outcode_filter))
    return list(session.scalars(stmt))


def _sectors_in_scope(session: Session, outcode_filter: list[str] | None) -> list[PostcodeSector]:
    stmt = select(PostcodeSector)
    if outcode_filter:
        stmt = stmt.where(PostcodeSector.outcode.in_(outcode_filter))
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
    primary_provider: GeoProvider,
    config: ModelConfig,
    outcodes: list[Outcode],
    keys: list[geo_cache.CacheKey],
    log,
    provider_cache: dict[str, GeoProvider],
) -> None:
    outcode_by_code = {o.outcode: o for o in outcodes}
    for i, key in enumerate(keys, start=1):
        outcode_row = outcode_by_code.get(key.outcode)
        if outcode_row is None:
            continue
        category = config.category_by_key(key.category_key)
        provider = _resolve_category_provider(category.provider, config, session, primary_provider, provider_cache)
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
        # Providers can (and HERE's Discover does) return duplicate POIs for
        # a single query -- e.g. the same chain outlet listed twice at
        # identical coordinates. Dedupe on the same fields the unique
        # constraint covers, and ignore-on-conflict as a second safety net,
        # rather than letting a provider's dirty data crash the run.
        seen: set[tuple] = set()
        rows = []
        for r in results:
            identity = (key.outcode, provider.name, key.category_key, r.title, r.lat, r.long)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(
                {
                    "outcode": key.outcode,
                    "provider": provider.name,
                    "category_key": key.category_key,
                    "title": r.title,
                    "address": r.address,
                    "lat": r.lat,
                    "long": r.long,
                    "distance_m": r.distance_m,
                    "fetched_at": now,
                    "is_seed": False,
                }
            )
        if rows:
            stmt = sqlite_insert(Amenity).values(rows).on_conflict_do_nothing(
                index_elements=["outcode", "provider", "category_key", "title", "lat", "long"]
            )
            session.execute(stmt)
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


def _fetch_and_upsert_sector_travel_times(
    session: Session,
    provider: GeoProvider,
    config: ModelConfig,
    sectors: list[PostcodeSector],
    reference_points: dict[str, GeoPoint],
    keys: list[tuple[str, str]],
    log,
) -> None:
    """Sector-grain counterpart to _fetch_and_upsert_travel_times -- same
    one-call-per-(unit, reference point) shape, just keyed by sector
    centroid instead of outcode centroid and written to
    sector_travel_times instead of travel_times."""
    sector_by_code = {s.sector: s for s in sectors}
    for i, (sector, ref_name) in enumerate(keys, start=1):
        sector_row = sector_by_code.get(sector)
        destination = reference_points.get(ref_name)
        if sector_row is None or destination is None:
            continue
        origin = GeoPoint(lat=sector_row.lat, long=sector_row.long)
        try:
            minutes = provider.travel_time_minutes(origin, destination, config.travel_mode)
        except Exception as e:
            log.error("travel_time_minutes failed for sector=%s ref=%s: %s", sector, ref_name, e)
            continue

        stmt = sqlite_insert(SectorTravelTime).values(
            sector=sector, reference_point_name=ref_name, mode=config.travel_mode,
            minutes=minutes, computed_at=dt.datetime.now(dt.timezone.utc),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["sector", "reference_point_name", "mode"],
            set_={"minutes": minutes, "computed_at": dt.datetime.now(dt.timezone.utc)},
        )
        session.execute(stmt)
        if i % 100 == 0 or i == len(keys):
            log.info("Sector travel time %d/%d: sector=%s -> %s = %s min", i, len(keys), sector, ref_name, minutes)
        if i % 25 == 0:
            session.commit()
    session.commit()


def _persist_usage_log(session: Session, providers: Iterable[GeoProvider], run_id: str, log) -> int:
    """Copies everything each provider recorded during this run into the
    api_usage table, so usage stays queryable/reconcilable against each
    provider's own dashboard long after the run's log lines have rotated
    away. Safe to call even for providers that don't track usage (returns
    an empty list by default -- see GeoProvider.get_usage_log), e.g.
    LocalDatasetProvider, which has nothing to reconcile."""
    total = 0
    for provider in providers:
        records = provider.get_usage_log()
        if not records:
            continue
        session.execute(
            ApiUsage.__table__.insert(),
            [
                {
                    "provider": provider.name,
                    "call_type": r.call_type,
                    "status_code": r.status_code,
                    "run_id": run_id,
                    "called_at": r.called_at,
                }
                for r in records
            ],
        )
        log.info("Recorded %d %s API call(s) for usage tracking", len(records), provider.name)
        total += len(records)
    return total


def compute_sector_prices(outcode_filter: list[str] | None = None) -> dict:
    """Wires geo_model.domain.pricing to the DB: loads PricePaidTransaction
    rows within the primary window + the full HpiIndex, computes a
    HPI-adjusted median price per (sector, property_type) with the
    outcode-level fallback for sparse cells, and upserts sector_prices.
    A maintenance step in its own right (like compute_sector_centroids),
    not part of run_model -- price only needs recomputing when new PPD/HPI
    data has been ingested, not on every scoring run."""
    ensure_db_ready()
    run_id = new_run_id()
    log = run_logger(__name__, run_id)
    now = dt.date.today()
    window_start = now - dt.timedelta(days=pricing.PRIMARY_WINDOW_DAYS)

    with get_session() as session:
        query = select(PricePaidTransaction).where(PricePaidTransaction.date >= window_start)
        if outcode_filter:
            query = query.where(PricePaidTransaction.outcode.in_(outcode_filter))
        txn_rows = session.scalars(query).all()
        transactions = [
            pricing.Transaction(
                outcode=t.outcode, sector=t.sector, property_type=t.property_type, price=t.price,
                date=t.date, district=t.district, old_new=t.old_new, ppd_category=t.ppd_category,
            )
            for t in txn_rows
        ]
        log.info("Loaded %d Price Paid transactions within the %d-day window", len(transactions), pricing.PRIMARY_WINDOW_DAYS)

        hpi_rows = session.scalars(select(HpiIndex)).all()
        hpi_by_district_month: dict[tuple[str, dt.date], dict[str, float | None]] = {}
        latest_month = None
        for h in hpi_rows:
            hpi_by_district_month[(h.district, h.month)] = {
                "index_all": h.index_all, "index_detached": h.index_detached,
                "index_semi": h.index_semi, "index_terraced": h.index_terraced, "index_flat": h.index_flat,
            }
            if latest_month is None or h.month > latest_month:
                latest_month = h.month
        if latest_month is None:
            raise RuntimeError("hpi_index is empty -- run `ingest-price-data` first")
        log.info("Adjusting all prices to %s-equivalent (latest month in hpi_index)", latest_month)

        estimates = pricing.estimate_sector_prices(transactions, hpi_by_district_month, as_of_month=latest_month, now=now)

        for e in estimates:
            if e.grain == "none":
                continue
            stmt = sqlite_insert(SectorPrice).values(
                sector=e.key, property_type=e.property_type, median_price=e.median_price,
                transaction_count=e.transaction_count, estimate_grain=e.grain,
                computed_at=dt.datetime.now(dt.timezone.utc),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["sector", "property_type"],
                set_={
                    "median_price": stmt.excluded.median_price,
                    "transaction_count": stmt.excluded.transaction_count,
                    "estimate_grain": stmt.excluded.estimate_grain,
                    "computed_at": stmt.excluded.computed_at,
                },
            )
            session.execute(stmt)
        session.commit()

    by_grain: dict[str, int] = {}
    for e in estimates:
        by_grain[e.grain] = by_grain.get(e.grain, 0) + 1
    log.info("compute_sector_prices complete: %d (sector, type) estimates, by grain: %s", len(estimates), by_grain)
    return {"run_id": run_id, "estimates": len(estimates), "by_grain": by_grain, "as_of_month": latest_month.isoformat()}


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
        provider_cache: dict[str, GeoProvider] = {config.provider: provider}
        outcodes = _outcodes_in_scope(session, outcode_filter)
        log.info("Scope: %d outcodes x %d categories", len(outcodes), len(category_keys))

        reference_points = _ensure_reference_points(session, provider, config, log)

        cached = _cached_fetch_times(session, category_keys)
        keys = geo_cache.keys_needing_refresh(
            [o.outcode for o in outcodes], category_keys, cached, config.staleness_days,
            dt.datetime.now(dt.timezone.utc), force=True,
        )
        log.info("%d/%d (outcode, category) pairs need refresh", len(keys), len(outcodes) * len(category_keys))
        _fetch_and_upsert_amenities(session, provider, config, outcodes, keys, log, provider_cache)

        # Travel time is scored per postcode sector, not per outcode (see
        # geo_model.domain.pricing's module docstring) -- refreshed for
        # every sector under an in-scope outcode.
        sectors = _sectors_in_scope(session, outcode_filter)
        travel_keys = [(s.sector, rp.name) for s in sectors for rp in config.reference_points]
        _fetch_and_upsert_sector_travel_times(session, provider, config, sectors, reference_points, travel_keys, log)

        _persist_usage_log(session, provider_cache.values(), run_id, log)

    elapsed = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    log.info("refresh_geo_data complete in %.1fs", elapsed)
    return {"run_id": run_id, "outcodes": len(outcodes), "sectors": len(sectors), "amenity_keys_refreshed": len(keys), "elapsed_seconds": elapsed}


def run_model(config: ModelConfig, outcode_filter: list[str] | None = None) -> dict:
    """Scores every in-scope postcode sector against the given config.
    Amenities are fetched/cached per outcode and shared by every sector
    inside it; travel time is fetched/cached per sector (see
    geo_model.domain.pricing's module docstring for why the split). Only
    fetches geo data / travel times that are fully missing from the
    cache -- use refresh_geo_data() first if you want stale data
    re-validated. Requires compute_sector_centroids() (and, for price
    columns to be populated, compute_sector_prices()) to have already
    been run -- a sector with no centroid can't be scored."""
    ensure_db_ready()
    run_id = new_run_id()
    log = run_logger(__name__, run_id)
    log.info("run_model started (outcode_filter=%s)", outcode_filter or "ALL")
    start = dt.datetime.now(dt.timezone.utc)

    category_keys = [c.key for c in config.amenity_categories]
    with get_session() as session:
        provider = build_provider(config)
        provider_cache: dict[str, GeoProvider] = {config.provider: provider}
        outcodes = _outcodes_in_scope(session, outcode_filter)
        if not outcodes:
            log.error("No outcodes in scope -- has the outcodes table been seeded? (see geo_model.postcodes)")
            return {"run_id": run_id, "outcodes": 0, "sectors": 0}
        sectors = _sectors_in_scope(session, outcode_filter)
        if not sectors:
            log.error("No postcode sectors in scope -- has compute_sector_centroids() been run?")
            return {"run_id": run_id, "outcodes": len(outcodes), "sectors": 0}
        log.info("Scope: %d outcodes (amenities) x %d categories, %d sectors (price/travel time)", len(outcodes), len(category_keys), len(sectors))

        reference_points = _ensure_reference_points(session, provider, config, log)

        cached = _cached_fetch_times(session, category_keys)
        missing_keys = geo_cache.keys_missing_only([o.outcode for o in outcodes], category_keys, cached)
        log.info("%d/%d (outcode, category) pairs are missing from cache and will be fetched now", len(missing_keys), len(outcodes) * len(category_keys))
        _fetch_and_upsert_amenities(session, provider, config, outcodes, missing_keys, log, provider_cache)

        existing_travel = {
            (tt.sector, tt.reference_point_name)
            for tt in session.scalars(select(SectorTravelTime).where(SectorTravelTime.mode == config.travel_mode))
        }
        missing_travel_keys = [
            (s.sector, rp.name) for s in sectors for rp in config.reference_points
            if (s.sector, rp.name) not in existing_travel
        ]
        log.info("%d sector<->reference-point travel times are missing and will be fetched now", len(missing_travel_keys))
        _fetch_and_upsert_sector_travel_times(session, provider, config, sectors, reference_points, missing_travel_keys, log)

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
            select(SectorTravelTime).where(SectorTravelTime.sector.in_([s.sector for s in sectors]), SectorTravelTime.mode == config.travel_mode)
        )
        travel_times = [
            scoring.SectorTravelTimeRecord(sector=t.sector, reference_point_name=t.reference_point_name, minutes=t.minutes)
            for t in travel_rows if t.minutes is not None
        ]

        category_weights = [scoring.CategoryWeight(key=c.key, weight=c.weight) for c in config.amenity_categories]
        reference_weights = [scoring.CategoryWeight(key=rp.name, weight=rp.weight) for rp in config.reference_points]

        scores = scoring.score_sectors(
            sectors=[s.sector for s in sectors],
            sector_to_outcode={s.sector: s.outcode for s in sectors},
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

        for sector_score in scores:
            result_row = RunResult(
                run_config_id=run_id, outcode=sector_score.outcode, sector=sector_score.sector,
                total_score=sector_score.total_score,
            )
            session.add(result_row)
            session.flush()
            for cat_score in sector_score.categories:
                session.add(
                    RunResultCategory(
                        run_result_id=result_row.id,
                        category_key=cat_score.category_key,
                        raw_score=cat_score.raw_score,
                        normalized_score=cat_score.normalized_score,
                        weight_applied=cat_score.weight_applied,
                    )
                )

        _persist_usage_log(session, provider_cache.values(), run_id, log)
        session.commit()

    elapsed = (dt.datetime.now(dt.timezone.utc) - start).total_seconds()
    top = sorted(scores, key=lambda s: s.total_score, reverse=True)[:5]
    bottom = sorted(scores, key=lambda s: s.total_score)[:5]
    log.info(
        "run_model complete in %.1fs: %d sectors scored. top=%s bottom=%s",
        elapsed, len(scores),
        [(s.sector, round(s.total_score, 3)) for s in top],
        [(s.sector, round(s.total_score, 3)) for s in bottom],
    )
    return {"run_id": run_id, "outcodes": len(outcodes), "sectors": len(scores), "elapsed_seconds": elapsed}
