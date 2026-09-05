"""SQLAlchemy ORM models -- the data layer. No business logic lives here;
see geo_model.domain for that.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class Outcode(Base):
    """A UK postcode outcode and its centroid -- the unit everything else
    is scored against."""

    __tablename__ = "outcodes"

    outcode: Mapped[str] = mapped_column(String(8), primary_key=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    long: Mapped[float] = mapped_column(Float, nullable=False)
    last_updated: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    # Local authority district (postcodes.io admin_district[0], e.g.
    # "Guildford", "Hackney") and region (admin_district[0]'s region,
    # e.g. "London", "South East") -- used for the table's Borough/Group
    # columns and the map's labels. Nullable: only populated once
    # backfill_outcode_areas() has run against this outcode.
    borough: Mapped[str | None] = mapped_column(String(128), nullable=True)
    region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Derived from region + borough (see postcodes.py): "Inner London",
    # "Greater London", or "Home Counties".
    geo_group: Mapped[str | None] = mapped_column(String(32), nullable=True)


class Amenity(Base):
    """One cached amenity/POI result for a given outcode + category, from
    a given provider. Refreshed by geo_model.domain.geo_cache."""

    __tablename__ = "amenities"
    __table_args__ = (
        UniqueConstraint("outcode", "provider", "category_key", "title", "lat", "long", name="uq_amenity_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    outcode: Mapped[str] = mapped_column(String(8), ForeignKey("outcodes.outcode"), nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    category_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    address: Mapped[str | None] = mapped_column(String(512), nullable=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    long: Mapped[float] = mapped_column(Float, nullable=False)
    distance_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    fetched_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    is_seed: Mapped[bool] = mapped_column(default=False)  # imported from v2 reference_data, not yet re-validated


class ReferencePoint(Base):
    """A geocoded destination (e.g. an office) that outcodes are scored on
    travel time to. Name is the stable identity across runs."""

    __tablename__ = "reference_points"

    name: Mapped[str] = mapped_column(String(128), primary_key=True)
    address: Mapped[str] = mapped_column(String(512), nullable=False)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    long: Mapped[float | None] = mapped_column(Float, nullable=True)
    geocoded_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TravelTime(Base):
    """Cached travel time from an outcode centroid to a reference point."""

    __tablename__ = "travel_times"
    __table_args__ = (
        UniqueConstraint("outcode", "reference_point_name", "mode", name="uq_travel_time_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    outcode: Mapped[str] = mapped_column(String(8), ForeignKey("outcodes.outcode"), nullable=False, index=True)
    reference_point_name: Mapped[str] = mapped_column(
        String(128), ForeignKey("reference_points.name"), nullable=False, index=True
    )
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    minutes: Mapped[float | None] = mapped_column(Float, nullable=True)
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SectorTravelTime(Base):
    """Cached travel time from a postcode sector centroid to a reference
    point -- the sector-level counterpart to TravelTime, which stays
    outcode-level and unused by scoring now that travel time is scored per
    sector (amenities are the only thing still scored at outcode grain;
    see geo_model.domain.pricing module docstring)."""

    __tablename__ = "sector_travel_times"
    __table_args__ = (
        UniqueConstraint("sector", "reference_point_name", "mode", name="uq_sector_travel_time_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=False, index=True)
    reference_point_name: Mapped[str] = mapped_column(
        String(128), ForeignKey("reference_points.name"), nullable=False, index=True
    )
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    minutes: Mapped[float | None] = mapped_column(Float, nullable=True)
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class RunConfig(Base):
    """Snapshot of the weights/reference-points/radius-bins config used
    for one model run -- so past runs stay comparable/inspectable even
    after the live config changes."""

    __tablename__ = "run_configs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)  # run_id
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    weights_json: Mapped[str] = mapped_column(String, nullable=False)
    reference_points_json: Mapped[str] = mapped_column(String, nullable=False)
    radius_bins_json: Mapped[str] = mapped_column(String, nullable=False)

    results: Mapped[list["RunResult"]] = relationship(back_populates="run_config")


class RunResult(Base):
    """One postcode sector's total score for one run (before sector-level
    scoring, this was one outcode's -- ``outcode`` is kept as the parent
    outcode, still needed to look up the shared amenity scores; ``sector``
    is the actual scored unit and what price/travel time are specific to).
    ``sector`` is nullable only so pre-sector-scoring historical rows keep
    loading; every row from a sector-aware run_model has it set."""

    __tablename__ = "run_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_config_id: Mapped[str] = mapped_column(String(32), ForeignKey("run_configs.id"), nullable=False, index=True)
    outcode: Mapped[str] = mapped_column(String(8), ForeignKey("outcodes.outcode"), nullable=False, index=True)
    sector: Mapped[str | None] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=True, index=True)
    total_score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    run_config: Mapped["RunConfig"] = relationship(back_populates="results")
    categories: Mapped[list["RunResultCategory"]] = relationship(back_populates="run_result")


class RunResultCategory(Base):
    """One category's score contribution to one outcode's result, kept as
    normalized rows (not a JSON blob) so the frontend table can sort/color
    by any individual category directly -- this is what makes weak/strong
    per category queryable rather than only visible via a blob."""

    __tablename__ = "run_result_categories"
    __table_args__ = (
        UniqueConstraint("run_result_id", "category_key", name="uq_run_result_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_result_id: Mapped[int] = mapped_column(Integer, ForeignKey("run_results.id"), nullable=False, index=True)
    category_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    raw_score: Mapped[float] = mapped_column(Float, nullable=False)
    normalized_score: Mapped[float] = mapped_column(Float, nullable=False)
    weight_applied: Mapped[float] = mapped_column(Float, nullable=False)

    run_result: Mapped["RunResult"] = relationship(back_populates="categories")


class ApiUsage(Base):
    """One outbound call a provider made and got a response for (any
    status code), persisted so usage can be reconciled against the
    provider's own quota/billing dashboard over time. Written by
    geo_model.pipeline from a provider's get_usage_log() after each run;
    ``run_id`` is nullable since not every provider call happens inside a
    scored run (e.g. reference-point geocoding during a bare cache check)."""

    __tablename__ = "api_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    call_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)
    run_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    called_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)


class PrivateSchool(Base):
    """A curated, pre-geocoded dataset entry (private schools register) --
    NOT a per-outcode search result like Amenity. Each school has its own
    fixed lat/long, geocoded once from its postcode; geo_model.providers.
    local_dataset.LocalDatasetProvider scans this table to answer
    nearby_amenities() the same way HERE's Discover does, so the rest of
    the pipeline (scoring, caching) needs no special-casing for it."""

    __tablename__ = "private_schools"
    __table_args__ = (
        UniqueConstraint("name", "postcode", name="uq_private_school_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    address: Mapped[str] = mapped_column(String(512), nullable=False)
    postcode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    long: Mapped[float | None] = mapped_column(Float, nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    gender_profile: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    day_boarding_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    religious_affiliation: Mapped[str | None] = mapped_column(String(64), nullable=True)
    geocoded_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PostcodeSector(Base):
    """A UK postcode sector (outcode + the first digit of the incode, e.g.
    "SW11 1") and its centroid -- the unit price/travel-time are scored
    against (finer than Outcode, which amenities stay scored against; see
    geo_model.domain.pricing module docstring for why the split)."""

    __tablename__ = "postcode_sectors"

    sector: Mapped[str] = mapped_column(String(8), primary_key=True)
    outcode: Mapped[str] = mapped_column(String(8), ForeignKey("outcodes.outcode"), nullable=False, index=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    long: Mapped[float] = mapped_column(Float, nullable=False)
    postcode_count: Mapped[int] = mapped_column(Integer, nullable=False)  # how many real postcodes the centroid was averaged from
    last_updated: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    # Royal Mail post town (e.g. "Gerrards Cross"), taken as the most common
    # `posttown` value among this sector's EPC certificates -- see
    # geo_model.epc_data.ingest_epc_data(). Nullable: only populated once
    # EPC ingestion has run and found at least one certificate in the
    # sector with a post town on record.
    town: Mapped[str | None] = mapped_column(String(64), nullable=True)


class PricePaidTransaction(Base):
    """One HM Land Registry Price Paid Data sale, filtered down to our
    outcode scope at ingest time (geo_model.price_data). Raw/uncomputed --
    geo_model.domain.pricing aggregates these into SectorPrice rows. Kept
    as its own table (rather than aggregating straight to SectorPrice on
    ingest) so the aggregation window/backoff logic can be re-tuned and
    re-run without re-fetching from Land Registry."""

    __tablename__ = "price_paid_transactions"

    transaction_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[dt.date] = mapped_column(nullable=False, index=True)
    postcode: Mapped[str] = mapped_column(String(16), nullable=False)
    sector: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    outcode: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    property_type: Mapped[str] = mapped_column(String(1), nullable=False)  # D/S/T/F/O
    old_new: Mapped[str] = mapped_column(String(1), nullable=False)  # Y = new build, N = resale
    duration: Mapped[str] = mapped_column(String(1), nullable=False)  # F = freehold, L = leasehold
    district: Mapped[str] = mapped_column(String(128), nullable=False)  # matches HpiIndex.district
    ppd_category: Mapped[str] = mapped_column(String(1), nullable=False)  # A = standard sale, B = additional (repossession/BTL panel/etc)


class HpiIndex(Base):
    """One month's UK House Price Index row for one local authority
    district, from HM Land Registry's UK-HPI-full-file download --
    used to scale a Price Paid transaction to today's-equivalent value.
    ``district`` matches PricePaidTransaction.district (both upper-cased
    local authority names); index values are base-100 at Jan 2015."""

    __tablename__ = "hpi_index"
    __table_args__ = (
        UniqueConstraint("district", "month", name="uq_hpi_index_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    district: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    month: Mapped[dt.date] = mapped_column(nullable=False, index=True)
    index_all: Mapped[float | None] = mapped_column(Float, nullable=True)
    index_detached: Mapped[float | None] = mapped_column(Float, nullable=True)
    index_semi: Mapped[float | None] = mapped_column(Float, nullable=True)
    index_terraced: Mapped[float | None] = mapped_column(Float, nullable=True)
    index_flat: Mapped[float | None] = mapped_column(Float, nullable=True)


class SectorPrice(Base):
    """Computed (not raw) price estimate for one postcode sector + property
    type, produced by geo_model.domain.pricing from PricePaidTransaction +
    HpiIndex. ``estimate_grain`` records whether the number came from the
    sector's own transactions or backed off to its parent outcode's
    (sparse-sector fallback) -- surfaced to the frontend so a backed-off
    estimate can be shown/labelled differently from a direct one."""

    __tablename__ = "sector_prices"
    __table_args__ = (
        UniqueConstraint("sector", "property_type", name="uq_sector_price_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=False, index=True)
    property_type: Mapped[str] = mapped_column(String(1), nullable=False)  # D/S/T/F
    median_price: Mapped[float | None] = mapped_column(Float, nullable=True)  # HPI-adjusted to today's-equivalent
    transaction_count: Mapped[int] = mapped_column(Integer, nullable=False)  # count actually used (sector- or outcode-level, whichever was used)
    estimate_grain: Mapped[str] = mapped_column(String(16), nullable=False)  # "sector" | "outcode" | "none"
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class EpcCertificate(Base):
    """One dwelling's current Energy Performance Certificate, filtered
    down to our outcode scope at ingest time (geo_model.epc_data). Raw/
    uncomputed -- geo_model.domain.floor_area aggregates these into
    SectorFloorArea rows. Deduped to one row per dwelling at ingest time
    (by UPRN, falling back to address) -- a dwelling re-assessed over the
    years would otherwise appear multiple times and skew the median."""

    __tablename__ = "epc_certificates"

    dwelling_key: Mapped[str] = mapped_column(String(128), primary_key=True)  # UPRN, or a normalized address when UPRN is absent
    postcode: Mapped[str] = mapped_column(String(16), nullable=False)
    sector: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    outcode: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    property_type: Mapped[str] = mapped_column(String(1), nullable=False)  # D/S/T/F (mapped from EPC's PROPERTY_TYPE+BUILT_FORM)
    total_floor_area_m2: Mapped[float] = mapped_column(Float, nullable=False)
    lodgement_date: Mapped[dt.date] = mapped_column(nullable=False, index=True)


class SectorFloorArea(Base):
    """Computed (not raw) median floor area for one postcode sector +
    property type, produced by geo_model.domain.floor_area from
    EpcCertificate. ``estimate_grain`` mirrors SectorPrice's -- whether
    the number came from the sector's own certificates or backed off to
    its parent outcode's (sparse-sector fallback)."""

    __tablename__ = "sector_floor_areas"
    __table_args__ = (
        UniqueConstraint("sector", "property_type", name="uq_sector_floor_area_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=False, index=True)
    property_type: Mapped[str] = mapped_column(String(1), nullable=False)  # D/S/T/F
    median_floor_area_m2: Mapped[float | None] = mapped_column(Float, nullable=True)
    certificate_count: Mapped[int] = mapped_column(Integer, nullable=False)
    estimate_grain: Mapped[str] = mapped_column(String(16), nullable=False)  # "sector" | "outcode" | "none"
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class GrammarSchool(Base):
    """A curated, pre-geocoded dataset entry (selective state grammar /
    partially-selective consortium schools register) -- same shape and
    purpose as PrivateSchool: LocalDatasetProvider scans this table (under
    the "grammar_schools" dataset key) to answer nearby_amenities()."""

    __tablename__ = "grammar_schools"
    __table_args__ = (
        UniqueConstraint("name", "postcode", name="uq_grammar_school_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    region: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    address: Mapped[str] = mapped_column(String(512), nullable=False)
    postcode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    long: Mapped[float | None] = mapped_column(Float, nullable=True)
    intake_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    geocoded_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
