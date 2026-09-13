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
    # Raw address components -- not used for sector/outcode (postcode
    # already gives us that), kept so geo_model.domain.address_match can
    # join a sale to its EPC certificate without a shared ID (Price Paid
    # Data carries no UPRN). paon is usually a house number but can be a
    # house name ("The Cottage"); saon is the flat/unit qualifier.
    paon: Mapped[str | None] = mapped_column(String(128), nullable=True)
    saon: Mapped[str | None] = mapped_column(String(128), nullable=True)
    street: Mapped[str | None] = mapped_column(String(128), nullable=True)


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
    # Raw address lines -- address1 is usually the flat/unit qualifier (e.g.
    # "Flat 4") when there is one, address2 the house number + street (e.g.
    # "44 Shoot-Up Hill"). Kept so geo_model.domain.address_match can join
    # this certificate to its Price Paid Data sale without a shared ID.
    address1: Mapped[str | None] = mapped_column(String(256), nullable=True)
    address2: Mapped[str | None] = mapped_column(String(256), nullable=True)


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


class MatchedPropertySale(Base):
    """One EPC certificate joined to the Price Paid Data sale of the same
    dwelling, produced by geo_model.domain.address_match. Price Paid Data
    carries no UPRN, so the join is done on postcode + a normalized house
    number (+ flat/unit qualifier for flats) rather than a shared ID --
    ``confidence`` records how sure that match is, and rows below
    "flat_match_fuzzy" are kept here (not silently dropped) specifically so
    a low-confidence match can be looked up and checked later rather than
    only ever seen as a mysteriously-off aggregate. Only "house_match",
    "flat_match_exact", and "flat_match_fuzzy" rows feed the price-per-m2
    aggregation (geo_model.domain.matched_pricing); "ambiguous" rows are
    stored for troubleshooting but never used in a computed estimate."""

    __tablename__ = "matched_property_sales"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dwelling_key: Mapped[str] = mapped_column(String(128), ForeignKey("epc_certificates.dwelling_key"), nullable=False, index=True)
    transaction_id: Mapped[str] = mapped_column(String(64), ForeignKey("price_paid_transactions.transaction_id"), nullable=False, index=True)
    sector: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    outcode: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    property_type: Mapped[str] = mapped_column(String(1), nullable=False)  # D/S/T/F -- from the EPC side
    total_floor_area_m2: Mapped[float] = mapped_column(Float, nullable=False)
    sale_price: Mapped[int] = mapped_column(Integer, nullable=False)  # raw, not HPI-adjusted (done at aggregation time)
    sale_date: Mapped[dt.date] = mapped_column(nullable=False)
    lodgement_date: Mapped[dt.date] = mapped_column(nullable=False)
    # "house_match": unique postcode+house-number match, not a flat.
    # "flat_match_exact": postcode+house-number+flat-id matched cleanly.
    # "flat_match_fuzzy": flat-id matched only after non-trivial text
    #   cleanup (e.g. "GROUND FLOOR FLAT" vs "FLAT G") -- used, but worth
    #   spot-checking if a sector's number looks off.
    # "ambiguous": more than one plausible candidate on one side that
    #   couldn't be disambiguated -- stored, never aggregated.
    confidence: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    match_note: Mapped[str | None] = mapped_column(String(256), nullable=True)  # why it's ambiguous/fuzzy, for troubleshooting
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SectorMatchedPrice(Base):
    """Computed (not raw) price-per-m2 estimate for one postcode sector +
    property type, built directly from MatchedPropertySale rows (real
    floor-area-to-sale-price pairs) rather than dividing two independently
    computed medians the way SectorPrice/SectorFloorArea are combined at
    export time. ``size_bin_m2`` is the lower bound of a 100m2 bucket
    (0, 100, 200, ...) when there were enough matched pairs in that bucket
    to report one, else NULL for the sector+type's overall figure."""

    __tablename__ = "sector_matched_prices"
    __table_args__ = (
        UniqueConstraint("sector", "property_type", "size_bin_m2", name="uq_sector_matched_price_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=False, index=True)
    property_type: Mapped[str] = mapped_column(String(1), nullable=False)  # D/S/T/F
    size_bin_m2: Mapped[int | None] = mapped_column(Integer, nullable=True)  # NULL = overall (not broken out by size)
    median_price_per_sqm: Mapped[float | None] = mapped_column(Float, nullable=True)  # HPI-adjusted to today's-equivalent
    matched_count: Mapped[int] = mapped_column(Integer, nullable=False)  # matched pairs actually used (sector- or outcode-level)
    estimate_grain: Mapped[str] = mapped_column(String(16), nullable=False)  # "sector" | "outcode" | "none"
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class RailStation(Base):
    """One National Rail station, from NaPTAN (the free, no-signup UK
    government dataset of public transport access points -- see
    geo_model.rail_stations). ``atco_code`` is NaPTAN's own identifier
    (e.g. "9100PADTON") -- NOT a CRS code; the fares feed (once wired up)
    brings its own station reference list keyed by CRS/NLC, matched to
    these rows by name/location rather than by a shared code, since NaPTAN
    doesn't carry CRS directly."""

    __tablename__ = "rail_stations"

    atco_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    lat: Mapped[float] = mapped_column(Float, nullable=False)
    long: Mapped[float] = mapped_column(Float, nullable=False)
    last_updated: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SectorStation(Base):
    """The N closest rail stations to one postcode sector's centroid,
    computed by geo_model.domain.stations from RailStation -- a sector's
    "closest station" is rarely singular enough to price a commute from,
    so every candidate within the top N is kept (rank 1 = closest), not
    just the nearest, letting a viewer compare fares across alternatives
    (e.g. a 10-minute-further station on a cheaper fare zone)."""

    __tablename__ = "sector_stations"
    __table_args__ = (
        UniqueConstraint("sector", "station_atco_code", name="uq_sector_station_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=False, index=True)
    station_atco_code: Mapped[str] = mapped_column(String(16), ForeignKey("rail_stations.atco_code"), nullable=False, index=True)
    station_name: Mapped[str] = mapped_column(String(128), nullable=False)
    distance_miles: Mapped[float] = mapped_column(Float, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)  # 1 = closest
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class FareLocation(Base):
    """One station/cluster location from the ATOC/RSP fares feed's LOC
    file (see geo_model.fares_data) -- NLC-keyed, distinct from RailStation
    (NaPTAN, atco_code-keyed). Kept for every real physical station with a
    CRS code (PlusBus/zone-only locations, which have no CRS, are never
    stored -- they can't be a rail journey endpoint). ``fare_group_nlc``
    equals ``nlc`` itself for a location that isn't part of a cluster;
    otherwise it's a shared cluster NLC (e.g. '1072' = "London Terminals"),
    and flows are frequently only recorded against the cluster, not the
    individual station -- see geo_model.domain.station_fares."""

    __tablename__ = "fare_locations"

    nlc: Mapped[str] = mapped_column(String(4), primary_key=True)
    description: Mapped[str] = mapped_column(String(16), nullable=False)  # raw, hand-abbreviated to fit 16 chars -- see station_fares module docstring
    crs_code: Mapped[str] = mapped_column(String(3), nullable=False, index=True)
    fare_group_nlc: Mapped[str] = mapped_column(String(4), nullable=False, index=True)


class FareTicketType(Base):
    """One ticket-type code from the fares feed's TTY file -- e.g. 'SDR' =
    Anytime Day Return standard, '7DS' = 7-Day (Weekly) Season standard.
    Small (~4,000 rows), stored in full."""

    __tablename__ = "fare_ticket_types"

    ticket_code: Mapped[str] = mapped_column(String(3), primary_key=True)
    description: Mapped[str] = mapped_column(String(16), nullable=False)
    tkt_class: Mapped[str] = mapped_column(String(1), nullable=False)  # '1' first, '2' standard, '9' undefined
    tkt_type: Mapped[str] = mapped_column(String(1), nullable=False)  # 'S' single, 'R' return, 'N' season


class FareFlow(Base):
    """One flow (an origin/destination NLC pair with a route) from the
    fares feed's FFL file, restricted at ingest time to flows connecting
    our origin stations (geo_model.domain.stations' sector_stations) to
    our destination targets (nearest station(s) to each reference point)
    -- see geo_model.fares_data.parse_flows_and_fares. The full feed is
    ~8.3M lines; storing every flow nationwide would dwarf everything else
    in this DB for no benefit we currently need."""

    __tablename__ = "fare_flows"

    flow_id: Mapped[str] = mapped_column(String(7), primary_key=True)
    origin_nlc: Mapped[str] = mapped_column(String(4), nullable=False, index=True)
    dest_nlc: Mapped[str] = mapped_column(String(4), nullable=False, index=True)
    route_code: Mapped[str] = mapped_column(String(5), nullable=False)
    status_code: Mapped[str] = mapped_column(String(3), nullable=False)  # '000' = adult
    usage_code: Mapped[str] = mapped_column(String(1), nullable=False)  # 'A' actual, 'G' generated
    direction: Mapped[str] = mapped_column(String(1), nullable=False)  # 'S' single-direction, 'R' reversible
    toc: Mapped[str] = mapped_column(String(3), nullable=False)


class Fare(Base):
    """One ticket-code's fare for one flow, from the fares feed's FFL file
    -- same ingest-time restriction as FareFlow."""

    __tablename__ = "fares"
    __table_args__ = (
        UniqueConstraint("flow_id", "ticket_code", name="uq_fare_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_id: Mapped[str] = mapped_column(String(7), ForeignKey("fare_flows.flow_id"), nullable=False, index=True)
    ticket_code: Mapped[str] = mapped_column(String(3), nullable=False, index=True)
    fare_pence: Mapped[int] = mapped_column(Integer, nullable=False)
    restriction_code: Mapped[str | None] = mapped_column(String(2), nullable=True)


class StationFareMatch(Base):
    """RailStation (NaPTAN, atco_code-keyed) joined to FareLocation (fares
    feed, NLC-keyed) by name, produced by geo_model.domain.station_fares --
    the two datasets share no common code (see RailStation's docstring).
    Every RailStation gets a row, even an "unmatched" one (nlc/crs_code/
    fare_group_nlc all NULL) -- never silently dropped, so a station that
    should have fare data but doesn't can be looked up and the reason seen,
    same rationale as MatchedPropertySale's "ambiguous" tier. Only "exact"
    and "fuzzy" rows are usable for fare lookups."""

    __tablename__ = "station_fare_matches"

    station_atco_code: Mapped[str] = mapped_column(String(16), ForeignKey("rail_stations.atco_code"), primary_key=True)
    nlc: Mapped[str | None] = mapped_column(String(4), nullable=True, index=True)
    crs_code: Mapped[str | None] = mapped_column(String(3), nullable=True)
    fare_group_nlc: Mapped[str | None] = mapped_column(String(4), nullable=True)
    # "exact": normalized names matched directly (after abbreviation
    #   expansion, e.g. STREET->ST). "fuzzy": matched via a vowel-dropped
    #   consonant-skeleton comparison (the fares feed's 16-char DESCRIPTION
    #   field abbreviates by hand, e.g. "Paddington" -> "PDDNGTN") --
    #   restricted to long/multi-word names to avoid the false positives
    #   short names produce (e.g. "Chelmsford" incorrectly resembling
    #   "Chelsfield"). "unmatched": no safe candidate found.
    confidence: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    match_note: Mapped[str | None] = mapped_column(String(256), nullable=True)
    matched_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SectorStationFare(Base):
    """Computed Anytime Day Return + Monthly Season cost from one of a
    sector's nearest stations (SectorStation) to one reference point,
    produced by geo_model.pipeline.compute_sector_station_fares. Monthly
    season is never a direct fare-feed line item for standard class (only
    First Class has one) -- it's computed from a real Weekly fare using
    the nationally fixed regulated multiplier (Weekly x 3.84), not looked
    up; ``monthly_is_computed`` records that plainly so the frontend can
    label it rather than imply it was fetched verbatim.

    Which Weekly fare depends on ``monthly_fare_basis``: "zones_1_6_
    travelcard" means the Weekly is the Zones 1-6 Travelcard-inclusive
    one (what a commuter from outside the zonal boundary actually buys,
    and what a fare-comparison site shows by default -- see
    geo_model.domain.station_fares.select_monthly_season); "station_or_
    cluster" means the plain rail-only Weekly to the destination station/
    cluster (used when no Zones 1-6 flow exists at all, e.g. a station
    already inside the TfL zones like Charlton). The two are not
    interchangeable -- surfaced so the frontend can label which kind of
    price it's showing."""

    __tablename__ = "sector_station_fares"
    __table_args__ = (
        UniqueConstraint("sector", "station_atco_code", "reference_point_name", name="uq_sector_station_fare_identity"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sector: Mapped[str] = mapped_column(String(8), ForeignKey("postcode_sectors.sector"), nullable=False, index=True)
    station_atco_code: Mapped[str] = mapped_column(String(16), ForeignKey("rail_stations.atco_code"), nullable=False, index=True)
    station_name: Mapped[str] = mapped_column(String(128), nullable=False)
    station_rank: Mapped[int] = mapped_column(Integer, nullable=False)  # copied from SectorStation.rank at compute time
    reference_point_name: Mapped[str] = mapped_column(
        String(128), ForeignKey("reference_points.name"), nullable=False, index=True
    )
    anytime_day_return_pence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    monthly_season_pence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    monthly_is_computed: Mapped[bool] = mapped_column(default=True)  # always True today -- see class docstring
    monthly_fare_basis: Mapped[str | None] = mapped_column(String(24), nullable=True)  # "zones_1_6_travelcard" | "station_or_cluster" -- see class docstring
    computed_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
