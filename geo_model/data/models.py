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
    """One outcode's total score for one run."""

    __tablename__ = "run_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_config_id: Mapped[str] = mapped_column(String(32), ForeignKey("run_configs.id"), nullable=False, index=True)
    outcode: Mapped[str] = mapped_column(String(8), ForeignKey("outcodes.outcode"), nullable=False, index=True)
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
