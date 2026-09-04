"""Loads .env (secrets/paths) and config.yaml (scoring defaults) into a
single, immutable ``Settings``/``ModelConfig`` pair.

Nothing else in geo_model reads environment variables or config.yaml
directly -- every other module receives config as plain Python objects,
which is what keeps geo_model.domain unit-testable without any file I/O.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class AmenityCategory:
    key: str
    label: str
    weight: float
    query: str
    provider: str  # which GeoProvider (by name) answers nearby_amenities() for this category


@dataclass(frozen=True)
class ReferencePoint:
    name: str
    address: str
    weight: float


@dataclass(frozen=True)
class ModelConfig:
    provider: str
    amenity_categories: tuple[AmenityCategory, ...]
    radius_bins_miles: tuple[float, ...]
    radius_bin_labels: tuple[str, ...]
    max_amenities_per_category: int
    search_radius_miles: float
    reference_points: tuple[ReferencePoint, ...]
    travel_mode: str
    staleness_days: int

    def category_by_key(self, key: str) -> AmenityCategory:
        for c in self.amenity_categories:
            if c.key == key:
                return c
        raise KeyError(f"Unknown amenity category: {key}")


@dataclass(frozen=True)
class Settings:
    here_api_key: str | None
    db_path: Path
    config_path: Path
    log_level: str
    log_dir: Path


def load_settings() -> Settings:
    return Settings(
        here_api_key=os.environ.get("HERE_API_KEY") or None,
        db_path=Path(os.environ.get("GEO_MODEL_DB_PATH", "geo_model.db")),
        config_path=Path(os.environ.get("GEO_MODEL_CONFIG_PATH", "config.yaml")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        log_dir=Path(os.environ.get("GEO_MODEL_LOG_DIR", "logs")),
    )


def load_model_config(path: Path | None = None, overrides: dict[str, Any] | None = None) -> ModelConfig:
    """Load config.yaml, optionally merging a dict of overrides (e.g. the
    weights/reference-points snapshot the Artifact frontend produced) on
    top of it. ``overrides`` uses the same shape as the yaml file."""
    settings = load_settings()
    cfg_path = path or settings.config_path
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    if overrides:
        raw = _deep_merge(raw, overrides)

    categories = tuple(
        AmenityCategory(
            key=c["key"],
            label=c["label"],
            weight=float(c["weight"]),
            query=c["query"],
            provider=c.get("provider") or raw["provider"],  # falls back (None included) to the top-level provider
        )
        for c in raw["amenity_categories"]
    )
    reference_points = tuple(
        ReferencePoint(name=r["name"], address=r["address"], weight=float(r.get("weight", 5)))
        for r in raw["reference_points"]
    )

    return ModelConfig(
        provider=raw["provider"],
        amenity_categories=categories,
        radius_bins_miles=tuple(raw["radius_bins_miles"]),
        radius_bin_labels=tuple(raw["radius_bin_labels"]),
        max_amenities_per_category=int(raw["max_amenities_per_category"]),
        search_radius_miles=float(raw["search_radius_miles"]),
        reference_points=reference_points,
        travel_mode=raw["travel_mode"],
        staleness_days=int(raw["geo_data"]["staleness_days"]),
    )


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
