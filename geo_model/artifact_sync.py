"""Bridges geo_model's SQLite state and the Claude Artifact frontend's `db`
documents (config/current, results/latest, results/latest/outcodes/*).

This is an interface/adapter layer, like scripts/run_model.py: it reads
SQLite and writes plain JSON files to disk, or reads a JSON file back into
an override dict. It never talks to the Artifact platform itself -- that
happens from the Claude session via the Artifact tool's read_db/write_db,
pointed at the files this module produces, or handing this module the
files that tool's read_db saved.
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select

from geo_model.config import ModelConfig, load_model_config
from geo_model.data.db import get_session
from geo_model.data.models import Amenity, Outcode, PostcodeSector, RunConfig, RunResult, SectorFloorArea, SectorPrice
from geo_model.domain import scoring

# How many of the nearest amenities to embed per outcode/category in the
# Artifact export, for the table's click-to-drill-down view. Capped well
# below max_amenities_per_category (20) to keep each outcode doc's payload
# small -- the closest handful is what a drill-down view actually needs.
MAX_DRILLDOWN_AMENITIES = 10


def config_to_artifact_doc(config: ModelConfig) -> dict:
    return {
        "amenity_categories": [
            {"key": c.key, "label": c.label, "weight": c.weight, "query": c.query, "provider": c.provider}
            for c in config.amenity_categories
        ],
        "reference_points": [
            {"name": r.name, "address": r.address, "weight": r.weight} for r in config.reference_points
        ],
        "radius_bins_miles": list(config.radius_bins_miles),
    }


def artifact_doc_to_overrides(doc: dict) -> dict:
    """Inverse of config_to_artifact_doc, shaped for
    geo_model.config.load_model_config(overrides=...). The settings form
    only lets a viewer edit weights/names/addresses, not each category's
    provider or query -- so those missing from the doc (an older save, or
    a category the viewer never touched) fall back to config.yaml's. This
    matters beyond cosmetics: a category whose `provider` silently reset
    to the default on save (e.g. private_schools losing its
    local_dataset assignment) would start hitting HERE instead."""
    overrides: dict = {}
    if "amenity_categories" in doc:
        defaults = {c["key"]: c for c in config_to_artifact_doc(load_model_config())["amenity_categories"]}
        overrides["amenity_categories"] = [
            {
                "key": c["key"],
                "label": c.get("label", defaults.get(c["key"], {}).get("label", c["key"])),
                "weight": c["weight"],
                "query": c.get("query", defaults.get(c["key"], {}).get("query", c["key"])),
                "provider": c.get("provider", defaults.get(c["key"], {}).get("provider")),
            }
            for c in doc["amenity_categories"]
        ]
    if "reference_points" in doc:
        overrides["reference_points"] = [
            {"name": r["name"], "address": r["address"], "weight": r.get("weight", 5)}
            for r in doc["reference_points"]
        ]
    if "radius_bins_miles" in doc:
        overrides["radius_bins_miles"] = doc["radius_bins_miles"]
    return overrides


def export_config_doc(path: Path) -> dict:
    doc = config_to_artifact_doc(load_model_config())
    doc["updated_at"] = None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2))
    return doc


def sector_doc_id(sector: str) -> str:
    """A postcode sector like "SW11 1" isn't a valid `db` document id
    (space isn't in the allowed id character set) -- this is the sanitized
    id used for its doc/filename, while the doc's own ``sector`` field
    keeps the real, spaced value."""
    return sector.replace(" ", "_")


def export_results_for_artifact(out_dir: Path, run_id: str | None = None, is_example: bool = False) -> dict:
    """Writes ``out_dir/latest.json`` (the results/latest metadata doc),
    one ``out_dir/sectors/<sector_id>.json`` per postcode sector (each a
    results/latest/sectors/<sector_id> doc, the table/map's row grain),
    and one ``out_dir/amenities/<outcode>.json`` per PARENT outcode (each
    a results/latest/amenities/<outcode> doc) for the given run
    (defaulting to the most recent one in the DB).

    The amenities drill-down detail is kept in its OWN collection, fetched
    lazily by the frontend only when a viewer expands a cell, rather than
    embedded in the sector doc. Embedding it there once made every
    outcodes doc ~10x bigger (~1KB -> ~11KB); multiplied across ~700
    outcodes that pushed the ALWAYS-subscribed results/latest/outcodes
    collection's total realtime payload well past what a single
    onSnapshot listener reliably delivers (confirmed: a plain `list` read
    against that collection silently truncated at ~400 of 681 docs despite
    asking for 1000), which is what made the dashboard's table and map go
    blank. Keeping sector docs lean (score + per-category raw/normalized
    numbers + price, no amenity names/distances) is what makes the
    realtime subscription work reliably again -- amenities stay keyed by
    the shared PARENT outcode (not duplicated per sector), so this
    collection is still one doc per outcode (~700), not per sector."""
    out_dir.mkdir(parents=True, exist_ok=True)
    sectors_dir = out_dir / "sectors"
    sectors_dir.mkdir(parents=True, exist_ok=True)
    amenities_dir = out_dir / "amenities"
    amenities_dir.mkdir(parents=True, exist_ok=True)

    with get_session() as session:
        if run_id is None:
            run_config = session.scalars(select(RunConfig).order_by(RunConfig.created_at.desc())).first()
        else:
            run_config = session.get(RunConfig, run_id)
        if run_config is None:
            raise ValueError("No run_config found in the DB -- run `run-model` first")

        results = list(session.scalars(select(RunResult).where(RunResult.run_config_id == run_config.id)))
        outcode_rows = {
            o.outcode: o
            for o in session.scalars(select(Outcode).where(Outcode.outcode.in_([r.outcode for r in results])))
        }

        # Nearest-amenity detail per outcode/category, for the frontend's
        # click-a-cell drill-down. Amenity rows have no run_id (they're a
        # standing cache, not run-scoped), so this reads whatever's
        # currently cached for these outcodes -- the same rows run_model
        # itself scored against, unless a refresh has since replaced them.
        radius_bins = json.loads(run_config.radius_bins_json)
        amenity_rows = session.scalars(
            select(Amenity).where(Amenity.outcode.in_(outcode_rows.keys())).order_by(Amenity.distance_m)
        )
        amenities_by_outcode: dict[str, dict[str, list[dict]]] = {}
        for a in amenity_rows:
            o = outcode_rows.get(a.outcode)
            if o is None:
                continue
            by_category = amenities_by_outcode.setdefault(a.outcode, {})
            bucket = by_category.setdefault(a.category_key, [])
            if len(bucket) >= MAX_DRILLDOWN_AMENITIES:
                continue
            distance_miles = (
                a.distance_m / 1609.34 if a.distance_m is not None
                else scoring.haversine_miles(o.lat, o.long, a.lat, a.long)
            )
            # Short keys (n/d/s) deliberately -- this list is duplicated
            # into every one of ~700 outcode documents, so field-name
            # overhead adds up fast.
            bucket.append({
                "n": a.title,
                "d": round(distance_miles, 3),
                "s": round(scoring.radius_weight(distance_miles, radius_bins), 3),
            })

        for outcode in outcode_rows:
            amenities_doc = {"outcode": outcode, "amenities": amenities_by_outcode.get(outcode, {})}
            (amenities_dir / f"{outcode}.json").write_text(json.dumps(amenities_doc))

        sectors_in_scope = [r.sector for r in results if r.sector]
        sector_rows = {
            s.sector: s
            for s in session.scalars(select(PostcodeSector).where(PostcodeSector.sector.in_(sectors_in_scope)))
        }
        price_rows = list(session.scalars(select(SectorPrice).where(SectorPrice.sector.in_(sectors_in_scope))))
        prices_by_sector: dict[str, dict[str, dict]] = {}
        for p in price_rows:
            prices_by_sector.setdefault(p.sector, {})[p.property_type] = {
                "median_price": p.median_price,
                "transaction_count": p.transaction_count,
                "grain": p.estimate_grain,
            }

        # Floor area comes from a wholly separate source (EPC certificates,
        # not Land Registry) and can legitimately be missing/backed-off
        # independently of price -- e.g. a sector with plenty of sales but
        # few EPCs on file. price_per_sqm is derived here (not stored) from
        # whichever grain each side resolved to; it's still a genuine
        # sector+type-level ratio, just not from paired individual sales.
        floor_area_rows = list(
            session.scalars(select(SectorFloorArea).where(SectorFloorArea.sector.in_(sectors_in_scope)))
        )
        floor_area_by_sector: dict[str, dict[str, dict]] = {}
        for f in floor_area_rows:
            floor_area_by_sector.setdefault(f.sector, {})[f.property_type] = {
                "median_floor_area_m2": f.median_floor_area_m2,
                "certificate_count": f.certificate_count,
                "grain": f.estimate_grain,
            }

        sector_docs = []
        for r in results:
            o = outcode_rows.get(r.outcode)
            ps = sector_rows.get(r.sector) if r.sector else None
            if o is None or ps is None:
                continue
            categories = {
                c.category_key: {
                    "raw_score": c.raw_score,
                    "normalized_score": c.normalized_score,
                    "weight_applied": c.weight_applied,
                }
                for c in r.categories
            }
            prices = {k: dict(v) for k, v in prices_by_sector.get(r.sector, {}).items()}
            floor_areas = floor_area_by_sector.get(r.sector, {})
            for ptype, fa in floor_areas.items():
                entry = prices.setdefault(ptype, {})
                entry["median_floor_area_m2"] = fa["median_floor_area_m2"]
                entry["floor_area_certificate_count"] = fa["certificate_count"]
                entry["floor_area_grain"] = fa["grain"]
                if entry.get("median_price") and fa["median_floor_area_m2"]:
                    entry["price_per_sqm"] = entry["median_price"] / fa["median_floor_area_m2"]
            doc = {
                "sector": r.sector,
                "outcode": r.outcode,
                "lat": ps.lat,
                "long": ps.long,
                "total_score": r.total_score,
                "categories": categories,
                "borough": o.borough,
                "geo_group": o.geo_group,
                "town": ps.town,
                "prices": prices,
            }
            (sectors_dir / f"{sector_doc_id(r.sector)}.json").write_text(json.dumps(doc))
            sector_docs.append(doc)

        meta = {
            "run_id": run_config.id,
            "created_at": run_config.created_at.isoformat(),
            "outcode_count": len(outcode_rows),
            "sector_count": len(sector_docs),
            "is_example": is_example,
        }
        (out_dir / "latest.json").write_text(json.dumps(meta, indent=2))

    return {
        "meta": meta,
        "sector_count": len(sector_docs),
        "sectors_dir": str(sectors_dir),
        "amenities_dir": str(amenities_dir),
    }


def export_geo_labels(out_path: Path) -> dict:
    """Writes the results/latest/labels doc: map label points for the
    dashboard's county/town labels. One "town" label per borough
    (centroid of that borough's outcodes) and one "county" label per
    Home Counties region (centroid of that region's outcodes) --
    "Greater London" is excluded from county labels since the borough
    labels already cover it. Independent of any run_config (it only
    depends on the outcodes table's borough/region columns from
    postcodes.py's backfill_outcode_areas), so this doesn't need a
    run_id and can be regenerated any time that backfill changes."""
    with get_session() as session:
        outcodes = list(session.scalars(select(Outcode).where(Outcode.borough.is_not(None))))

    def _centroids(key_fn, kind: str) -> list[dict]:
        groups: dict[str, list[Outcode]] = {}
        for o in outcodes:
            key = key_fn(o)
            if key:
                groups.setdefault(key, []).append(o)
        return [
            {
                "name": name,
                "lat": sum(o.lat for o in members) / len(members),
                "long": sum(o.long for o in members) / len(members),
                "kind": kind,
                "outcode_count": len(members),
            }
            for name, members in groups.items()
        ]

    labels = _centroids(lambda o: o.borough, "town") + _centroids(
        lambda o: o.region if o.region != "Greater London" else None, "county"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc = {"labels": labels}
    out_path.write_text(json.dumps(doc))
    return {"label_count": len(labels), "out_path": str(out_path)}
