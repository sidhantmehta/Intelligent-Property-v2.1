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
from geo_model.data.models import Outcode, RunConfig, RunResult


def config_to_artifact_doc(config: ModelConfig) -> dict:
    return {
        "amenity_categories": [
            {"key": c.key, "label": c.label, "weight": c.weight, "query": c.query}
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
    provider query -- so a query missing from the doc (an older save, or a
    category the viewer never touched) falls back to config.yaml's."""
    overrides: dict = {}
    if "amenity_categories" in doc:
        defaults = {c["key"]: c for c in config_to_artifact_doc(load_model_config())["amenity_categories"]}
        overrides["amenity_categories"] = [
            {
                "key": c["key"],
                "label": c.get("label", defaults.get(c["key"], {}).get("label", c["key"])),
                "weight": c["weight"],
                "query": c.get("query", defaults.get(c["key"], {}).get("query", c["key"])),
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


def export_results_for_artifact(out_dir: Path, run_id: str | None = None, is_example: bool = False) -> dict:
    """Writes ``out_dir/latest.json`` (the results/latest metadata doc) and
    one ``out_dir/outcodes/<outcode>.json`` per outcode (each a
    results/latest/outcodes/<outcode> doc) for the given run (defaulting to
    the most recent one in the DB)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    outcodes_dir = out_dir / "outcodes"
    outcodes_dir.mkdir(parents=True, exist_ok=True)

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

        outcode_docs = []
        for r in results:
            o = outcode_rows.get(r.outcode)
            if o is None:
                continue
            categories = {
                c.category_key: {
                    "raw_score": c.raw_score,
                    "normalized_score": c.normalized_score,
                    "weight_applied": c.weight_applied,
                }
                for c in r.categories
            }
            doc = {
                "outcode": r.outcode,
                "lat": o.lat,
                "long": o.long,
                "total_score": r.total_score,
                "categories": categories,
            }
            (outcodes_dir / f"{r.outcode}.json").write_text(json.dumps(doc))
            outcode_docs.append(doc)

        meta = {
            "run_id": run_config.id,
            "created_at": run_config.created_at.isoformat(),
            "outcode_count": len(outcode_docs),
            "is_example": is_example,
        }
        (out_dir / "latest.json").write_text(json.dumps(meta, indent=2))

    return {"meta": meta, "outcode_count": len(outcode_docs), "outcodes_dir": str(outcodes_dir)}
