#!/usr/bin/env python3
"""CLI/interface layer -- this is what Claude actually executes when the
user asks it to run/refresh the model. Thin argument parsing only; all
real logic lives in geo_model.pipeline and below.

Usage:
    python scripts/run_model.py seed-outcodes
    python scripts/run_model.py seed-legacy
    python scripts/run_model.py refresh-geo-data [--outcodes-file PATH]
    python scripts/run_model.py run-model [--outcodes-file PATH]

With no --outcodes-file, seed-outcodes, refresh-geo-data and run-model all
default to a small London sample in
connector_scraper_data/outcodes_london_sample.txt -- NOT the full ~2,900 UK
outcodes -- because a full run means outcodes x categories (and x reference
points, for travel time) individual HERE API calls, which is expensive and
slow to do by accident. Pass --all to scope to the whole outcodes.txt list.
(v2's own outcodes_debug_London_zone1_zone2.txt turned out to contain a
single outcode -- a debug fixture, not a usable London subset -- so this
sample file replaces it as the default scope.)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from geo_model import pipeline  # noqa: E402
from geo_model.artifact_sync import export_config_doc, export_results_for_artifact  # noqa: E402
from geo_model.config import load_model_config  # noqa: E402
from geo_model.data.db import get_session  # noqa: E402
from geo_model.logging_setup import get_logger  # noqa: E402
from geo_model.postcodes import seed_outcodes_table  # noqa: E402
from geo_model.seed_legacy_data import seed_amenities_from_legacy_data  # noqa: E402

logger = get_logger(__name__)

DEFAULT_OUTCODES_FILE = REPO_ROOT / "connector_scraper_data" / "outcodes.txt"
DEFAULT_SCOPE_FILE = REPO_ROOT / "connector_scraper_data" / "outcodes_london_sample.txt"
REFERENCE_DATA_DIR = REPO_ROOT / "reference_data"


def _read_scope(outcodes_file: Path | None, use_all: bool) -> list[str] | None:
    if use_all:
        return None  # None == no filter == every outcode in the DB
    path = outcodes_file or DEFAULT_SCOPE_FILE
    with open(path, "r", encoding="utf-8") as f:
        codes = [line.strip() for line in f if line.strip()]
    logger.info("Scoping to %d outcodes from %s", len(codes), path)
    return codes


def cmd_seed_outcodes(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    scope = _read_scope(args.outcodes_file, args.all)
    with get_session() as session:
        n = seed_outcodes_table(session, DEFAULT_OUTCODES_FILE, outcode_filter=scope)
    print(f"Seeded {n} outcode centroids.")


def cmd_seed_legacy(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    with get_session() as session:
        counts = seed_amenities_from_legacy_data(session, REFERENCE_DATA_DIR)
    print(json.dumps(counts, indent=2))


def cmd_refresh_geo_data(args: argparse.Namespace) -> None:
    config = load_model_config()
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.refresh_geo_data(config, outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_run_model(args: argparse.Namespace) -> None:
    config = load_model_config()
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.run_model(config, outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_export_artifact_config(args: argparse.Namespace) -> None:
    doc = export_config_doc(args.out)
    print(f"Wrote {args.out} ({len(doc['amenity_categories'])} categories, {len(doc['reference_points'])} reference points)")


def cmd_export_artifact_results(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    result = export_results_for_artifact(args.out_dir, run_id=args.run_id, is_example=args.example)
    print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("seed-legacy", help="Import v2's reference_data/*.txt as a stale-flagged starting amenity cache")

    for name, fn in (
        ("seed-outcodes", cmd_seed_outcodes),
        ("refresh-geo-data", cmd_refresh_geo_data),
        ("run-model", cmd_run_model),
    ):
        p = sub.add_parser(name)
        p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to")
        p.add_argument("--all", action="store_true", help="Scope to every outcode in outcodes.txt, not just the London sample")
        p.set_defaults(func=fn)

    p = sub.add_parser("export-artifact-config", help="Write config.yaml as a config/current doc for the Artifact frontend")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "frontend" / "export" / "config.json")
    p.set_defaults(func=cmd_export_artifact_config)

    p = sub.add_parser("export-artifact-results", help="Write the latest (or a given) run as results/latest docs for the Artifact frontend")
    p.add_argument("--run-id", type=str, default=None, help="Defaults to the most recent run in the DB")
    p.add_argument("--out-dir", type=Path, default=REPO_ROOT / "frontend" / "export" / "results")
    p.add_argument("--example", action="store_true", help="Flag this export as example/illustrative data")
    p.set_defaults(func=cmd_export_artifact_results)

    args = parser.parse_args()
    if args.command == "seed-legacy":
        cmd_seed_legacy(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
