#!/usr/bin/env python3
"""CLI/interface layer -- this is what Claude actually executes when the
user asks it to run/refresh the model. Thin argument parsing only; all
real logic lives in geo_model.pipeline and below.

Usage:
    python scripts/run_model.py seed-outcodes
    python scripts/run_model.py seed-legacy
    python scripts/run_model.py refresh-geo-data [--outcodes-file PATH] [--overrides-file PATH]
    python scripts/run_model.py run-model [--outcodes-file PATH] [--overrides-file PATH]
    python scripts/run_model.py usage-summary [--month | --since-days N] [--provider here]

With no --outcodes-file, seed-outcodes, refresh-geo-data and run-model all
default to connector_scraper_data/outcodes_london_and_home_counties.txt
(681 outcodes: Inner/Greater London + the surrounding commuter-belt
counties) -- NOT the full ~2,900 UK outcodes. Pass --outcodes-file
connector_scraper_data/outcodes_london_sample.txt for a cheap 16-outcode
smoke test, or --all to scope to the whole outcodes.txt list.

A first full run against the 681-outcode default means roughly outcodes x
categories (~7,500) discover calls plus outcodes x reference-points (~1,360)
travel-time calls -- real HERE quota, not free. Check
`usage-summary` before and after a big run. Subsequent runs only fetch
what's missing, so this cost is mostly one-time per outcode/category pair.

--overrides-file takes a JSON file shaped like the Artifact's config/current
doc (what the dashboard's settings panel saves) and merges it on top of
config.yaml, so a re-run picks up weights/reference-points a viewer edited
in the dashboard rather than always using the repo's checked-in defaults.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from geo_model import pipeline  # noqa: E402
from geo_model.artifact_sync import artifact_doc_to_overrides, export_config_doc, export_geo_labels, export_results_for_artifact  # noqa: E402
from geo_model.config import load_model_config  # noqa: E402
from geo_model.data.db import get_session  # noqa: E402
from geo_model.logging_setup import get_logger  # noqa: E402
from geo_model.grammar_schools import import_grammar_schools  # noqa: E402
from geo_model.postcodes import backfill_outcode_areas, compute_sector_centroids, seed_outcodes_table  # noqa: E402
from geo_model.epc_data import download_full_load_csv, ingest_epc_data  # noqa: E402
from geo_model.price_data import ingest_hpi_index, ingest_price_paid_data  # noqa: E402
from geo_model.private_schools import import_private_schools  # noqa: E402
from geo_model.seed_legacy_data import seed_amenities_from_legacy_data  # noqa: E402
from geo_model.usage_report import current_calendar_month_start, summarize_usage  # noqa: E402

logger = get_logger(__name__)

DEFAULT_OUTCODES_FILE = REPO_ROOT / "connector_scraper_data" / "outcodes.txt"
DEFAULT_PRIVATE_SCHOOLS_CSV = REPO_ROOT / "reference_data" / "private_schools_greater_london.csv"
DEFAULT_GRAMMAR_SCHOOLS_CSV = REPO_ROOT / "reference_data" / "grammar_schools_london_home_counties.csv"
DEFAULT_SCOPE_FILE = REPO_ROOT / "connector_scraper_data" / "outcodes_london_and_home_counties.txt"
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


def cmd_backfill_areas(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    scope = _read_scope(args.outcodes_file, args.all)
    with get_session() as session:
        n = backfill_outcode_areas(session, outcode_filter=scope)
    print(f"Backfilled borough/geo_group for {n} outcodes.")


def cmd_ingest_price_data(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    scope = _read_scope(args.outcodes_file, args.all)
    if scope is None:
        raise SystemExit("ingest-price-data requires a bounded outcode scope (default is fine) -- --all would try to pull all of England & Wales")
    outcodes = set(scope)
    with get_session() as session:
        ppd_result = ingest_price_paid_data(session, outcodes, years=args.years)
        hpi_result = ingest_hpi_index(session)
    print(json.dumps({"price_paid": ppd_result, "hpi": hpi_result}, indent=2))


def cmd_compute_sector_centroids(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    with get_session() as session:
        n = compute_sector_centroids(session)
    print(f"Computed centroids for {n} postcode sectors.")


def cmd_compute_sector_prices(args: argparse.Namespace) -> None:
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.compute_sector_prices(outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_ingest_epc_data(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    scope = _read_scope(args.outcodes_file, args.all)
    if scope is None:
        raise SystemExit("ingest-epc-data requires a bounded outcode scope (default is fine) -- --all would try to pull all of England & Wales")
    outcodes = set(scope)
    zip_path = args.zip_path
    if zip_path is None:
        zip_path = REPO_ROOT / "epc-domestic-full-load.zip"
        download_full_load_csv(zip_path)
    with get_session() as session:
        result = ingest_epc_data(session, outcodes, zip_path)
    print(json.dumps(result, indent=2))


def cmd_compute_sector_floor_area(args: argparse.Namespace) -> None:
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.compute_sector_floor_area(outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_match_epc_to_price_paid(args: argparse.Namespace) -> None:
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.match_epc_to_price_paid(outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_compute_matched_sector_prices(args: argparse.Namespace) -> None:
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.compute_matched_sector_prices(outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_seed_legacy(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    with get_session() as session:
        counts = seed_amenities_from_legacy_data(session, REFERENCE_DATA_DIR)
    print(json.dumps(counts, indent=2))


def cmd_import_private_schools(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    with get_session() as session:
        result = import_private_schools(session, args.csv)
    print(json.dumps(result, indent=2))


def cmd_import_grammar_schools(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    with get_session() as session:
        result = import_grammar_schools(session, args.csv)
    print(json.dumps(result, indent=2))


def _load_config_with_overrides(overrides_file: Path | None):
    overrides = None
    if overrides_file is not None:
        with open(overrides_file, "r", encoding="utf-8") as f:
            doc = json.load(f)
        overrides = artifact_doc_to_overrides(doc)
    return load_model_config(overrides=overrides)


def cmd_refresh_geo_data(args: argparse.Namespace) -> None:
    config = _load_config_with_overrides(args.overrides_file)
    scope = _read_scope(args.outcodes_file, args.all)
    result = pipeline.refresh_geo_data(config, outcode_filter=scope)
    print(json.dumps(result, indent=2))


def cmd_run_model(args: argparse.Namespace) -> None:
    config = _load_config_with_overrides(args.overrides_file)
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


def cmd_export_artifact_labels(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    result = export_geo_labels(args.out)
    print(json.dumps(result, indent=2))


def cmd_usage_summary(args: argparse.Namespace) -> None:
    pipeline.ensure_db_ready()
    since = None
    if args.month:
        since = current_calendar_month_start()
    elif args.since_days is not None:
        since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.since_days)

    with get_session() as session:
        summary = summarize_usage(session, since=since, provider=args.provider)

    print(json.dumps(
        {
            "note": "Local count of calls this pipeline made and got a response for -- not a substitute for the provider's own usage/billing dashboard.",
            "since": summary.since.isoformat() if summary.since else "all time",
            "total_calls": summary.total_calls,
            "by_provider": summary.by_provider,
            "by_call_type": summary.by_call_type,
            "by_status_code": summary.by_status,
            "first_call_at": summary.first_call_at.isoformat() if summary.first_call_at else None,
            "last_call_at": summary.last_call_at.isoformat() if summary.last_call_at else None,
        },
        indent=2,
    ))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("seed-legacy", help="Import v2's reference_data/*.txt as a stale-flagged starting amenity cache")

    p = sub.add_parser("import-private-schools", help="Import/geocode the private schools register (reference_data/private_schools_greater_london.csv)")
    p.add_argument("--csv", type=Path, default=DEFAULT_PRIVATE_SCHOOLS_CSV)
    p.set_defaults(func=cmd_import_private_schools)

    p = sub.add_parser("import-grammar-schools", help="Import/geocode the grammar schools register (reference_data/grammar_schools_london_home_counties.csv)")
    p.add_argument("--csv", type=Path, default=DEFAULT_GRAMMAR_SCHOOLS_CSV)
    p.set_defaults(func=cmd_import_grammar_schools)

    p = sub.add_parser("compute-sector-centroids", help="Derive a lat/long centroid for every postcode sector present in price_paid_transactions")
    p.set_defaults(func=cmd_compute_sector_centroids)

    p = sub.add_parser("compute-sector-prices", help="Aggregate price_paid_transactions + hpi_index into sector_prices (HPI-adjusted median per sector x property type)")
    p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to (default: London + Home Counties)")
    p.add_argument("--all", action="store_true", help="Scope to every outcode with ingested transactions")
    p.set_defaults(func=cmd_compute_sector_prices)

    p = sub.add_parser("ingest-price-data", help="Fetch Price Paid Data (recent years) + the UK HPI, filtered to our outcode scope")
    p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to (default: London + Home Counties)")
    p.add_argument("--all", action="store_true", help="Refuse -- ingest-price-data always needs a bounded scope, see --outcodes-file")
    p.add_argument("--years", type=int, nargs="+", default=[2021, 2022, 2023, 2024, 2025, 2026], help="Which pp-<year>.csv files to pull")
    p.set_defaults(func=cmd_ingest_price_data)

    p = sub.add_parser("ingest-epc-data", help="Fetch the EPC domestic full-load CSV (needs EPC_API_KEY) and extract floor area, filtered to our outcode scope")
    p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to (default: London + Home Counties)")
    p.add_argument("--all", action="store_true", help="Refuse -- ingest-epc-data always needs a bounded scope, see --outcodes-file")
    p.add_argument("--zip-path", type=Path, default=None, help="Path to an already-downloaded domestic-csv.zip, to skip re-downloading the ~8GB file")
    p.set_defaults(func=cmd_ingest_epc_data)

    p = sub.add_parser("compute-sector-floor-area", help="Aggregate epc_certificates into sector_floor_areas (median m^2 per sector x property type)")
    p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to (default: London + Home Counties)")
    p.add_argument("--all", action="store_true", help="Scope to every outcode with ingested certificates")
    p.set_defaults(func=cmd_compute_sector_floor_area)

    p = sub.add_parser("match-epc-to-price-paid", help="Join epc_certificates to price_paid_transactions by address (postcode + house/flat number) into matched_property_sales")
    p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to (default: London + Home Counties)")
    p.add_argument("--all", action="store_true", help="Scope to every outcode with ingested data")
    p.set_defaults(func=cmd_match_epc_to_price_paid)

    p = sub.add_parser("compute-matched-sector-prices", help="Aggregate matched_property_sales into sector_matched_prices (real price/m^2, not divided medians)")
    p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to (default: London + Home Counties)")
    p.add_argument("--all", action="store_true", help="Scope to every outcode with matched sales")
    p.set_defaults(func=cmd_compute_matched_sector_prices)

    for name, fn in (
        ("seed-outcodes", cmd_seed_outcodes),
        ("backfill-areas", cmd_backfill_areas),
        ("refresh-geo-data", cmd_refresh_geo_data),
        ("run-model", cmd_run_model),
    ):
        p = sub.add_parser(name)
        p.add_argument("--outcodes-file", type=Path, default=None, help="Newline-delimited outcode list to scope to")
        p.add_argument("--all", action="store_true", help="Scope to every outcode in outcodes.txt, not just London + Home Counties")
        p.add_argument(
            "--overrides-file", type=Path, default=None,
            help="JSON file shaped like the Artifact's config/current doc (weights/reference-points); "
                 "merged on top of config.yaml via artifact_sync.artifact_doc_to_overrides",
        )
        p.set_defaults(func=fn)

    p = sub.add_parser("export-artifact-config", help="Write config.yaml as a config/current doc for the Artifact frontend")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "frontend" / "export" / "config.json")
    p.set_defaults(func=cmd_export_artifact_config)

    p = sub.add_parser("export-artifact-results", help="Write the latest (or a given) run as results/latest docs for the Artifact frontend")
    p.add_argument("--run-id", type=str, default=None, help="Defaults to the most recent run in the DB")
    p.add_argument("--out-dir", type=Path, default=REPO_ROOT / "frontend" / "export" / "results")
    p.add_argument("--example", action="store_true", help="Flag this export as example/illustrative data")
    p.set_defaults(func=cmd_export_artifact_results)

    p = sub.add_parser("export-artifact-labels", help="Write results/latest/labels (map county/town label points) for the Artifact frontend")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "frontend" / "export" / "labels.json")
    p.set_defaults(func=cmd_export_artifact_labels)

    p = sub.add_parser("usage-summary", help="Summarize provider API calls this pipeline has made and recorded")
    p.add_argument("--provider", type=str, default=None, help="Restrict to one provider (e.g. 'here')")
    scope_group = p.add_mutually_exclusive_group()
    scope_group.add_argument("--month", action="store_true", help="Only calls since the start of the current calendar month")
    scope_group.add_argument("--since-days", type=int, default=None, help="Only calls in the last N days")
    p.set_defaults(func=cmd_usage_summary)

    args = parser.parse_args()
    if args.command == "seed-legacy":
        cmd_seed_legacy(args)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
