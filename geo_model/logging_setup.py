"""Central logging configuration for every layer of geo_model.

Every module gets its logger via ``get_logger(__name__)`` -- nothing outside
this file calls ``logging.basicConfig`` or touches handlers directly, so log
format/destinations stay consistent across the whole package.

A ``run_id`` (see :func:`new_run_id`) is attached to every log line emitted
during a pipeline run via :func:`run_logger`, so a run's full trace --
including partial failures on individual outcodes -- can be grepped out of
the log file by that id and cross-referenced against the ``run_configs`` /
``run_results`` rows it produced.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import uuid
from pathlib import Path

_CONFIGURED = False


def _log_dir() -> Path:
    return Path(os.environ.get("GEO_MODEL_LOG_DIR", "logs"))


def configure_logging(level: str | None = None) -> None:
    """Idempotently set up console + rotating-file handlers on the root
    ``geo_model`` logger. Safe to call multiple times (e.g. once from the
    CLI entrypoint, once from a test)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    log_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    log_dir = _log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(name)s] run_id=%(run_id)s %(message)s"
    )

    root = logging.getLogger("geo_model")
    root.setLevel(log_level)
    root.propagate = False

    # `_default_run_id_filter` has to run per-handler, not as a Logger-level
    # filter on `root`: child loggers (e.g. "geo_model.pipeline") reach
    # these handlers via propagation, which invokes each ancestor's
    # *handlers* directly and does NOT re-run ancestor Logger.filter() --
    # so a filter attached to `root` itself would silently never fire for
    # any child logger's records.
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.addFilter(_default_run_id_filter)
    root.addHandler(console)

    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "geo_model.log", maxBytes=10_000_000, backupCount=5
    )
    file_handler.setFormatter(fmt)
    file_handler.addFilter(_default_run_id_filter)
    root.addHandler(file_handler)

    _CONFIGURED = True


def _default_run_id_filter(record: logging.LogRecord) -> bool:
    if not hasattr(record, "run_id"):
        record.run_id = "-"
    return True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def run_logger(name: str, run_id: str) -> logging.LoggerAdapter:
    """Logger that stamps every record with the given run_id."""
    return logging.LoggerAdapter(get_logger(name), {"run_id": run_id})
