"""SQLite engine/session management -- the only place a SQLAlchemy engine
is constructed.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from geo_model.data.models import Base

_engine = None
_SessionLocal: sessionmaker | None = None

# Columns added to existing tables after their first release. create_all()
# only creates missing TABLES, not missing COLUMNS on ones that already
# exist -- there's no formal migration tool in this project, so a bare
# ALTER TABLE ADD COLUMN (SQLite supports this directly) is run for any of
# these not already present. Safe to re-run: each column is added at most
# once.
_COLUMN_MIGRATIONS: list[tuple[str, str, str]] = [
    # (table, column, DDL type)
    ("outcodes", "borough", "VARCHAR(128)"),
    ("outcodes", "region", "VARCHAR(64)"),
    ("outcodes", "geo_group", "VARCHAR(32)"),
]


def _run_column_migrations(engine) -> None:
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table, column, ddl_type in _COLUMN_MIGRATIONS:
            if table not in inspector.get_table_names():
                continue  # create_all() will make it with the column already
            existing = {c["name"] for c in inspector.get_columns(table)}
            if column in existing:
                continue
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}"))


def init_db(db_path: Path) -> None:
    """Create the engine/session factory and any missing tables. Safe to
    call more than once (e.g. from tests with a fresh temp path)."""
    global _engine, _SessionLocal
    _engine = create_engine(f"sqlite:///{db_path}", future=True)
    _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    Base.metadata.create_all(_engine)
    _run_column_migrations(_engine)


@contextmanager
def get_session() -> Iterator[Session]:
    if _SessionLocal is None:
        raise RuntimeError("init_db() must be called before get_session()")
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
