"""SQLite engine/session management -- the only place a SQLAlchemy engine
is constructed.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from geo_model.data.models import Base

_engine = None
_SessionLocal: sessionmaker | None = None


def init_db(db_path: Path) -> None:
    """Create the engine/session factory and any missing tables. Safe to
    call more than once (e.g. from tests with a fresh temp path)."""
    global _engine, _SessionLocal
    _engine = create_engine(f"sqlite:///{db_path}", future=True)
    _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    Base.metadata.create_all(_engine)


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
