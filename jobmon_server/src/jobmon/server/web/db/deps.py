# jobmon/server/web/db/deps.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from jobmon.server.web.db.engine import get_sessionmaker


@contextmanager
def _session_scope() -> Generator[Session, None, None]:
    """Context-manager that commits/rolls back and closes the Session."""
    db: Session = get_sessionmaker()()
    try:
        yield db
        db.commit()
    except Exception:  # pragma: no cover  – let caller handle/log
        db.rollback()
        raise
    finally:
        db.close()


# ── FastAPI dependency (sync-friendly) ──────────────────────────────────────
def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy Session managed inside a context-manager."""
    with _session_scope() as db:
        yield db


# Handy alias so route handlers can declare  `db: Session = DB`
DB: Callable[[Session], Session] = Depends(get_db)
