# jobmon/server/db/deps.py
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable

from fastapi import Depends
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from jobmon.server.web.db.engine import get_sessionmaker


def _open_session() -> Session:
    """Create a new (synchronous) SQLAlchemy Session object."""
    return get_sessionmaker()()


# ---  async dependency  ------------------------------------------------------
@asynccontextmanager  # ➟ produces a true async context‑manager
async def get_db() -> AsyncGenerator[Session, None]:
    """Yield a SQLAlchemy Session from a thread-pool.

    This ensures that the FastAPI event-loop never blocks on DB work.
    """
    db: Session = await run_in_threadpool(_open_session)
    try:
        # If you need explicit BEGIN / COMMIT wrapping, do it here,
        # again via run_in_threadpool to keep it non‑blocking.
        yield db
        await run_in_threadpool(db.commit)  # optional
    except Exception:
        await run_in_threadpool(db.rollback)  # optional
        raise
    finally:
        await run_in_threadpool(db.close)  # always close


# Handy alias so route handlers can write `db: Session = DB`
DB: Callable[[Session], Session] = Depends(get_db)
