# jobmon/server/web/db/deps.py
"""FastAPI dependency injection for database sessions.

This module provides the database session dependency for route handlers.
Sessions are created from the sessionmaker stored in app.state by the
db_lifespan context manager.
"""
from __future__ import annotations

from typing import Generator

from fastapi import Depends, Request
from sqlalchemy.orm import Session


def get_db(request: Request) -> Generator[Session, None, None]:
    """Yield a SQLAlchemy Session for FastAPI dependency injection.

    The session is automatically committed on success, rolled back on
    exception, and closed when the request completes.

    Args:
        request: The FastAPI request object (provides access to app.state)

    Yields:
        Session: A SQLAlchemy session bound to the application's engine
    """
    SessionLocal = request.app.state.db_sessionmaker
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_dialect(request: Request) -> str:
    """Get the database dialect name from app state.

    Args:
        request: The FastAPI request object

    Returns:
        str: The dialect name (e.g., 'mysql', 'sqlite', 'postgresql')
    """
    return request.app.state.db_dialect


# Dependency aliases for cleaner route handler signatures
DB = Depends(get_db)
Dialect = Depends(get_dialect)
