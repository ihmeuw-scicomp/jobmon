"""Tests for :class:`DBResetRetryMiddleware`.

Uses a real Starlette app with a tunable endpoint that can fail once
before succeeding, so we exercise the raw-ASGI middleware exactly as
it runs in production without needing a real database.
"""

from __future__ import annotations

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from jobmon.server.web.middleware.db_reset_retry import (
    DBResetRetryMiddleware,
    is_connection_reset,
)


def _op_err(errno: int, msg: str = "boom") -> OperationalError:
    """Build a SQLAlchemy ``OperationalError`` with a MySQL-style errno."""

    class FakeDBAPIError(Exception):
        def __init__(self, errno: int, msg: str) -> None:
            self.args = (errno, msg)

    orig = FakeDBAPIError(errno, msg)
    return OperationalError(statement="SELECT 1", params={}, orig=orig)


# --- classifier ------------------------------------------------------------


def test_is_connection_reset_matches_tls_error() -> None:
    assert is_connection_reset(_op_err(2026, "TLS/SSL error: Connection reset"))


def test_is_connection_reset_matches_server_gone_away() -> None:
    assert is_connection_reset(_op_err(2006, "Server has gone away"))


def test_is_connection_reset_matches_lost_connection() -> None:
    assert is_connection_reset(_op_err(2013, "Lost connection to MySQL server"))


def test_is_connection_reset_skips_deadlock() -> None:
    assert not is_connection_reset(
        _op_err(1213, "Deadlock found when trying to get lock")
    )


def test_is_connection_reset_skips_lock_timeout() -> None:
    assert not is_connection_reset(_op_err(1205, "Lock wait timeout exceeded"))


def test_is_connection_reset_skips_non_operational_error() -> None:
    assert not is_connection_reset(ValueError("unrelated"))


def test_is_connection_reset_matches_message_fallback() -> None:
    # Some driver paths surface OperationalError without a numeric errno.
    class Orig(Exception):
        pass

    exc = OperationalError(
        statement="SELECT 1",
        params={},
        orig=Orig("broken pipe"),
    )
    assert is_connection_reset(exc)


# --- middleware behavior ---------------------------------------------------


def _make_app(
    exc_factory,
    *,
    fail_times: int = 1,
    raise_in_finalizer: bool = False,
) -> FastAPI:
    """Build a minimal app with the retry middleware around one handler.

    ``exc_factory`` returns a fresh exception for each failure. A counter
    tracks how many times the handler (or dep finalizer) has raised so
    we can simulate a transient failure.
    """
    app = FastAPI()
    app.add_middleware(DBResetRetryMiddleware)
    state = {"calls": 0, "finalizer_calls": 0}

    if raise_in_finalizer:

        def dep():
            yield None
            state["finalizer_calls"] += 1
            if state["finalizer_calls"] <= fail_times:
                raise exc_factory()

        @app.get("/probe")
        def probe(_dep: None = Depends(dep)) -> dict:
            state["calls"] += 1
            return {"ok": True, "calls": state["calls"]}

    else:

        @app.get("/probe")
        def probe() -> dict:
            state["calls"] += 1
            if state["calls"] <= fail_times:
                raise exc_factory()
            return {"ok": True, "calls": state["calls"]}

    app.state.counter = state
    return app


def test_middleware_retries_once_and_succeeds() -> None:
    app = _make_app(lambda: _op_err(2026, "TLS/SSL error: Connection reset"))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/probe")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "calls": 2}
    assert app.state.counter["calls"] == 2  # first attempt + one retry


def test_middleware_does_not_retry_deadlock() -> None:
    app = _make_app(lambda: _op_err(1213, "Deadlock found"))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/probe")
    # Handler raised once; no retry → 500 from FastAPI
    assert response.status_code == 500
    assert app.state.counter["calls"] == 1


def test_middleware_gives_up_after_max_attempts() -> None:
    app = _make_app(
        lambda: _op_err(2026, "TLS/SSL error: Connection reset"),
        fail_times=10,  # more than max_attempts=2
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/probe")
    assert response.status_code == 500
    assert app.state.counter["calls"] == 2  # exactly max_attempts=2 invocations


def test_middleware_catches_commit_phase_error_in_dep_finalizer() -> None:
    """The handler returns cleanly; the dep finalizer raises on commit.

    This is the exact shape of ``get_db`` when ``db.commit()`` hits a
    connection reset. Middleware should retry the whole request.
    """
    app = _make_app(
        lambda: _op_err(2026, "TLS/SSL error: Connection reset"),
        fail_times=1,
        raise_in_finalizer=True,
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/probe")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "calls": 2}
    assert app.state.counter["calls"] == 2
    assert app.state.counter["finalizer_calls"] == 2


def test_middleware_validates_max_attempts_positive() -> None:
    app = FastAPI()
    with pytest.raises(ValueError):
        app.add_middleware(DBResetRetryMiddleware, max_attempts=0)
        # Middleware stack builds lazily, so we must provoke it.
        TestClient(app).get("/")
