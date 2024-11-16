"""Routes used by task instances on worker nodes."""

from http import HTTPStatus as StatusCodes
import os
from typing import Any

from fastapi.responses import JSONResponse
from sqlalchemy import func, select, text
from structlog import get_logger

from jobmon.server.web.db_admin import get_session_local

logger = get_logger(__name__)

SessionLocal = get_session_local()


# ############################ SHARED LANDING ROUTES ##########################################
def is_alive() -> Any:
    """Action that sends a response to the requester indicating that responder is listening."""
    logger.info(f"{os.getpid()}: received is_alive?")
    rd = {"msg": "Yes, I am alive"}
    resp = JSONResponse(content=rd, status_code=StatusCodes.OK)
    return resp


def _get_time() -> str:
    with SessionLocal() as session:
        db_time = session.execute(select(func.now())).scalar()
        str_time = (
            db_time.strftime("%Y-%m-%d %H:%M:%S") if db_time else "0000-00-00 00:00:00"
        )
    return str_time


def get_pst_now() -> Any:
    """Get the time from the database."""
    time = _get_time()
    rd = {"time": time}
    resp = JSONResponse(content=rd, status_code=StatusCodes.OK)
    return resp


def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    _get_time()
    resp = JSONResponse(content={"status": "OK"}, status_code=StatusCodes.OK)
    return resp


# ############################ TESTING ROUTES ################################################
def test_route() -> None:
    """Test route to force a 500 error."""
    session = SessionLocal()
    with session.begin():
        session.execute(text("SELECT * FROM blip_bloop_table")).all()
        session.commit()
