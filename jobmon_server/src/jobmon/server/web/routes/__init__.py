"""Routes used by task instances on worker nodes."""
from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import current_app, jsonify
from sqlalchemy import func, select
from sqlalchemy import orm
from structlog import get_logger

from jobmon.server.web import session_factory

# scoped session associated with the current thread
SessionLocal = orm.scoped_session(session_factory)


logger = get_logger(__name__)


# ############################ SHARED LANDING ROUTES ##########################################
def is_alive() -> Any:
    """Action that sends a response to the requester indicating that responder is listening."""
    logger.info(f"{os.getpid()}: {current_app.__class__.__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


def _get_time() -> str:
    with SessionLocal() as session:
        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")
    return str_time


def get_pst_now() -> Any:
    """Get the time from the database."""
    time = _get_time()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    _get_time()
    resp = jsonify(status="OK")
    resp.status_code = StatusCodes.OK
    return resp


# ############################ TESTING ROUTES ################################################
def test_route() -> None:
    """Test route to force a 500 error."""
    session = SessionLocal()
    with session.begin():
        session.execute("SELECT * FROM blip_bloop_table").all()
        session.commit()
