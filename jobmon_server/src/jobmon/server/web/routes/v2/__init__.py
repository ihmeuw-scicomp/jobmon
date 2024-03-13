from http import HTTPStatus as StatusCodes
from importlib import import_module
from typing import Any, Optional

from flask import Blueprint, jsonify

from jobmon.server.web import routes
from jobmon.server.web.routes import SessionLocal

api_v2_blueprint = Blueprint("v2", __name__, url_prefix="/v2")

# Shared routes
api_v2_blueprint.add_url_rule("/", view_func=routes.is_alive, methods=["GET"])
api_v2_blueprint.add_url_rule("/time", view_func=routes.get_pst_now, methods=["GET"])
api_v2_blueprint.add_url_rule("/health", view_func=routes.health, methods=["GET"])
api_v2_blueprint.add_url_rule(
    "/test_bad", view_func=routes.test_route, methods=["GET"]  # type: ignore
)


@api_v2_blueprint.route("/api_version", methods=["GET"])
def api_version() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    resp = jsonify(status="v2")
    resp.status_code = StatusCodes.OK
    return resp


@api_v2_blueprint.teardown_request
def teardown(e: Optional[BaseException]) -> None:
    """Remove threadlocal session from registry."""
    SessionLocal.remove()


for module in ["cli", "fsm", "reaper"]:
    import_module(f"jobmon.server.web.routes.v2.{module}")
