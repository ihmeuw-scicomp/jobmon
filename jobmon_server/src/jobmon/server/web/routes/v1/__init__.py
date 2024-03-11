from importlib import import_module
from typing import Optional

from flask import Blueprint

from jobmon.server.web import routes
from jobmon.server.web.routes import SessionLocal

api_v1_blueprint = Blueprint("v1", __name__, url_prefix="/v1")

# Shared routes
api_v1_blueprint.add_url_rule("/", view_func=routes.is_alive, methods=["GET"])
api_v1_blueprint.add_url_rule("/time", view_func=routes.get_pst_now, methods=["GET"])
api_v1_blueprint.add_url_rule("/health", view_func=routes.health, methods=["GET"])
api_v1_blueprint.add_url_rule(
    "/test_bad", view_func=routes.test_route, methods=["GET"]  # type: ignore
)


@api_v1_blueprint.teardown_request
def teardown(e: Optional[BaseException]) -> None:
    """Remove threadlocal session from registry."""
    SessionLocal.remove()


import jobmon.server.web.routes.v1.fsm
