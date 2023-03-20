"""Routes used to move through the finite state."""
from typing import Optional

from flask import Blueprint

from jobmon.server.web import routes
from jobmon.server.web.routes import SessionLocal

blueprint = Blueprint("finite_state_machine", __name__)
blueprint.add_url_rule("/", view_func=routes.is_alive, methods=["GET"])
blueprint.add_url_rule("/time", view_func=routes.get_pst_now, methods=["GET"])
blueprint.add_url_rule("/health", view_func=routes.health, methods=["GET"])
blueprint.add_url_rule(
    "/test_bad", view_func=routes.test_route, methods=["GET"]  # type: ignore
)

from jobmon.server.web.routes.fsm import (
    array,
    cluster,
    dag,
    node,
    queue,
    task,
    task_instance,
    task_resources,
    task_template,
    tool,
    tool_version,
    workflow,
    workflow_run,
)


@blueprint.teardown_request
def teardown(e: Optional[BaseException]) -> None:
    """Remove threadlocal session from registry."""
    SessionLocal.remove()
