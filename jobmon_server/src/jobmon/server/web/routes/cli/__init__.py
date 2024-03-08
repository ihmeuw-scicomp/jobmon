from flask import Blueprint

api_v1_blueprint = Blueprint("cli_v1", __name__)
api_v2_blueprint = Blueprint("cli_v2", __name__)

from jobmon.server.web.routes.cli import array, task, task_template, workflow
