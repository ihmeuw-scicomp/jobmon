from flask import Blueprint

blueprint = Blueprint("cli", __name__)

from jobmon.server.web.routes.cli import array, task, task_template, workflow
