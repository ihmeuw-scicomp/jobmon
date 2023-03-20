from flask import Blueprint

blueprint = Blueprint("reaper", __name__)

from jobmon.server.web.routes.reaper import reaper
