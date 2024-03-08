from flask import Blueprint

api_v1_blueprint = Blueprint("reaper_v1", __name__)
api_v2_blueprint = Blueprint("reaper_v2", __name__)

from jobmon.server.web.routes.reaper import reaper
