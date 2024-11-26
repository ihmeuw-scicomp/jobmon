from importlib import import_module

from fastapi import APIRouter

reaper_router = APIRouter(tags=["reaper"])

# import routes from v2 that did not change
import_module("jobmon.server.web.routes.v2.reaper.reaper")
