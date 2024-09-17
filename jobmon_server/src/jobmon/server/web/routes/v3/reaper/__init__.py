from importlib import import_module

from fastapi import APIRouter

reaper_router = APIRouter(tags=["reaper"])

import_module("jobmon.server.web.routes.v3.reaper.reaper")
