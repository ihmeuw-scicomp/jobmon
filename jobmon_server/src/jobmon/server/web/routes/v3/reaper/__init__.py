from fastapi import APIRouter
from importlib import import_module

reaper_router = APIRouter(tags=["reaper"])

import_module("jobmon.server.web.routes.v3.reaper.reaper")
