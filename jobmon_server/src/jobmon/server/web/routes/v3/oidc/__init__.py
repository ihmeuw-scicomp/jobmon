"""Routes used to move through the finite state."""

from importlib import import_module

from fastapi import APIRouter

oidc_router = APIRouter(tags=["oidc"])

import_module("jobmon.server.web.routes.v3.oidc.oidc")