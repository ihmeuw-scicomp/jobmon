from importlib import import_module

from fastapi import APIRouter
from starlette.responses import JSONResponse
from starlette.status import HTTP_200_OK

from jobmon.server.web import routes

version = "v2"
# Create a router for version 2 of the API
api_v2_router = APIRouter(tags=[version], prefix=f"/{version}")

for r in ["fsm", "cli", "reaper"]:
    mod = import_module(f"jobmon.server.web.routes.{version}.{r}")
    router = getattr(mod, f"{r}_router")
    api_v2_router.include_router(router)


# Shared routes
@api_v2_router.get("/")
def is_alive() -> JSONResponse:
    """Test connectivity to the database."""
    return routes.is_alive()


@api_v2_router.get("/time")
def get_pst_now() -> JSONResponse:
    """Get the current time in the Pacific."""
    return routes.get_pst_now()


@api_v2_router.get("/health")
def health() -> JSONResponse:
    """Test connectivity to the app."""
    return routes.health()


@api_v2_router.get("/test_bad")
def test_route() -> None:
    """Test route."""
    return routes.test_route()


# Define an API version route
@api_v2_router.get("/api_version", status_code=HTTP_200_OK)
def api_version() -> JSONResponse:
    """Test connectivity to the database."""
    return JSONResponse(content={"status": version})
