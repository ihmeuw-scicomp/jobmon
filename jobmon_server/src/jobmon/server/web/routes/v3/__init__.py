from importlib import import_module

from fastapi import APIRouter
from starlette.responses import JSONResponse, Response
from starlette.status import HTTP_200_OK

from jobmon.server.web import routes

version = "v3"
# Create a router for version 3 of the API
api_v3_router = APIRouter(tags=[version], prefix=f"/{version}")

# Create a separate health router without authentication
api_v3_health_router = APIRouter(tags=[f"{version}-health"], prefix=f"/{version}")


# Add explicit CORS preflight handler
@api_v3_router.options("/{full_path:path}")
async def options_handler(full_path: str) -> Response:
    """Handle CORS preflight requests for all v3 endpoints."""
    response = Response()
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response


for r in ["fsm", "cli", "reaper"]:
    mod = import_module(f"jobmon.server.web.routes.{version}.{r}")
    router = getattr(mod, f"{r}_router")
    api_v3_router.include_router(router)


# Shared routes
@api_v3_router.get("/")
def is_alive() -> JSONResponse:
    """Test connectivity to the database."""
    return routes.is_alive()


@api_v3_router.get("/time")
def get_pst_now() -> JSONResponse:
    """Get the current time in the Pacific."""
    return routes.get_pst_now()


@api_v3_health_router.get("/health")
def health() -> JSONResponse:
    """Test connectivity to the app. Always unauthenticated for health checks."""
    return routes.health()


@api_v3_router.get("/test_bad")
def test_route() -> None:
    """Test route."""
    return routes.test_route()


# Define an API version route
@api_v3_router.get("/api_version", status_code=HTTP_200_OK)
def api_version() -> JSONResponse:
    """Test connectivity to the database."""
    return JSONResponse(content={"status": version})
