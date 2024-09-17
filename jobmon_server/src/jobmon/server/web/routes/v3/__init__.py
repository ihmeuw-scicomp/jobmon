from fastapi import APIRouter
from starlette.responses import JSONResponse
from starlette.status import HTTP_200_OK

from jobmon.server.web import routes

# Create a router for version 3 of the API
api_v3_router = APIRouter(tags=["v3"])


# Shared routes
@api_v3_router.get("/")
def is_alive() -> JSONResponse:
    """Test connectivity to the database."""
    return routes.is_alive()


@api_v3_router.get("/time")
def get_pst_now() -> JSONResponse:
    """Get the current time in the Pacific."""
    return routes.get_pst_now()


@api_v3_router.get("/health")
def health() -> JSONResponse:
    """Test connectivity to the app."""
    return routes.health()


@api_v3_router.get("/test_bad")
def test_route() -> None:
    """Test route."""
    return routes.test_route()


# Define an API version route
@api_v3_router.get("/api_version", status_code=HTTP_200_OK)
def api_version() -> JSONResponse:
    """Test connectivity to the database."""
    return JSONResponse(content={"status": "v3"})
