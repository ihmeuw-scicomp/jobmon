from importlib import import_module

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse
from starlette.status import HTTP_200_OK

from jobmon.server.web import routes
from jobmon.server.web.db.deps import get_db

version = "v3"
# Create a router for version 3 of the API
api_v3_router = APIRouter(tags=[version], prefix=f"/{version}")

# Create a separate health router without authentication
api_v3_health_router = APIRouter(tags=[f"{version}-health"], prefix=f"/{version}")


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
def get_pst_now(db: Session = Depends(get_db)) -> JSONResponse:
    """Get the current time in the Pacific."""
    return routes.get_pst_now(db)


@api_v3_health_router.get("/health")
def health(db: Session = Depends(get_db)) -> JSONResponse:
    """Test connectivity to the app. Always unauthenticated for health checks."""
    return routes.health(db)


@api_v3_router.get("/test_bad")
def test_route(db: Session = Depends(get_db)) -> None:
    """Test route."""
    return routes.test_route(db)


# Define an API version route
@api_v3_router.get("/api_version", status_code=HTTP_200_OK)
def api_version() -> JSONResponse:
    """Test connectivity to the database."""
    return JSONResponse(content={"status": version})
