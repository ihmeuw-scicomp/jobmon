from importlib import import_module

from fastapi import APIRouter

# Create a router for version 3 of the API
api_auth_router = APIRouter(tags=["auth"], prefix="/auth")

for r in ["oidc"]:
    mod = import_module(f"jobmon.server.web.routes.auth.{r}")
    router = getattr(mod, f"{r}_router")
    api_auth_router.include_router(router)
