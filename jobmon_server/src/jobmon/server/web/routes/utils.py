from datetime import datetime

from fastapi import HTTPException
from starlette.requests import Request
import structlog
from jobmon.core.configuration import JobmonConfig


_CONFIG = JobmonConfig()

logger = structlog.get_logger(__name__)


def get_user(request: Request):
    """A shared function to get the user from the session.
    Make it a method to mock in testing.
    """
    # for dev and production
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=403, detail="Unauthorized")
    # if 'exp' not in user or user['exp'] < int(datetime.now().timestamp()):
    #     raise HTTPException(status_code=403, detail="Session expired")
    return user


def user_in_group(request: Request, group):
    user = get_user(request)
    groups = user["groups"]
    logger.info(f"{groups}")
    if group in groups:
        return True
    return False


def is_super_user(user) -> bool:
    admin_group = _CONFIG.get("oidc", "admin_group")
    return admin_group in user["groups"]
