from typing import Union

from fastapi import HTTPException
from starlette.requests import Request
import structlog
from jobmon.core.configuration import JobmonConfig

_CONFIG = JobmonConfig()

logger = structlog.get_logger(__name__)


def get_user(request: Request) -> dict[str, Union[str, int, list[str]]]:
    """get_user
    A shared function to get the user from the session.
    Make it a method to mock in testing."""
    # for dev and production
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=403, detail="Unauthorized")
    # if 'exp' not in user or user['exp'] < int(datetime.now().timestamp()):
    #     raise HTTPException(status_code=403, detail="Session expired")
    return user


def user_in_group(request: Request, group: str) -> bool:
    """user_in_group

    Check is a user is a member of the specified group."""
    user = get_user(request)
    groups = user["groups"]
    logger.info(f"{groups}")
    if group in groups:
        return True
    return False


def is_super_user(user: dict[str, Union[str, int, list[str]]]) -> bool:
    """is_super_user

    Checks if a user is a member of the superuser group defined in the OIDC__ADMIN_GROUP configuration option"""
    admin_group = _CONFIG.get("oidc", "admin_group")
    return admin_group in user["groups"]
