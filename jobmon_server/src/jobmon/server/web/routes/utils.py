from typing import Any, List, Mapping

import structlog
from fastapi import HTTPException
from starlette.requests import Request
from typing_extensions import TypedDict

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError

_CONFIG = JobmonConfig()

logger = structlog.get_logger(__name__)


class User(TypedDict):
    sub: str
    email: str
    preferred_username: str
    name: str
    updated_at: int
    given_name: str
    family_name: str
    groups: List[str]
    nonce: str
    at_hash: str
    sid: str
    aud: str
    exp: int
    iat: int
    iss: str


def to_user_dict(data: Mapping[str, Any]) -> User:
    """to_user_dict.

    Converts dict to User TypedDict.
    """
    return User(
        sub=data["sub"],
        email=data["email"],
        preferred_username=data["preferred_username"],
        name=data["name"],
        updated_at=data["updated_at"],
        given_name=data["given_name"],
        family_name=data["family_name"],
        groups=data["groups"],
        nonce=data["nonce"],
        at_hash=data["at_hash"],
        sid=data["sid"],
        aud=data["aud"],
        exp=data["exp"],
        iat=data["iat"],
        iss=data["iss"],
    )


def get_user(request: Request) -> User:
    """get_user.

    A shared function to get the user from the session.
    Make it a method to mock in testing.
    """
    # for dev and production
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=403, detail="Unauthorized")
    # if 'exp' not in user or user['exp'] < int(datetime.now().timestamp()):
    #     raise HTTPException(status_code=403, detail="Session expired")
    return to_user_dict(user)


def get_request_username(request: Request) -> str:
    """get_request_username.

    Returns the username part of the email address from the request.
    """
    email = get_user(request)["email"]
    return email.split("@")[0]


def user_in_group(request: Request, group: str) -> bool:
    """user_in_group.

    Check is a user is a member of the specified group.
    """
    user = get_user(request)
    groups: List[str] = user["groups"]
    logger.info(f"{groups}")
    if group in groups:
        return True
    return False


def is_super_user(user: User) -> bool:
    """is_super_user.

    Checks if a user is a member of the superuser group defined in the
    OIDC__ADMIN_GROUP configuration option.
    """
    admin_group = _CONFIG.get("oidc", "admin_group")
    return admin_group in user["groups"]


def is_auth_enabled() -> bool:
    """Check if authentication is enabled."""
    config = JobmonConfig()
    try:
        return config.get_boolean("auth", "enabled")
    except ConfigError:
        return True  # Default to enabled for backwards compatibility


def create_anonymous_user() -> User:
    """Create an anonymous user for unauthenticated mode."""
    return User(
        sub="anonymous",
        email="anonymous@localhost",
        preferred_username="anonymous",
        name="Anonymous User",
        updated_at=0,
        given_name="Anonymous",
        family_name="User",
        groups=["anonymous"],
        nonce="",
        at_hash="",
        sid="",
        aud="",
        exp=0,
        iat=0,
        iss="localhost",
    )


def get_user_or_anonymous(request: Request) -> User:
    """Get user or return anonymous user when auth is disabled."""
    if not is_auth_enabled():
        return create_anonymous_user()

    return get_user(request)
