from authlib.integrations.base_client import OAuthError
from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse
import structlog

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.auth import oauth
from jobmon.server.web.routes.auth.oidc import oidc_router as api_auth_router
from jobmon.server.web.routes.utils import get_user, User

logger = structlog.get_logger(__name__)

_CONFIG = JobmonConfig()


@api_auth_router.get("/login")
async def login(request: Request) -> RedirectResponse:
    """login.

    Handles user login via OIDC.
    """
    redirect_uri = _CONFIG.get("oidc", "redirect_uri")

    logger.debug(f"Redirect URL {redirect_uri}")
    logger.debug(request.session)
    logger.debug("login state: {state}")

    # If the user is here cleanup any old cookies to prevent any login issues
    request.session.pop("user", None)

    return await oauth.onelogin.authorize_redirect(request, redirect_uri)


@api_auth_router.get("/auth")
async def auth(request: Request) -> RedirectResponse:
    """auth.

    Validates authorization data from OIDC identify provider.
    """
    # logger.debug(f"AUTH_DEBUG:oauth.onelogin:{oauth.onelogin}")
    # logger.debug(f"AUTH_DEBUG:request:{request}")
    # logger.debug(f"AUTH_DEBUG:request.query_params:{dict(request.query_params)}")
    # stored_state = request.session.get("oauth_state")
    # logger.debug(f"AUTH_DEBUG:in_token_logic:{stored_state}")
    # state = request.query_params.get("state")
    # logger.debug(f"AUTH_DEBUG:request_params:{request.query_params}")
    # logger.debug(f"AUTH_DEBUG:auth_state:{state}")

    try:
        token = await oauth.onelogin.authorize_access_token(request)
        logger.error(f"token: {token}")
    except OAuthError as error:
        logger.error(error)
        raise HTTPException(status_code=404, detail=error.error)
    user = token.get("userinfo")
    if user:
        # We must sort groups to prevent "mismatching_state: CSRF Warning!
        # State not equal in request and response."
        if "groups" in user:
            # Remove groups starting with "app-" to prevent long
            # lists of groups that break auth
            logger.debug(f"AUTH_DEBUG:groups:{user['groups']}")
            user["groups"] = [
                group for group in user["groups"] if not str(group).startswith("app-")
            ]
            logger.debug(f"AUTH_DEBUG:groups:{user['groups']}")
            user["groups"].sort()

        else:
            user["groups"] = []
        logger.debug(f"AUTH_DEBUG:groups_after_sort:{user['groups']}")
        print(dict(user))
        request.session["user"] = dict(user)
        logger.debug(f"AUTH_DEBUG:request_session_user:{request.session['user']}")

    redirect_url = _CONFIG.get("oidc", "login_landing_page_uri")

    return RedirectResponse(redirect_url)


@api_auth_router.get("/userinfo")
async def userinfo(request: Request) -> User:
    """userinfo.

    Returns the user's information from the user's session cookie.
    Used to check if the user is logged in.
    """
    user = get_user(request)
    return user


@api_auth_router.get("/logout")
async def logout(request: Request) -> RedirectResponse:
    """logout.

    Delete the user's session cookie.
    """
    request.session.pop("user", None)
    return RedirectResponse(url=_CONFIG.get("oidc", "login_landing_page_uri"))
