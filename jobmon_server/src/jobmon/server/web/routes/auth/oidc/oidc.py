import structlog
from authlib.integrations.base_client import OAuthError
from fastapi import Depends
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse
from jobmon.server.web.auth import oauth
from jobmon.server.web.routes.utils import get_user

from jobmon.server.web.routes.auth.oidc import oidc_router as api_auth_router

from jobmon.core.configuration import JobmonConfig

logger = structlog.get_logger(__name__)

_CONFIG = JobmonConfig()


@api_auth_router.get("/login")
async def login(request: Request):
    redirect_uri = _CONFIG.get("oidc", "redirect_uri")

    logger.debug(f"Redirect URL {redirect_uri}")
    logger.debug(request.session)
    logger.debug("login state: {state}")

    # If the user is here cleanup any old cookies to prevent any login issues
    request.session.pop("user", None)

    return await oauth.onelogin.authorize_redirect(request, redirect_uri)


@api_auth_router.get("/auth")
async def auth(request: Request):
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
        return HTMLResponse(f"<h1>{error.error}</h1>")
    user = token.get("userinfo")
    if user:
        # We must sort groups to prevent "mismatching_state: CSRF Warning! State not equal in request and response."
        if "groups" in user:
            # Remove groups starting with "app-" to prevent long lists of groups that break auth
            logger.debug(f"AUTH_DEBUG:groups:{user['groups']}")
            user["groups"] = [
                group for group in user["groups"] if not str(group).startswith("app-")
            ]
            logger.debug(f"AUTH_DEBUG:groups:{user['groups']}")
            user["groups"].sort()

        else:
            user["groups"] = []
        logger.debug(f"AUTH_DEBUG:groups_after_sort:{user['groups']}")
        request.session["user"] = dict(user)
        logger.debug(f"AUTH_DEBUG:request_session_user:{request.session['user']}")

    redirect_url = _CONFIG.get("oidc", "login_landing_page_uri")

    return RedirectResponse(redirect_url)


@api_auth_router.get("/userinfo")
async def userinfo(request: Request) -> dict:
    user = get_user(request)
    # logger.info(f"User info: {user}")
    # logger.info(f"User is admin: {is_super_user(user)}")
    # user["is_admin"] = is_super_user(user)
    return user


@api_auth_router.get("/logout")
async def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(url=_CONFIG.get("oidc", "login_landing_page_uri"))
