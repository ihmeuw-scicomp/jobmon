import logging
import sys
from typing import Optional

from authlib.integrations.starlette_client import OAuth
from starlette.config import Config

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError

log = logging.getLogger("authlib")
log.addHandler(logging.StreamHandler(sys.stderr))
log.setLevel(logging.DEBUG)

_CONFIG = JobmonConfig()

config = Config()
oauth = OAuth(config)

# Global variable to track if OAuth is setup
_oauth_setup = False


def is_auth_enabled() -> bool:
    """Check if authentication is enabled."""
    try:
        return _CONFIG.get_boolean("auth", "enabled")
    except ConfigError:
        return True  # Default to enabled for backwards compatibility


def setup_oauth() -> Optional[OAuth]:
    """Setup OAuth only if authentication is enabled."""
    global _oauth_setup

    if not is_auth_enabled():
        log.info("Authentication is disabled, skipping OAuth setup")
        return None

    if _oauth_setup:
        return oauth

    try:
        # https://docs.authlib.org/en/latest/client/frameworks.html
        # https://docs.authlib.org/en/latest/client/frameworks.html#using-oauth-2-0-to-log-in

        oauth.register(
            name=_CONFIG.get("oidc", "name"),
            server_metadata_url=_CONFIG.get("oidc", "conf_url"),
            client_id=_CONFIG.get("oidc", "client_id"),
            client_secret=_CONFIG.get("oidc", "client_secret"),
            token_endpoint_auth_method="client_secret_post",  # default: client_secret_basic
            client_kwargs={
                "scope": _CONFIG.get("oidc", "scope"),
            },
        )
        _oauth_setup = True
        log.info("OAuth setup completed successfully")
        return oauth

    except ConfigError as e:
        log.warning(f"OAuth setup failed due to missing configuration: {e}")
        return None


# Setup OAuth conditionally on import
oauth_instance = setup_oauth()
