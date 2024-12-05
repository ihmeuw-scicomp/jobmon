import logging
import sys

from authlib.integrations.starlette_client import OAuth
from starlette.config import Config

from jobmon.core.configuration import JobmonConfig

log = logging.getLogger("authlib")
log.addHandler(logging.StreamHandler(sys.stderr))
log.setLevel(logging.DEBUG)

_CONFIG = JobmonConfig()

config = Config()
oauth = OAuth(config)

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
