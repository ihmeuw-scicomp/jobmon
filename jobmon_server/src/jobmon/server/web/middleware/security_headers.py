"""Middleware for security.

From: https://github.com/tiangolo/fastapi/issues/4420#issuecomment-1234146365
"""

from collections import OrderedDict
from typing import List, Union

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

swagger_bundle_shasum = "sha256-eV3QMumkWxytVHa/LDvu+mnW+PcSAEI4SfFu0iIlbDc="

CSP: dict[str, Union[str, List[str]]] = {
    "default-src": "'self'",
    "img-src": [
        "*",
        # For SWAGGER UI
        "data:",
    ],
    "connect-src": "'self'",
    "script-src": [
        "'self'",
        f"/api/docs_static/swagger-ui-bundle.js '{swagger_bundle_shasum}'",
    ],
    "style-src": [
        "'self'",
        "'unsafe-inline'",
        "/api/docs_static/swagger-ui.css",
    ],
}


def parse_policy(policy: Union[dict[str, Union[str, List[str]]], str]) -> str:
    """Parse a given policy dict to string."""
    if isinstance(policy, str):
        # parse the string into a policy dict
        policy_string = policy
        policy = OrderedDict()

        for policy_part in policy_string.split(";"):
            policy_parts = policy_part.strip().split(" ")
            policy[policy_parts[0]] = " ".join(policy_parts[1:])

    policies = []
    for section, content in policy.items():
        if not isinstance(content, str):
            content = " ".join(content)
        policy_part = f"{section} {content}"

        policies.append(policy_part)

    parsed_policy = "; ".join(policies)

    return parsed_policy


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""

    def __init__(self, app: FastAPI, csp: bool = True) -> None:
        """Init SecurityHeadersMiddleware.

        :param app: FastAPI instance
        :param no_csp: If no CSP should be used;
            defaults to :py:obj:`False`
        """
        super().__init__(app)
        self.csp = csp

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Dispatch of the middleware.

        :param request: Incoming request
        :param call_next: Function to process the request
        :return: Return response coming from processed request
        """
        headers = {
            "Content-Security-Policy": "" if not self.csp else parse_policy(CSP),
            "Cross-Origin-Opener-Policy": "same-origin",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Strict-Transport-Security": "max-age=31556926; includeSubDomains",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
        }
        response = await call_next(request)
        response.headers.update(headers)

        return response
