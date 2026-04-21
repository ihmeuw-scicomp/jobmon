"""ASGI middleware that retries a request once on transient DB connection resets.

Azure Private Link (and similar cloud NATs / load balancers) periodically
sever idle TCP connections. Those resets surface from SQLAlchemy as
``OperationalError`` with MySQL errno 2006 / 2013 / 2026.
``pool_pre_ping=True`` already validates pooled connections at checkout,
but it cannot help when a query is already in flight or when the commit
roundtrip that happens in ``jobmon.server.web.db.deps.get_db``'s
dependency finalizer hits the reset.

This middleware wraps the entire request pipeline — handler body AND
dependency finalizers — in a single retry boundary. A transient
connection-reset exception triggers one retry with a short backoff.
Other ``OperationalError`` variants (deadlocks 1213, lock timeouts 1205,
etc.) propagate immediately.

Implementation notes
--------------------
We intentionally implement this as raw ASGI middleware rather than
``starlette.middleware.base.BaseHTTPMiddleware``. ``BaseHTTPMiddleware``
wires ``call_next`` through anyio memory streams that are closed after a
single use, so it cannot re-invoke the downstream app. Raw ASGI lets us
buffer the request body once, replay it on each attempt, and stage the
outgoing send() messages so a failed attempt can be discarded cleanly
before any bytes reach the client.

Safety
------
``get_db`` defers ``session.commit()`` until after the handler returns
and rolls back on any in-handler exception before the session is closed.
That means a mid-handler reset guarantees no write was committed, so a
replay is safe. The rarer commit-phase race (MySQL committed but client
got RST before ACK) has identical semantics whether or not we retry;
jobmon's hash-based unique constraints on the important writes already
absorb the duplicate case.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, List, Tuple, Type

from sqlalchemy.exc import OperationalError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)


# MySQL error numbers indicating the connection was lost / reset mid-query.
#   2006 -> MySQL server has gone away
#   2013 -> Lost connection to MySQL server during query
#   2026 -> SSL/TLS connection error (includes "Connection reset by peer")
_CONNECTION_LOST_ERRNOS: frozenset[int] = frozenset({2006, 2013, 2026})

# Message-substring fallback for drivers that don't populate a numeric
# errno. Keep this tight to avoid masking real bugs.
_CONNECTION_LOST_SUBSTRINGS: Tuple[str, ...] = (
    "server has gone away",
    "lost connection",
    "tls/ssl error",
    "ssl connection has been closed",
    "connection reset by peer",
    "broken pipe",
)


def _build_retryable_exceptions() -> Tuple[Type[BaseException], ...]:
    """Return the tuple of exception classes we consider retry candidates."""
    exc_types: List[Type[BaseException]] = [OperationalError]
    try:
        import MySQLdb  # type: ignore[import-not-found]

        exc_types.append(MySQLdb.OperationalError)
    except ImportError:
        pass
    return tuple(exc_types)


_RETRYABLE_EXCEPTIONS: Tuple[Type[BaseException], ...] = _build_retryable_exceptions()


def _extract_errno(exc: BaseException) -> int | None:
    """Best-effort extraction of the driver-level MySQL error number."""
    for candidate in (getattr(exc, "orig", None), exc):
        if candidate is None:
            continue
        args = getattr(candidate, "args", None)
        if args and isinstance(args[0], int):
            return int(args[0])
    return None


def is_connection_reset(exc: BaseException) -> bool:
    """Return True iff ``exc`` represents a transient connection-loss error.

    Only errors clearly caused by a severed connection are retryable.
    Deadlocks (1213), lock timeouts (1205), integrity errors, etc. must
    propagate.
    """
    if not isinstance(exc, _RETRYABLE_EXCEPTIONS):
        return False

    errno = _extract_errno(exc)
    if errno is not None:
        return errno in _CONNECTION_LOST_ERRNOS

    msg = str(exc).lower()
    return any(needle in msg for needle in _CONNECTION_LOST_SUBSTRINGS)


class DBResetRetryMiddleware:
    """ASGI middleware that retries one HTTP request on a transient DB reset.

    Non-HTTP scopes (lifespan, websocket) are forwarded unchanged.
    """

    DEFAULT_MAX_ATTEMPTS = 2
    DEFAULT_BACKOFF_SECONDS = 0.2

    def __init__(
        self,
        app: ASGIApp,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    ) -> None:
        """Store retry policy; ``max_attempts`` must be >= 1."""
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.app = app
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Route HTTP through the retry loop; pass other scopes through."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        body, disconnected = await _buffer_request_body(receive)
        if disconnected:
            # Client disconnected before completing the request; propagate.
            await self.app(scope, _replay_receive(b"", disconnected=True), send)
            return

        for attempt in range(1, self._max_attempts + 1):
            staged: List[Message] = []

            async def staged_send(message: Message) -> None:
                staged.append(message)

            try:
                await self.app(scope, _replay_receive(body), staged_send)
            except (
                Exception
            ) as exc:  # noqa: BLE001 - intentionally broad, filtered below
                if not is_connection_reset(exc):
                    raise
                if attempt >= self._max_attempts:
                    logger.warning(
                        "DB reset retry exhausted after %d attempts on %s %s: %s",
                        attempt,
                        scope.get("method"),
                        scope.get("path"),
                        exc,
                    )
                    raise
                logger.warning(
                    "Retrying %s %s after transient DB error (%s): %s",
                    scope.get("method"),
                    scope.get("path"),
                    type(exc).__name__,
                    exc,
                )
                await asyncio.sleep(self._backoff_seconds)
                continue

            # Success: flush the staged response messages to the real send.
            for message in staged:
                await send(message)
            return


async def _buffer_request_body(receive: Receive) -> Tuple[bytes, bool]:
    """Consume the upstream receive() into a single byte buffer.

    Returns ``(body, disconnected)``. ``disconnected`` is True if the
    client sent ``http.disconnect`` before the body completed.
    """
    chunks: List[bytes] = []
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return b"".join(chunks), True
        if message["type"] == "http.request":
            chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                return b"".join(chunks), False
        # Any other message types are unexpected on http scope — drop.


def _replay_receive(
    body: bytes, *, disconnected: bool = False
) -> Callable[[], Awaitable[Message]]:
    """Build a fresh receive() that replays the buffered body once."""
    sent = False

    async def receive() -> Message:
        nonlocal sent
        if disconnected:
            return {"type": "http.disconnect"}
        if sent:
            # Downstream already consumed the body; mimic a hung client.
            await asyncio.Event().wait()
            raise RuntimeError("unreachable")  # pragma: no cover
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return receive
