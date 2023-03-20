"""Exceptions only used on the Server Side."""

from typing import Dict, Optional


# Use as base class for server side error
class ServerSideException(Exception):
    """Used for all exceptions on the server side."""

    def __init__(self, msg: str) -> None:
        """Initialize Exception with message."""
        self.msg = msg

    def __str__(self) -> str:
        """Return a printable representational string of the message."""
        return repr(self.msg)


class InvalidUsage(ServerSideException):
    """Error caused by client mistakes (ex. bad data provided)."""

    # TODO: make status_code a parameter for future extension. So far we only use 400
    def __init__(
        self,
        msg: str,
        status_code: Optional[int] = None,
        payload: Optional[tuple] = None,
    ) -> None:
        """Initialize ServerSide exception and default to 400 if not status_code available."""
        super().__init__(msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 400
            self.status_code = 400
        self.payload = payload

    def to_dict(self) -> Dict:
        """Put exception in a dictionary."""
        rv = dict(self.payload or ())
        rv["message"] = self.msg
        return rv


class ServerError(ServerSideException):
    """Use for Internal Server Error."""

    # TODO: make status_code a parameter for future extension. So far we only use 500
    def __init__(
        self,
        msg: str,
        status_code: Optional[int] = None,
        payload: Optional[tuple] = None,
    ) -> None:
        """Initialize and assign 500 status code if none provided."""
        super().__init__(msg)
        self.status_code = status_code
        if self.status_code is None:
            # by default, use 500
            self.status_code = 500
        self.payload = payload

    def to_dict(self) -> Dict:
        """Return payload and message as dictionary."""
        rv = dict(self.payload or ())
        rv["message"] = self.msg
        return rv
