"""Remote exit info from cluster plugins."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RemoteExitInfo:
    """Exit information returned by cluster plugins during triage.

    Attributes:
        error_state: The TaskInstanceStatus error code to transition to
            (e.g., UNKNOWN_ERROR, RESOURCE_ERROR, ERROR_FATAL).
        error_message: Human-readable error description for the error log.
        finalized: Whether the cluster's accounting system has finalized
            this job's metadata. If False, the distributor will defer
            the triage transition and retry on the next cycle, giving
            the accounting system time to produce accurate exit info
            (needed for correct resource scaling on OOM, etc.).
    """

    error_state: str
    error_message: str
    finalized: bool = True
