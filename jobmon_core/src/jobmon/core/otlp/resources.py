"""OpenTelemetry resource detection for jobmon."""

from __future__ import annotations

import getpass
import os
import socket
import sys
from typing import Any, Optional

from jobmon.core import __version__
from jobmon.core.configuration import JobmonConfig

from . import OTLP_AVAILABLE

if OTLP_AVAILABLE:
    from opentelemetry.sdk import resources
    from opentelemetry.sdk.resources import ResourceDetector


def create_jobmon_resources() -> Optional[Any]:
    """Create OpenTelemetry resources for jobmon."""
    if not OTLP_AVAILABLE:
        return None

    try:
        detectors = [
            ProcessResourceDetector(),
            HostResourceDetector(),
            JobmonServiceResourceDetector(),
        ]
        return resources.get_aggregated_resources(detectors)
    except ImportError:
        return None


class ProcessResourceDetector(ResourceDetector):
    """Detects process-related resource attributes."""

    def detect(self) -> Optional[Any]:  # type: ignore[override]
        """Detect process resource attributes."""
        if not OTLP_AVAILABLE:
            return None

        try:
            config = JobmonConfig()

            # Get deployment environment with fallback
            try:
                deployment_environment = config.get(
                    "telemetry", "deployment_environment"
                )
            except Exception:
                deployment_environment = "unknown"

            attrs = {
                str(resources.PROCESS_PID): int(os.getpid()),
                str(resources.PROCESS_RUNTIME_NAME): str(sys.implementation.name),
                str(resources.PROCESS_OWNER): str(getpass.getuser()),
                str(resources.DEPLOYMENT_ENVIRONMENT): deployment_environment,
            }
            return resources.Resource(attrs)  # type: ignore[arg-type]
        except ImportError:
            return None


class JobmonServiceResourceDetector(ResourceDetector):
    """Detects jobmon service-related resource attributes."""

    def detect(self) -> Optional[Any]:  # type: ignore[override]
        """Detect jobmon service resource attributes."""
        if not OTLP_AVAILABLE:
            return None

        try:
            config = JobmonConfig()

            # Get deployment environment with fallback
            try:
                deployment_environment = config.get(
                    "telemetry", "deployment_environment"
                )
            except Exception:
                deployment_environment = "unknown"

            attrs = {
                resources.SERVICE_NAME: "jobmon",
                resources.SERVICE_VERSION: __version__,
                str(resources.DEPLOYMENT_ENVIRONMENT): deployment_environment,
            }
            return resources.Resource(attrs)  # type: ignore[arg-type]
        except ImportError:
            return None


class HostResourceDetector(ResourceDetector):
    """Detects host-related resource attributes."""

    def detect(self) -> Optional[Any]:  # type: ignore[override]
        """Detect host resource attributes."""
        if not OTLP_AVAILABLE:
            return None

        try:
            config = JobmonConfig()

            # Get deployment environment with fallback
            try:
                deployment_environment = config.get(
                    "telemetry", "deployment_environment"
                )
            except Exception:
                deployment_environment = "unknown"

            attrs = {
                resources.HOST_NAME: socket.gethostname(),
                str(resources.DEPLOYMENT_ENVIRONMENT): deployment_environment,
            }
            return resources.Resource(attrs)  # type: ignore[arg-type]
        except ImportError:
            return None
