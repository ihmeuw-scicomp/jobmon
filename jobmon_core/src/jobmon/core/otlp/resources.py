"""OpenTelemetry resource detection for jobmon."""

from __future__ import annotations

import getpass
import os
import socket
import sys
from typing import Any, Dict, Optional

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
        detectors: list[ResourceDetector] = [
            ProcessResourceDetector(),
            HostResourceDetector(),
            JobmonServiceResourceDetector(),
        ]
        return resources.get_aggregated_resources(detectors)
    except ImportError:
        return None


class BaseJobmonResourceDetector(ResourceDetector):
    """Base class for jobmon resource detectors that handles common logic."""

    def detect(self) -> Optional[Any]:  # type: ignore[override]
        """Detect resource attributes using common jobmon logic."""
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

            # Get detector-specific attributes
            attrs = self._get_attributes(config, deployment_environment)
            return resources.Resource(attrs)  # type: ignore[arg-type]
        except ImportError:
            return None

    def _get_attributes(
        self, config: JobmonConfig, deployment_environment: str
    ) -> Dict[str, Any]:
        """Get detector-specific attributes. Must be implemented by subclasses."""
        raise NotImplementedError


class ProcessResourceDetector(BaseJobmonResourceDetector):
    """Detects process-related resource attributes."""

    def _get_attributes(
        self, config: JobmonConfig, deployment_environment: str
    ) -> Dict[str, Any]:
        """Get process-specific attributes."""
        return {
            str(resources.PROCESS_PID): int(os.getpid()),
            str(resources.PROCESS_RUNTIME_NAME): str(sys.implementation.name),
            str(resources.PROCESS_OWNER): str(getpass.getuser()),
            str(resources.DEPLOYMENT_ENVIRONMENT): deployment_environment,
        }


class JobmonServiceResourceDetector(BaseJobmonResourceDetector):
    """Detects jobmon service-related resource attributes."""

    def _get_attributes(
        self, config: JobmonConfig, deployment_environment: str
    ) -> Dict[str, Any]:
        """Get jobmon service-specific attributes."""
        return {
            resources.SERVICE_NAME: "jobmon",
            resources.SERVICE_VERSION: __version__,
            str(resources.DEPLOYMENT_ENVIRONMENT): deployment_environment,
        }


class HostResourceDetector(BaseJobmonResourceDetector):
    """Detects host-related resource attributes."""

    def _get_attributes(
        self, config: JobmonConfig, deployment_environment: str
    ) -> Dict[str, Any]:
        """Get host-specific attributes."""
        return {
            resources.HOST_NAME: socket.gethostname(),
            str(resources.DEPLOYMENT_ENVIRONMENT): deployment_environment,
        }
