from importlib.metadata import version

__version__ = version("jobmon_client")

# Jobmon uses LAZY structlog configuration to ensure host applications
# always have the opportunity to configure structlog first.
#
# Configuration happens on first use (e.g., in workflow.run() or workflow.bind())
# rather than at import time, eliminating import-order dependencies.
#
# See: ensure_structlog_configured() in jobmon.client.logging
