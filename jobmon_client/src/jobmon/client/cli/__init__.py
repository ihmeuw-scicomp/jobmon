"""Jobmon CLI v2 - Click-based command line interface.

This module provides the new hierarchical CLI structure using Click.
The old argparse-based CLI is preserved in cli_v1.py for reference.

For backward compatibility, the legacy ClientCLI class is re-exported
from cli_v1.py to maintain compatibility with code that imports it directly.
"""

from jobmon.client.cli.main import cli, main

# Import the real ClientCLI from cli_v1 for backward compatibility
# This class inherits from jobmon.core.cli.CLI and has the expected interface
# (component_name, parse_args, configure_component_logging, etc.)
from jobmon.client.cli_v1 import ClientCLI

__all__ = ["cli", "main", "ClientCLI"]
