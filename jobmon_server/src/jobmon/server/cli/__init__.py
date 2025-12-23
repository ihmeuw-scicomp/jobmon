"""Jobmon Server CLI - Click-based command line interface.

This module provides the CLI for server administration commands.
Use 'jobmon-server --help' for available commands.
"""

from jobmon.server.cli.main import cli, main

__all__ = ["cli", "main"]
