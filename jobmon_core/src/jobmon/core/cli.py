import argparse
import shlex
from typing import Any, List, Optional


class CLI:
    """Base CLI with automatic component logging support."""

    def __init__(self, component_name: Optional[str] = None) -> None:
        """Initialize the CLI with optional component logging.

        Args:
            component_name: Component name for automatic logging configuration
                          ('distributor', 'worker', 'server', or None to disable)
        """
        self.parser = argparse.ArgumentParser("jobmon CLI")
        self.component_name = component_name

    def main(self, argstr: Optional[str] = None) -> Any:
        """Parse args and configure component logging before execution."""
        args = self.parse_args(argstr)

        # Configure component logging if enabled
        if self.component_name:
            self.configure_component_logging()

        return args.func(args)

    def configure_component_logging(self) -> None:
        """Configure logging for this component using existing infrastructure.

        This method can be called directly when needed (e.g., by plugins that
        bypass the normal main() flow).
        """
        if not self.component_name:
            return

        try:
            from jobmon.core.config.logconfig_utils import configure_component_logging

            configure_component_logging(self.component_name)
        except Exception:
            # Fail silently - component starts with no logging
            # This ensures components always start successfully
            pass

    def parse_args(self, argstr: Optional[str] = None) -> argparse.Namespace:
        """Construct a parser, parse either sys.argv (default) or the provided argstr.

        Returns a Namespace. The Namespace should have a 'func' attribute which can be used to
        dispatch to the appropriate downstream function.
        """
        arglist: Optional[List[str]] = None
        if argstr is not None:
            arglist = shlex.split(argstr)
        args = self.parser.parse_args(arglist)
        return args
