import argparse
import shlex
from typing import Any, List, Optional


class CLI:
    """Base CLI."""

    def __init__(self) -> None:
        """Initialize the CLI."""
        self.parser = argparse.ArgumentParser("jobmon CLI")

    def main(self, argstr: Optional[str] = None) -> Any:
        """Parse args."""
        args = self.parse_args(argstr)
        return args.func(args)

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
