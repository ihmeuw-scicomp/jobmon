from __future__ import annotations

import logging
import os
import psutil
from subprocess import PIPE, Popen, TimeoutExpired
import sys
from types import TracebackType
from typing import Optional

from jobmon.core.exceptions import (
    DistributorStartupTimeout,
)


class DistributorContext:
    def __init__(self, cluster_name: str, timeout: int, workflow_run_id: Optional[int] = None,
                 logger: Optional[logging.Logger] = None) -> None:
        """Initialization of the DistributorContext."""
        self._cluster_name = cluster_name
        self._workflow_run_id = workflow_run_id
        self._timeout = timeout
        if not logger:
            # Initialize a dummy do-nothing logger
            logger = logging.getLogger('dummy')
        self._logger = logger

    def __enter__(self) -> DistributorContext:
        """Starts the Distributor Process."""
        if self._logger:
            self._logger.info("Starting Distributor Process")

        # construct env
        env = os.environ.copy()
        entry_point = self.derive_jobmon_command_from_env()
        if entry_point is not None:
            env["JOBMON__DISTRIBUTOR__WORKER_NODE_ENTRY_POINT"] = f'"{entry_point}"'

        # Start the distributor. Write stderr to a file.
        cmd = [
            sys.executable,
            "-m",  # safest way to find the entrypoint
            "jobmon.distributor.cli",
            "start",
            "--cluster_name",
            self._cluster_name,
        ]

        if self._workflow_run_id is not None:
            cmd.extend([
                "--workflow_run_id",
                str(self._workflow_run_id),
            ])
        self.process = Popen(
            cmd,
            stderr=PIPE,
            universal_newlines=True,
            env=env,
        )

        # check if stderr contains "ALIVE"
        assert self.process.stderr is not None  # keep mypy happy on optional type
        stderr_val = self.process.stderr.read(5)
        if stderr_val != "ALIVE":
            err = self._shutdown()
            raise DistributorStartupTimeout(
                f"Distributor process did not start, stderr='{err} + {stderr_val}'"
            )
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        """Stops the Distributor Process."""
        if self._logger:
            self._logger.info("Stopping Distributor Process")
        err = self._shutdown()
        if self._logger:
            self._logger.info(f"Got {err} from Distributor Process")

    def alive(self) -> bool:
        self.process.poll()
        return self.process.returncode is None

    def _shutdown(self) -> str:
        self.process.terminate()
        try:
            _, err = self.process.communicate(timeout=self._timeout)
        except TimeoutExpired:
            err = ""

        if "SHUTDOWN" not in err:
            try:
                parent = psutil.Process(self.process.pid)
                for child in parent.children(recursive=True):
                    child.kill()
            except psutil.NoSuchProcess:
                pass
            self.process.kill()
            self.process.wait()

        return err

    @staticmethod
    def derive_jobmon_command_from_env() -> Optional[str]:
        """If a singularity path is provided, use it when running the worker node."""
        singularity_img_path = os.environ.get("IMGPATH", None)
        if singularity_img_path:
            return f"singularity run --app jobmon_command {singularity_img_path}"
        return None
