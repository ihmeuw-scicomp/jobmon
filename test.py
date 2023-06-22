import asyncio
from time import time

from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance
from jobmon.core.configuration import JobmonConfig


class TestInstance(WorkerNodeTaskInstance):

    def __init__(self, command):
        self._command = command
        self.last_heartbeat_time = time()

        # config
        config = JobmonConfig()
        self._task_instance_heartbeat_interval = config.get_int(
            "heartbeat", "task_instance_interval"
        )
        self._heartbeat_report_by_buffer = config.get_float(
            "heartbeat", "report_by_buffer"
        )
        self._command_interrupt_timeout = config.get_int(
            "worker_node", "command_interrupt_timeout"
        )

    @property
    def stderr(self):
        return "/dev/null"

    @property
    def stdout(self):
        return "/dev/null"

    @property
    def status(self):
        return "R"

    @property
    def command_add_env(self):
        return {}

    def log_running(self):
        pass
        

ti = TestInstance(
    "/ihme/singularity-images/rstudio/shells/execRscript.sh -s "
    "/ihme/code/nfrqe/dismod_at_testing/diagnostics/prod/run-diagnostics.R "
    "/mnt/team/nfrqe/pub/dismod_at/simulation/v092/09_100b_drill_eta0p01pctc_test"
)

asyncio.run(ti._run_cmd())
