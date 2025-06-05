import os
import subprocess
import sys
import tempfile
from unittest.mock import patch

from jobmon.client.workflow import DistributorContext
from jobmon.client.workflow_run import WorkflowRunFactory
from jobmon.core.constants import WorkflowRunStatus
from jobmon.core.exceptions import DistributorStartupTimeout


class TestDistributorContext:
    """Tests for DistributorContext startup communication."""

    def test_distributor_context_basic(self, tool, task_template, client_env):
        """Test basic distributor context functionality (integration test)."""
        t1 = task_template.create_task(arg="echo 1", cluster_name="sequential")
        workflow = tool.create_workflow(name="test_distributor_context_basic")

        workflow.add_tasks([t1])
        workflow.bind()
        assert workflow.workflow_id is not None
        workflow._bind_tasks()
        assert t1.task_id is not None
        wfr = WorkflowRunFactory(workflow.workflow_id).create_workflow_run()
        wfr._update_status(WorkflowRunStatus.BOUND)

        # Use a reasonable timeout to avoid hanging the test suite
        distributor_context = DistributorContext("sequential", wfr.workflow_run_id, 15)

        # Test with context manager
        with distributor_context:
            assert distributor_context.alive()

        # After context exit, process should be terminated
        assert not distributor_context.alive()

    def test_startup_with_stderr_pollution(self):
        """Test that startup detection works when stderr is polluted with warnings."""
        # Create a test script that simulates a distributor with stderr pollution
        test_script = """#!/usr/bin/env python3
import sys
import time

# Simulate package warnings that would interfere with simple "ALIVE" detection
sys.stderr.write("DeprecationWarning: some_package is deprecated\\n")
sys.stderr.flush()
time.sleep(0.1)

sys.stderr.write("UserWarning: another warning message\\n") 
sys.stderr.flush()
time.sleep(0.1)

sys.stderr.write("INFO: Loading configuration\\n")
sys.stderr.flush()
time.sleep(0.1)

# Finally send the ALIVE signal
sys.stderr.write("ALIVE")
sys.stderr.flush()

# Keep the process running for a bit
time.sleep(2)

# Send shutdown signal
sys.stderr.write("SHUTDOWN")
sys.stderr.flush()
"""

        # Write test script to temporary file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(test_script)
            script_path = f.name

        try:
            os.chmod(script_path, 0o755)

            # Mock Popen to use our test script instead of the real distributor
            with patch("jobmon.client.workflow.Popen") as mock_popen:
                # Create the actual test process
                test_process = subprocess.Popen(
                    [sys.executable, script_path],
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                mock_popen.return_value = test_process

                # Create DistributorContext and test startup detection
                distributor_context = DistributorContext("sequential", 12345, 10)

                # Test startup detection with polluted stderr
                with distributor_context:
                    assert distributor_context.alive()
                    print(
                        "✅ Successfully started distributor despite stderr pollution"
                    )

                # The key test is that startup worked despite pollution - that's already proven above!
                # No need to check stderr content since the process is terminated by the context manager
                print(
                    "✅ Test passed: Startup detection is robust against stderr pollution"
                )

                # Clean up the test process if still running
                if test_process.poll() is None:
                    test_process.terminate()
                    try:
                        test_process.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        test_process.kill()

        finally:
            # Clean up
            os.unlink(script_path)

    def test_startup_timeout_with_no_signal(self):
        """Test that startup detection times out properly when no ALIVE signal is sent."""
        test_script = """#!/usr/bin/env python3
import sys
import time

# Send various output but never send ALIVE
sys.stderr.write("Starting up...\\n")
sys.stderr.flush()
time.sleep(0.5)

sys.stderr.write("Still starting...\\n") 
sys.stderr.flush()
time.sleep(0.5)

sys.stderr.write("Almost ready...\\n")
sys.stderr.flush()
time.sleep(2)  # This should cause timeout
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(test_script)
            script_path = f.name

        try:
            os.chmod(script_path, 0o755)

            # Mock Popen to use our test script instead of the real distributor
            with patch("jobmon.client.workflow.Popen") as mock_popen:
                # Create the actual test process
                test_process = subprocess.Popen(
                    [sys.executable, script_path],
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
                mock_popen.return_value = test_process

                # Create DistributorContext with short timeout
                distributor_context = DistributorContext("sequential", 12345, 2)

                # This should timeout since no ALIVE signal is sent
                try:
                    with distributor_context:
                        # Should not reach here due to timeout
                        assert False, "Should have timed out"
                except DistributorStartupTimeout:
                    print("✅ Successfully timed out when no ALIVE signal was sent")
                    # This is expected

                # Clean up the test process
                test_process.terminate()
                try:
                    test_process.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    test_process.kill()

        finally:
            os.unlink(script_path)
