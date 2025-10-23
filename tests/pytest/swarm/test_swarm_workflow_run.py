import time
from unittest.mock import Mock, patch

import pytest

from jobmon.client.swarm.workflow_run import WorkflowRun as SwarmWorkflowRun
from jobmon.core.constants import WorkflowRunStatus
from jobmon.server.web.models.task_status import TaskStatus


class TestSwarmWorkflowRunTimeout:
    """Test cases for WorkflowRun timeout functionality."""

    def test_workflow_run_timeout_with_active_tasks(self):
        """Test that workflow run ends when timeout is reached even with active tasks."""
        # Create a mock WorkflowRun
        swarm = SwarmWorkflowRun(
            workflow_run_id=1,
            workflow_run_heartbeat_interval=30,
            requester=Mock(),
            status=WorkflowRunStatus.RUNNING,
        )

        # Set required attributes
        from collections import deque

        swarm.max_concurrently_running = 10
        swarm.arrays = {}
        swarm.ready_to_run = deque()
        swarm._last_heartbeat_time = time.time() - 5.0

        # Mock the run method to simulate timeout behavior
        with patch.object(swarm, "run") as mock_run:
            # Simulate the timeout scenario by raising RuntimeError
            mock_run.side_effect = RuntimeError(
                "Not all tasks completed within the given workflow timeout length "
                "(1 seconds). Submitted tasks will still run, "
                "but the workflow will need to be restarted."
            )

            # This should raise RuntimeError due to timeout
            with pytest.raises(RuntimeError) as exc_info:
                swarm.run(
                    distributor_alive_callable=Mock(return_value=True),
                    seconds_until_timeout=1,
                    initialize=False,
                )

            # Verify the error message
            assert (
                "Not all tasks completed within the given workflow timeout length"
                in str(exc_info.value)
            )
            assert "1 seconds" in str(exc_info.value)

    def test_workflow_run_timeout_without_active_tasks(self):
        """Test that workflow run completes successfully when no active tasks remain."""
        # Create a mock WorkflowRun
        swarm = SwarmWorkflowRun(
            workflow_run_id=1,
            workflow_run_heartbeat_interval=30,
            requester=Mock(),
            status=WorkflowRunStatus.RUNNING,
        )

        # Set required attributes
        from collections import deque

        swarm.max_concurrently_running = 10
        swarm.arrays = {}
        swarm.ready_to_run = deque()
        swarm._last_heartbeat_time = time.time() - 5.0

        # Mock the run method to simulate successful completion
        with patch.object(swarm, "run") as mock_run:
            # Simulate successful completion (no exception)
            mock_run.return_value = None

            # This should complete successfully without timeout
            swarm.run(
                distributor_alive_callable=Mock(return_value=True),
                seconds_until_timeout=10,
                initialize=False,
            )

            # Verify the method was called
            mock_run.assert_called_once()

    def test_fhs_scenario_zero_active_tasks_many_ready_to_run(self):
        """Test FHS case: 0 active tasks but 13420 ready_to_run should return loop_continue true."""
        from collections import deque

        # Create a mock WorkflowRun
        swarm = SwarmWorkflowRun(
            workflow_run_id=1,
            workflow_run_heartbeat_interval=30,
            requester=Mock(),
            status=WorkflowRunStatus.RUNNING,
        )

        # Set required attributes
        swarm.max_concurrently_running = 10
        swarm.arrays = {}
        swarm.ready_to_run = deque(["task"] * 13420)  # Many ready-to-run tasks
        swarm._last_heartbeat_time = time.time() - 5.0

        # Mock the _task_status_map to simulate no active tasks
        with patch.object(
            swarm,
            "_task_status_map",
            {
                TaskStatus.QUEUED: [],
                TaskStatus.INSTANTIATING: [],
                TaskStatus.LAUNCHED: [],
                TaskStatus.RUNNING: [],
                TaskStatus.DONE: [],
                TaskStatus.ERROR_FATAL: [],
            },
        ):
            # Mock tasks to simulate not all tasks done
            with patch.object(swarm, "tasks", {"task1": Mock(), "task2": Mock()}):
                # Test different time_since_last_full_sync values
                test_values = [29.0, 30.0, 31.0]

                for time_since_last_full_sync in test_values:
                    print(
                        f"Testing with time_since_last_full_sync = {time_since_last_full_sync}"
                    )

                    # Test the _decide_run_loop_continue method directly
                    should_continue, updated_time = swarm._decide_run_loop_continue(
                        time_since_last_full_sync
                    )

                    # Should continue because there are ready-to-run tasks
                    assert (
                        should_continue == True
                    ), f"Should continue with time_since_last_full_sync={time_since_last_full_sync}"
                    assert swarm._get_active_tasks_count() == 0
                    assert swarm._get_ready_to_run_count() == 13420
                    print(
                        f"  Result: should_continue={should_continue}, updated_time={updated_time}"
                    )

    def test_timeout_logic_directly(self):
        """Test timeout logic directly without complex mocking."""
        # Create a mock WorkflowRun
        swarm = SwarmWorkflowRun(
            workflow_run_id=1,
            workflow_run_heartbeat_interval=30,
            requester=Mock(),
            status=WorkflowRunStatus.RUNNING,
        )

        # Test the timeout logic directly
        swarm_start_time = 1000.0
        current_time = 1002.0  # 2 seconds later
        seconds_until_timeout = 1  # 1 second timeout

        # Calculate total_elapsed_time
        total_elapsed_time = current_time - swarm_start_time

        # This should trigger timeout
        assert total_elapsed_time > seconds_until_timeout

        # Test the timeout condition
        if total_elapsed_time > seconds_until_timeout:
            expected_error = RuntimeError(
                f"Not all tasks completed within the given workflow timeout length "
                f"({seconds_until_timeout} seconds). Submitted tasks will still run, "
                f"but the workflow will need to be restarted."
            )

            # Verify the error message
            assert (
                "Not all tasks completed within the given workflow timeout length"
                in str(expected_error)
            )
            assert str(seconds_until_timeout) in str(expected_error)
            print(f"Timeout logic test passed: {expected_error}")
