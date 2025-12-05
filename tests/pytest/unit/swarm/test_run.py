"""Tests for the swarm run module.

These tests verify the event loop detection and thread-based execution
that allows workflow.run() to work from Jupyter notebooks and other
async contexts.
"""

import asyncio
import threading

import pytest

from jobmon.client.swarm.run import _is_event_loop_running, _run_async_in_thread


class TestEventLoopDetection:
    """Tests for _is_event_loop_running()."""

    def test_no_running_event_loop(self):
        """Should return False when no event loop is running."""
        assert not _is_event_loop_running()

    def test_with_running_event_loop(self):
        """Should return True when called from within a running event loop."""

        async def check_loop_running():
            return _is_event_loop_running()

        result = asyncio.run(check_loop_running())
        assert result is True

    def test_after_event_loop_closes(self):
        """Should return False after event loop has closed."""
        # Run and close an event loop
        asyncio.run(asyncio.sleep(0))

        # Now check - should be False
        assert not _is_event_loop_running()


class TestRunAsyncInThread:
    """Tests for _run_async_in_thread()."""

    def test_runs_in_different_thread(self):
        """Should execute the async function in a different thread."""
        main_thread_id = threading.current_thread().ident

        async def get_thread_id():
            return threading.current_thread().ident

        result_thread_id = _run_async_in_thread(get_thread_id)
        assert result_thread_id != main_thread_id

    def test_returns_coroutine_result(self):
        """Should return the result from the async function."""

        async def return_value():
            return 42

        result = _run_async_in_thread(return_value)
        assert result == 42

    def test_passes_args_and_kwargs(self):
        """Should correctly pass arguments to the async function."""

        async def add_values(a, b, multiplier=1):
            return (a + b) * multiplier

        result = _run_async_in_thread(add_values, 2, 3, multiplier=10)
        assert result == 50

    def test_propagates_exceptions(self):
        """Should propagate exceptions from the async function."""

        async def raise_error():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            _run_async_in_thread(raise_error)

    def test_works_from_async_context(self):
        """Should work when called from within an async context.

        This is the key scenario - when a user calls workflow.run()
        from a Jupyter notebook which already has a running event loop.
        """

        async def outer_async():
            # Verify we're in an async context
            assert _is_event_loop_running()

            # Now run an async function via the thread helper
            async def inner_async():
                return "success"

            return _run_async_in_thread(inner_async)

        result = asyncio.run(outer_async())
        assert result == "success"

    def test_handles_async_operations_in_function(self):
        """Should handle async operations within the called function."""

        async def async_with_await():
            await asyncio.sleep(0.01)
            return "completed"

        result = _run_async_in_thread(async_with_await)
        assert result == "completed"


class TestNestedEventLoopScenario:
    """Integration tests simulating Jupyter notebook scenarios."""

    def test_simulate_jupyter_notebook_scenario(self):
        """Simulate the scenario where a user calls workflow code from Jupyter.

        In Jupyter, there's always a running event loop. This test verifies
        that our helper functions correctly handle this scenario.
        """

        async def simulate_jupyter_cell():
            # In Jupyter, an event loop is always running
            assert _is_event_loop_running()

            # User wants to run some async workflow code
            async def workflow_like_function():
                # Simulate some async work
                await asyncio.sleep(0.01)
                return {"status": "done", "count": 5}

            # This should work without "cannot be called from running event loop" error
            result = _run_async_in_thread(workflow_like_function)
            return result

        # Run the simulated Jupyter cell
        result = asyncio.run(simulate_jupyter_cell())
        assert result == {"status": "done", "count": 5}

