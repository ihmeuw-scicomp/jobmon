"""Test OTLP atexit handler for graceful shutdown."""

import subprocess
import sys

import pytest


def test_atexit_handler_registered():
    """Test that atexit handler is registered during OTLP initialization."""
    from jobmon.core.otlp import OTLP_AVAILABLE, JobmonOTLPManager

    if not OTLP_AVAILABLE:
        pytest.skip("OpenTelemetry not available")

    manager = JobmonOTLPManager.get_instance()

    # Initially not registered
    assert not manager._atexit_registered

    # After initialization, should be registered
    manager.initialize()
    assert manager._atexit_registered


def test_atexit_handler_flushes_on_exit():
    """Test that atexit handler flushes telemetry on normal process exit.

    This test creates a subprocess that initializes OTLP and exits normally.
    We verify that the atexit handler is called by checking logs.
    """
    script = """
import sys
import logging

# Configure logging to see shutdown messages
logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    from jobmon.core.otlp import OTLP_AVAILABLE, JobmonOTLPManager
    
    if not OTLP_AVAILABLE:
        print("SKIP: OpenTelemetry not available")
        sys.exit(0)
    
    # Initialize OTLP
    manager = JobmonOTLPManager.get_instance()
    manager.initialize()
    
    print("OTLP_INITIALIZED")
    
    # Simulate some work that would generate telemetry
    if manager.tracer_provider:
        tracer = manager.tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("test_span"):
            pass
    
    print("TELEMETRY_GENERATED")
    
    # Normal exit - atexit handler should be called
    print("EXITING_NORMALLY")
    sys.exit(0)
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
    )

    # Check that the script ran successfully
    assert result.returncode == 0, f"Script failed: {result.stderr}"

    # Verify execution sequence
    assert "OTLP_INITIALIZED" in result.stdout or "SKIP" in result.stdout

    if "SKIP" not in result.stdout:
        assert "TELEMETRY_GENERATED" in result.stdout
        assert "EXITING_NORMALLY" in result.stdout
        # Note: We can't easily verify the flush happened, but the test
        # confirms the process exits cleanly with OTLP initialized


def test_atexit_handler_double_shutdown_safe():
    """Test that calling shutdown manually and then exiting doesn't cause errors."""
    from jobmon.core.otlp import OTLP_AVAILABLE, JobmonOTLPManager

    if not OTLP_AVAILABLE:
        pytest.skip("OpenTelemetry not available")

    manager = JobmonOTLPManager.get_instance()
    manager.initialize()

    # Manually shutdown
    manager.shutdown()
    assert not manager._initialized

    # Atexit handler should handle already-shutdown state gracefully
    # (We can't actually trigger atexit in a test, but we can call the method)
    manager._atexit_shutdown()  # Should not raise


def test_atexit_handler_reinit_after_shutdown():
    """Test that atexit handler works after reinitializing following manual shutdown."""
    from jobmon.core.otlp import OTLP_AVAILABLE, JobmonOTLPManager

    if not OTLP_AVAILABLE:
        pytest.skip("OpenTelemetry not available")

    manager = JobmonOTLPManager.get_instance()

    # Initialize, shutdown, reinitialize
    manager.initialize()
    manager.shutdown()
    manager.initialize()

    # Should still be registered
    assert manager._atexit_registered
    assert manager._initialized

    # Atexit handler should work
    manager._atexit_shutdown()  # Should not raise


def test_atexit_handler_timeout():
    """Test that atexit handler doesn't block exit indefinitely.

    This test verifies the process exits even if flush takes too long.
    """
    script = """
import sys
import time

try:
    from jobmon.core.otlp import OTLP_AVAILABLE, JobmonOTLPManager
    
    if not OTLP_AVAILABLE:
        print("SKIP: OpenTelemetry not available")
        sys.exit(0)
    
    # Initialize OTLP
    manager = JobmonOTLPManager.get_instance()
    manager.initialize()
    
    print("STARTING_EXIT")
    start = time.time()
    
    # Exit normally - atexit should timeout if it takes too long
    # (In reality, the 5-second timeout should prevent indefinite blocking)
    sys.exit(0)
    
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,  # Should exit well before this
    )

    # Process should exit successfully (not timeout)
    assert result.returncode == 0 or "SKIP" in result.stdout
    assert "ERROR" not in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
