"""Tests for Jobmon telemetry context isolation."""

from __future__ import annotations

import structlog

from jobmon.core.config.structlog_config import create_telemetry_isolation_processor
from jobmon.core.logging.context import (
    bind_jobmon_context,
    clear_jobmon_context,
    get_jobmon_context,
    register_jobmon_metadata_keys,
    set_jobmon_context,
    unset_jobmon_context,
)


def setup_function() -> None:
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    clear_jobmon_context()


def get_test_processor():
    """Helper to get a processor for testing."""
    return create_telemetry_isolation_processor(["jobmon."])


def teardown_function() -> None:
    structlog.contextvars.clear_contextvars()
    clear_jobmon_context()


def test_jobmon_metadata_injected_only_for_jobmon_loggers() -> None:
    set_jobmon_context(
        workflow_run_id=123,
        task_instance_id=456,
    )

    processor = get_test_processor()
    jobmon_event = {"logger": "jobmon.client.workflow", "event": "started"}
    result = processor(None, "info", jobmon_event.copy())

    assert result["workflow_run_id"] == 123
    assert result["task_instance_id"] == 456

    fhs_event = {"logger": "fhs.pipeline.runner", "event": "started"}
    non_jobmon_result = processor(None, "info", fhs_event.copy())

    assert "workflow_run_id" not in non_jobmon_result
    assert "task_instance_id" not in non_jobmon_result

    unset_jobmon_context("workflow_run_id", "task_instance_id")
    cleared = processor(
        None,
        "info",
        {"logger": "jobmon.client.workflow", "event": "stopped"},
    )

    assert "workflow_run_id" not in cleared
    assert "task_instance_id" not in cleared


def test_bind_jobmon_context_manager_resets_metadata() -> None:
    assert get_jobmon_context() == {}

    with bind_jobmon_context(workflow_run_id=999, task_instance_id=111):
        current = get_jobmon_context()
        assert current["workflow_run_id"] == 999
        assert current["task_instance_id"] == 111

        processor = get_test_processor()
        event = processor(
            None,
            "info",
            {"logger": "jobmon.worker.runner", "event": "heartbeat"},
        )
        assert event["workflow_run_id"] == 999
        assert event["task_instance_id"] == 111

        other = processor(
            None,
            "info",
            {"logger": "fhs.pipeline.runner", "event": "heartbeat"},
        )
        assert "workflow_run_id" not in other
        assert "task_instance_id" not in other

    assert get_jobmon_context() == {}


def test_custom_telemetry_prefixes() -> None:
    """Test that custom telemetry prefixes work correctly."""
    custom_processor = create_telemetry_isolation_processor(
        ["myapp.telemetry", "special."]
    )

    # Bind some telemetry data
    set_jobmon_context(
        workflow_run_id=555,
        task_instance_id=777,
    )

    # Test that custom prefixes get telemetry
    telemetry_event = {"logger": "myapp.telemetry.worker", "event": "processing"}
    result = custom_processor(None, "info", telemetry_event.copy())
    assert result["workflow_run_id"] == 555
    assert result["task_instance_id"] == 777

    special_event = {"logger": "special.handler", "event": "handling"}
    result2 = custom_processor(None, "info", special_event.copy())
    assert result2["workflow_run_id"] == 555
    assert result2["task_instance_id"] == 777

    # Test that jobmon prefix doesn't get telemetry with custom config
    jobmon_event = {"logger": "jobmon.client.workflow", "event": "started"}
    result3 = custom_processor(None, "info", jobmon_event.copy())
    assert "workflow_run_id" not in result3
    assert "task_instance_id" not in result3

    unset_jobmon_context("workflow_run_id", "task_instance_id")


def test_bind_restores_previous_values() -> None:
    set_jobmon_context(workflow_run_id=42)

    with bind_jobmon_context(workflow_run_id=99):
        assert get_jobmon_context()["workflow_run_id"] == 99

    restored = get_jobmon_context()
    assert restored["workflow_run_id"] == 42

    unset_jobmon_context("workflow_run_id")


def test_register_jobmon_metadata_keys() -> None:
    register_jobmon_metadata_keys("jobmon_test_key")
    set_jobmon_context(jobmon_test_key="value-123")

    context = get_jobmon_context()
    assert context["jobmon_test_key"] == "value-123"

    unset_jobmon_context("jobmon_test_key")


def test_jobmon_as_library_with_fhs_style_config():
    """Test Jobmon working as a library with FHS-style structlog configuration."""
    import logging

    import structlog

    # Simulate FHS configuring structlog first
    def fhs_metadata_stamper(logger, method_name, event_dict):
        event_dict["fn"] = "test_function"
        event_dict["md"] = "test_module"
        event_dict["vs"] = "1.0.0"
        return event_dict

    def fhs_log_renderer(logger, method_name, event_dict):
        # FHS-style renderer: pops metadata and returns formatted string
        fn = event_dict.pop("fn")
        md = event_dict.pop("md")
        vs = event_dict.pop("vs")
        event = event_dict.pop("event", "")
        extras = f". {event_dict}" if event_dict else ""
        return f"[FHS] [{md}.{fn} : {vs}] - {event}{extras}"

    # FHS configures structlog
    structlog.configure(
        processors=[fhs_metadata_stamper, fhs_log_renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )

    # Verify FHS config is active (FHS renderer prints to stdout)
    import io
    from contextlib import redirect_stdout

    logger = structlog.get_logger("fhs.test")
    captured_output = io.StringIO()
    with redirect_stdout(captured_output):
        logger.info("FHS test message")
    output = captured_output.getvalue()
    assert "[FHS]" in output  # FHS renderer prints to stdout

    # Now Jobmon gets imported (simulating the prepend behavior)
    from jobmon.core.config.structlog_config import (
        prepend_jobmon_processors_to_existing_config,
    )

    prepend_jobmon_processors_to_existing_config()

    # Test that FHS logging still works
    captured_output2 = io.StringIO()
    with redirect_stdout(captured_output2):
        logger.info("FHS test after Jobmon")
    output2 = captured_output2.getvalue()
    assert "[FHS]" in output2

    # Test that Jobmon telemetry isolation works with FHS chain
    set_jobmon_context(workflow_run_id=123, task_instance_id=456)

    # FHS logger should not get telemetry metadata
    captured_output3 = io.StringIO()
    with redirect_stdout(captured_output3):
        fhs_logger = structlog.get_logger("fhs.test")
        fhs_logger.info("FHS message with telemetry")
    fhs_output = captured_output3.getvalue()
    assert "[FHS]" in fhs_output
    # Telemetry should be isolated - not in FHS output
    assert "workflow_run_id" not in fhs_output

    unset_jobmon_context("workflow_run_id", "task_instance_id")


def test_lazy_configuration_allows_host_to_configure_first():
    """Test that lazy configuration allows host to configure before Jobmon import."""
    import logging
    import sys

    import structlog

    # Remove jobmon.client from module cache if it was already imported
    modules_to_remove = [k for k in sys.modules.keys() if k.startswith("jobmon.client")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    # FHS configures structlog FIRST
    def fhs_renderer(logger, method_name, event_dict):
        return f"[FHS_LAZY] {event_dict.get('event', '')}"

    structlog.configure(
        processors=[fhs_renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )

    # NOW import Jobmon - it should NOT configure at import

    # Verify structlog still has only FHS renderer (Jobmon hasn't configured yet)
    config = structlog.get_config()
    processors = config.get("processors", [])

    # Should still have just FHS renderer - Jobmon hasn't configured yet!
    assert len(processors) == 1
    assert processors[0].__name__ == "fhs_renderer"

    # Manually trigger lazy configuration (simulates workflow.run())
    from jobmon.client.logging import ensure_structlog_configured

    ensure_structlog_configured()

    # NOW Jobmon processors should be prepended
    config_after = structlog.get_config()
    processors_after = config_after.get("processors", [])

    # Should have prepended processors + FHS renderer
    assert len(processors_after) > 1
    # Last processor should still be FHS renderer
    assert processors_after[-1].__name__ == "fhs_renderer"

    # Verify FHS renderer still works with prepended processors
    import io
    from contextlib import redirect_stdout

    logger = structlog.get_logger("test.lazy")
    captured = io.StringIO()
    with redirect_stdout(captured):
        logger.info("Test message")
    output = captured.getvalue()
    assert "[FHS_LAZY]" in output
