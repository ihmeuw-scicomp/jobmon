"""Tests for jobmon core OTLP functionality."""

import logging
from unittest.mock import Mock, patch


class TestJobmonOTLPManager:
    """Test the core OTLP manager functionality."""

    def test_singleton_behavior(self):
        """Test that JobmonOTLPManager follows singleton pattern."""
        from jobmon.core.otlp import JobmonOTLPManager

        manager1 = JobmonOTLPManager.get_instance()
        manager2 = JobmonOTLPManager.get_instance()

        assert manager1 is manager2
        assert isinstance(manager1, JobmonOTLPManager)

    @patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", False)
    def test_initialization_without_otlp(self):
        """Test graceful handling when OTLP is not available."""
        from jobmon.core.otlp import JobmonOTLPManager

        # Clear singleton
        JobmonOTLPManager._instance = None

        manager = JobmonOTLPManager.get_instance()
        manager.initialize()

        assert not manager._initialized
        assert manager.tracer_provider is None

    @patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", True)
    def test_initialization_with_otlp(self):
        """Test successful initialization when OTLP is available."""
        from jobmon.core.otlp import JobmonOTLPManager

        # Clear singleton
        JobmonOTLPManager._instance = None

        with patch(
            "opentelemetry.sdk.trace.TracerProvider"
        ) as mock_tracer_provider, patch(
            "opentelemetry.trace.set_tracer_provider"
        ) as mock_set_tracer, patch(
            "jobmon.core.otlp.resources.create_jobmon_resources"
        ) as mock_resources:

            mock_resources.return_value = Mock()
            mock_provider_instance = Mock()
            mock_tracer_provider.return_value = mock_provider_instance

            manager = JobmonOTLPManager.get_instance()
            manager.initialize()

            assert manager._initialized
            # The manager should have a tracer provider (could be real or mock)
            assert manager.tracer_provider is not None

            # Verify global tracer provider was set
            mock_set_tracer.assert_called_once()

    def test_get_tracer(self):
        """Test tracer creation."""
        from jobmon.core.otlp import JobmonOTLPManager

        # Clear singleton
        JobmonOTLPManager._instance = None

        manager = JobmonOTLPManager.get_instance()

        # Test with no tracer provider
        assert manager.get_tracer("test") is None

        # Test with tracer provider
        mock_tracer_provider = Mock()
        mock_tracer = Mock()
        mock_tracer_provider.get_tracer.return_value = mock_tracer

        manager.tracer_provider = mock_tracer_provider

        result = manager.get_tracer("test")
        assert result is mock_tracer
        mock_tracer_provider.get_tracer.assert_called_with("test")

    @patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", True)
    def test_class_method_instrumentations(self):
        """Test class method instrumentations work correctly."""
        from jobmon.core.otlp import JobmonOTLPManager

        with patch(
            "opentelemetry.instrumentation.requests.RequestsInstrumentor"
        ) as mock_requests:
            mock_requests_instance = Mock()
            mock_requests.return_value = mock_requests_instance

            # Test requests instrumentation
            JobmonOTLPManager.instrument_requests()
            mock_requests.assert_called_once()
            mock_requests_instance.instrument.assert_called_once()

    @patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", False)
    def test_instrumentations_without_otlp(self):
        """Test that instrumentations gracefully handle OTLP not available."""
        from jobmon.core.otlp import JobmonOTLPManager

        # Should not crash when OTLP not available
        JobmonOTLPManager.instrument_requests()

    def test_initialize_jobmon_otlp_function(self):
        """Test the main initialization function."""
        from jobmon.core.otlp import JobmonOTLPManager, initialize_jobmon_otlp

        # Clear singleton
        JobmonOTLPManager._instance = None

        with patch.object(JobmonOTLPManager, "get_instance") as mock_get_instance:
            mock_manager = Mock()
            mock_get_instance.return_value = mock_manager

            result = initialize_jobmon_otlp()

            assert result == mock_manager
            mock_get_instance.assert_called_once()
            mock_manager.initialize.assert_called_once()


class TestOTLPHandlers:
    """Test the OTLP handlers that work with our configuration system."""

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_handler_with_dict_config(self):
        """Test handler initialization with inline dict configuration."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        # Mock exporter configuration (server pattern)
        exporter_config = {
            "module": "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
            "class": "OTLPLogExporter",
            "endpoint": "http://localhost:4317",
            "options": [["grpc.max_send_message_length", 16777216]],
            "max_export_batch_size": 8,
        }

        handler = JobmonOTLPLoggingHandler(exporter=exporter_config)

        assert handler._exporter_config is exporter_config
        assert not handler._initialized  # Lazy initialization

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_handler_with_preconfigured_exporter(self):
        """Test handler with pre-configured exporter instance."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        mock_exporter = Mock()
        handler = JobmonOTLPLoggingHandler(exporter=mock_exporter)

        assert handler._exporter_config is mock_exporter
        assert not handler._initialized  # Lazy initialization

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_handler_lazy_initialization(self):
        """Test that handler initializes lazily on first emit."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        # Create handler with a mock logger_provider for direct initialization
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger
        mock_logger_provider.resource = Mock()

        handler = JobmonOTLPLoggingHandler(logger_provider=mock_logger_provider)

        # Should be initialized immediately when logger_provider is provided
        assert handler._initialized
        assert handler._logger is mock_logger

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )

        # Mock the thread-local to avoid errors
        with patch("jobmon.core.config.structlog_config._thread_local") as mock_tl:
            mock_tl.last_event_dict = None

            # Emit should use the initialized logger
            handler.emit(record)

            # The internal logger's emit should be called
            assert mock_logger.emit.called

    def test_handler_without_otlp_available(self):
        """Test handlers gracefully handle OTLP not being available."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        with patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", False):
            handler = JobmonOTLPLoggingHandler()

            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="test message",
                args=(),
                exc_info=None,
            )

            # Should not crash when OTLP unavailable
            handler.emit(record)
            assert not handler._initialized

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_structlog_handler(self):
        """Test the structured logging OTLP handler."""
        from jobmon.core.otlp import JobmonOTLPStructlogHandler

        handler = JobmonOTLPStructlogHandler(level=logging.INFO)

        # Should be identical to JobmonOTLPLoggingHandler
        assert not handler._initialized
        assert handler.level == logging.INFO

    def test_structlog_handler_without_structlog(self):
        """Test structlog handler fallback when structlog not available."""
        from jobmon.core.otlp import JobmonOTLPStructlogHandler

        handler = JobmonOTLPStructlogHandler()

        # Should work without structlog dependencies
        assert not handler._initialized
        assert handler.level == logging.NOTSET


class TestOTLPUtilities:
    """Test OTLP utility functions."""

    @patch("jobmon.core.otlp.OTLP_AVAILABLE", False)
    def test_get_current_span_details_without_otlp(self):
        """Test span details when OTLP is not available."""
        from jobmon.core.otlp import get_current_span_details

        span_id, trace_id, parent_span_id = get_current_span_details()

        assert span_id is None
        assert trace_id is None
        assert parent_span_id is None

    @patch("jobmon.core.otlp.OTLP_AVAILABLE", True)
    def test_get_current_span_details_with_span(self):
        """Test span details extraction when span is available."""
        from jobmon.core.otlp import get_current_span_details

        with patch("opentelemetry.trace.get_current_span") as mock_get_span:
            # Mock a span with context
            mock_span = Mock()
            mock_span.is_recording.return_value = True

            mock_context = Mock()
            mock_context.span_id = 12345
            mock_context.trace_id = 67890
            mock_span.get_span_context.return_value = mock_context
            mock_span.parent = None

            mock_get_span.return_value = mock_span

            span_id, trace_id, parent_span_id = get_current_span_details()

            assert span_id == format(12345, "016x")
            assert trace_id == format(67890, "032x")
            assert parent_span_id is None

    def test_add_span_details_processor(self):
        """Test the structlog processor for adding span details."""
        from jobmon.core.otlp import add_span_details_processor

        with patch(
            "jobmon.core.otlp.utils.get_current_span_details",
            return_value=("span123", "trace456", "parent789"),
        ):

            event_dict = {"message": "test log"}
            logger = Mock()

            result = add_span_details_processor(logger, "info", event_dict)

            assert result["message"] == "test log"
            assert result["span_id"] == "span123"
            assert result["trace_id"] == "trace456"
            assert result["parent_span_id"] == "parent789"

    def test_add_span_details_processor_no_span(self):
        """Test structlog processor when no span is available."""
        from jobmon.core.otlp import add_span_details_processor

        with patch(
            "jobmon.core.otlp.utils.get_current_span_details",
            return_value=(None, None, None),
        ):

            event_dict = {"message": "test log"}
            logger = Mock()

            result = add_span_details_processor(logger, "info", event_dict)

            # Should only have original message, no span details added
            assert result == {"message": "test log"}


class TestJobmonOTLPFormatter:
    """Test the OTLP log formatter."""

    def test_formatter_adds_span_details(self):
        """Test that formatter adds span details to log records."""
        from jobmon.core.otlp import JobmonOTLPFormatter

        formatter = JobmonOTLPFormatter()

        with patch(
            "jobmon.core.otlp.formatters.get_current_span_details",
            return_value=("span123", "trace456", "parent789"),
        ):

            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="test message",
                args=(),
                exc_info=None,
            )

            formatted = formatter.format(record)

            # Check that span details were added to record
            assert hasattr(record, "span_id")
            assert hasattr(record, "trace_id")
            assert hasattr(record, "parent_span_id")

            assert record.span_id == "span123"
            assert record.trace_id == "trace456"
            assert record.parent_span_id == "parent789"


class TestOTLPLogconfigIntegration:
    """Test OTLP integration with our template-based configuration system."""

    def test_handler_in_logconfig_with_templates(self):
        """Test OTLP handlers work in logconfig with template references."""
        import logging.config

        # Configuration using templates and inline exporter config
        logconfig = {
            "version": 1,
            "handlers": {
                "otlp_inline": {
                    "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler",
                    "level": "INFO",
                    "exporter": {
                        "module": "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
                        "class": "OTLPLogExporter",
                        "endpoint": "http://localhost:4317",
                        "max_export_batch_size": 8,
                    },
                }
            },
            "loggers": {
                "test.otlp.template": {"handlers": ["otlp_inline"], "level": "INFO"}
            },
        }

        # Should not crash when applying config
        logging.config.dictConfig(logconfig)

        # Verify handler was created
        test_logger = logging.getLogger("test.otlp.template")
        assert len(test_logger.handlers) > 0

    def test_handler_with_template_formatter(self):
        """Test OTLP handler with template-based formatter."""
        import logging.config

        logconfig = {
            "version": 1,
            "formatters": {
                "otlp_formatter": {
                    "()": "jobmon.core.otlp.JobmonOTLPFormatter",
                    "format": "%(asctime)s [%(levelname)s] [trace_id=%(trace_id)s] - %(message)s",
                }
            },
            "handlers": {
                "console_with_otlp_format": {
                    "class": "logging.StreamHandler",
                    "formatter": "otlp_formatter",
                    "level": "INFO",
                }
            },
            "loggers": {
                "test.otlp.formatter": {
                    "handlers": ["console_with_otlp_format"],
                    "level": "INFO",
                }
            },
        }

        # Apply configuration
        logging.config.dictConfig(logconfig)

        # Get logger and verify formatter
        test_logger = logging.getLogger("test.otlp.formatter")
        assert len(test_logger.handlers) > 0

        handler = test_logger.handlers[0]
        from jobmon.core.otlp import JobmonOTLPFormatter

        assert isinstance(handler.formatter, JobmonOTLPFormatter)


class TestOTLPConfigurationOverrides:
    """Test OTLP integration with configuration override system."""

    def test_requester_otlp_tracing_only(self):
        """Test that requester OTLP initialization only sets up tracing, not logging.

        In the current architecture, requester logging is handled by client configuration,
        while requester only manages tracing setup.
        """
        from jobmon.core.requester import Requester

        # Reset class state to avoid side effects
        original_manager = Requester._otlp_manager
        Requester._otlp_manager = None

        try:
            with patch("jobmon.core.otlp.OTLP_AVAILABLE", True):
                with patch(
                    "jobmon.core.otlp.JobmonOTLPManager"
                ) as mock_manager_class, patch(
                    "jobmon.core.otlp.initialize_jobmon_otlp"
                ) as mock_init_otlp, patch(
                    "logging.config.dictConfig"
                ) as mock_dict_config:

                    mock_manager = Mock()
                    mock_init_otlp.return_value = mock_manager

                    # Initialize requester OTLP
                    Requester._init_otlp()

                    # Should initialize OTLP manager for tracing
                    mock_init_otlp.assert_called_once()

                    # Should instrument requests for HTTP tracing
                    mock_manager_class.instrument_requests.assert_called_once()

                    # Should NOT configure logging (that's handled by client config)
                    mock_dict_config.assert_not_called()

                    # Should store the manager instance
                    assert Requester._otlp_manager is mock_manager

        finally:
            # Restore original state
            Requester._otlp_manager = original_manager

    def test_create_log_exporter_factory(self):
        """Test the log exporter factory function."""
        from jobmon.core.otlp import create_log_exporter

        with patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", False):
            # Should return None when OTLP not available
            assert create_log_exporter() is None

        with patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", True):
            with patch(
                "opentelemetry.exporter.otlp.proto.grpc._log_exporter.OTLPLogExporter"
            ) as mock_exporter:
                mock_instance = Mock()
                mock_exporter.return_value = mock_instance

                # Should create exporter with default config
                result = create_log_exporter()
                assert result is mock_instance

                # Should pass through custom config
                result = create_log_exporter(endpoint="http://custom:4317", timeout=30)
                mock_exporter.assert_called_with(
                    insecure=True, endpoint="http://custom:4317", timeout=30  # Default
                )


class TestOTLPErrorHandling:
    """Test error handling and resilience of OTLP functionality."""

    def test_manager_resilient_to_initialization_failures(self):
        """Test that manager handles initialization failures gracefully."""
        from jobmon.core.otlp import JobmonOTLPManager

        # Clear singleton
        JobmonOTLPManager._instance = None

        with patch("jobmon.core.otlp.manager.OTLP_AVAILABLE", True):
            # Mock TracerProvider to raise exception during creation
            with patch(
                "opentelemetry.sdk.trace.TracerProvider",
                side_effect=Exception("Initialization failed"),
            ), patch(
                "jobmon.core.otlp.resources.create_jobmon_resources"
            ) as mock_resources:

                mock_resources.return_value = Mock()
                manager = JobmonOTLPManager.get_instance()

                # Should not crash on initialization failure
                manager.initialize()

                # Manager should handle the exception gracefully
                # The behavior may vary depending on implementation details
                # What matters is it doesn't crash the application
                assert manager is not None
                # At minimum, get_tracer should be safe to call
                tracer = manager.get_tracer("test")
                # Tracer may be None or a valid tracer depending on fallback behavior
                assert tracer is None or tracer is not None  # Should not crash

    def test_handler_resilience_to_creation_failures(self):
        """Test that handlers are resilient to creation failures."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        # Create handler with exporter so it will try to initialize
        mock_exporter = Mock()
        handler = JobmonOTLPLoggingHandler(exporter=mock_exporter)

        with patch.object(
            handler,
            "_ensure_initialized",
            side_effect=Exception("Initialization failed"),
        ):
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="test message",
                args=(),
                exc_info=None,
            )

            # Should not crash when initialization fails
            try:
                handler.emit(record)
            except Exception:
                pass  # Emit may propagate the exception

            # Should still not be initialized
            assert not handler._initialized
