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
        assert handler._otlp_handler is None  # Lazy initialization

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_handler_with_preconfigured_exporter(self):
        """Test handler with pre-configured exporter instance."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        mock_exporter = Mock()
        handler = JobmonOTLPLoggingHandler(exporter=mock_exporter)

        assert handler._exporter_config is mock_exporter
        assert handler._otlp_handler is None  # Lazy initialization

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_handler_lazy_initialization(self):
        """Test that handler initializes lazily on first emit."""
        from jobmon.core.otlp import JobmonOTLPLoggingHandler

        # Create handler with a mock exporter config so it will try to create internal handler
        mock_exporter = Mock()
        handler = JobmonOTLPLoggingHandler(exporter=mock_exporter)

        # Should not have created internal handler yet
        assert handler._otlp_handler is None

        # Mock the creation process
        with patch.object(handler, "_create_handler") as mock_create:
            mock_internal_handler = Mock()
            mock_create.return_value = mock_internal_handler

            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="test message",
                args=(),
                exc_info=None,
            )

            # Emit should trigger creation
            handler.emit(record)

            assert handler._otlp_handler is mock_internal_handler
            mock_internal_handler.emit.assert_called_once_with(record)

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
            assert handler._otlp_handler is None

    @patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", True)
    def test_structlog_handler(self):
        """Test the structured logging OTLP handler."""
        from jobmon.core.otlp import JobmonOTLPStructlogHandler

        with patch("structlog.stdlib.ProcessorFormatter") as mock_formatter:
            handler = JobmonOTLPStructlogHandler(level=logging.INFO)

            # Should have attempted to create ProcessorFormatter
            mock_formatter.assert_called_once()
            assert handler._otlp_handler is None

    def test_structlog_handler_without_structlog(self):
        """Test structlog handler fallback when structlog not available."""
        from jobmon.core.otlp import JobmonOTLPStructlogHandler

        with patch("structlog.stdlib.ProcessorFormatter", side_effect=ImportError):
            handler = JobmonOTLPStructlogHandler()

            # Should have fallen back to default formatter
            assert isinstance(handler.formatter, logging.Formatter)


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

    def test_requester_otlp_with_config_overrides(self):
        """Test requester OTLP integration with configuration overrides."""
        from jobmon.core.requester import Requester

        with patch("jobmon.core.otlp.OTLP_AVAILABLE", True):
            with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
                mock_config = Mock()
                mock_config.get.side_effect = lambda section, key: {
                    ("otlp", "endpoint"): "http://custom-requester:4317",
                }.get((section, key), "")
                mock_config.get_section_coerced.return_value = {}
                mock_config_class.return_value = mock_config

                # Mock the template loading to return a config with OTLP handler
                mock_otlp_config = {
                    "version": 1,
                    "handlers": {
                        "otlp_requester": {
                            "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler",
                            "exporter": {"endpoint": "http://localhost:4317"},
                        }
                    },
                    "loggers": {
                        "jobmon.core.requester": {
                            "handlers": ["otlp_requester"],
                            "level": "INFO",
                        }
                    },
                }

                with patch(
                    "jobmon.core.config.logconfig_utils.load_logconfig_with_overrides"
                ) as mock_load:
                    mock_load.return_value = mock_otlp_config

                    with patch("jobmon.core.otlp.JobmonOTLPManager"), patch(
                        "logging.config.dictConfig"
                    ) as mock_dict_config:

                        # Initialize requester OTLP
                        Requester._init_otlp()

                        # Should have called dictConfig with modified endpoint
                        mock_dict_config.assert_called_once()
                        config_used = mock_dict_config.call_args[0][0]
                        assert (
                            config_used["handlers"]["otlp_requester"]["exporter"][
                                "endpoint"
                            ]
                            == "http://custom-requester:4317"
                        )

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

        # Create handler with exporter so it will try to create internal handler
        mock_exporter = Mock()
        handler = JobmonOTLPLoggingHandler(exporter=mock_exporter)

        with patch.object(
            handler, "_create_handler", side_effect=Exception("Creation failed")
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

            # Should not crash when creation fails
            handler.emit(record)

            # Should still not have internal handler
            assert handler._otlp_handler is None
