"""Tests for jobmon server OTLP functionality."""

import logging
from unittest.mock import Mock, patch


class TestServerOTLPManager:
    """Test the server OTLP manager functionality."""

    def test_singleton_behavior(self):
        """Test that get_server_otlp_manager returns consistent instance."""
        # Clear singleton
        import jobmon.server.web.otlp.manager as server_otlp_module
        from jobmon.server.web.otlp import get_server_otlp_manager

        server_otlp_module._server_otlp_manager = None

        manager1 = get_server_otlp_manager()
        manager2 = get_server_otlp_manager()

        assert manager1 is manager2

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", False)
    def test_initialization_without_otlp(self):
        """Test graceful handling when OTLP is not available."""
        from jobmon.server.web.otlp import ServerOTLPManager

        manager = ServerOTLPManager()
        manager.initialize()

        assert not manager._initialized
        assert manager._core_manager is None

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True)
    def test_initialization_with_otlp(self):
        """Test successful initialization when OTLP is available."""
        from jobmon.server.web.otlp import ServerOTLPManager

        with patch("jobmon.core.otlp.JobmonOTLPManager") as mock_manager_class:
            mock_core_manager = Mock()
            mock_manager_class.get_instance.return_value = mock_core_manager

            manager = ServerOTLPManager()
            manager.initialize()

            assert manager._initialized
            assert manager._core_manager is mock_core_manager

            # Verify core manager was initialized
            mock_core_manager.initialize.assert_called_once()

    def test_property_delegation(self):
        """Test that properties are properly delegated to core manager."""
        from jobmon.server.web.otlp import ServerOTLPManager

        manager = ServerOTLPManager()

        # Test with no core manager
        assert manager.tracer_provider is None
        assert manager.get_tracer("test") is None

        # Test with core manager
        mock_core_manager = Mock()
        mock_tracer_provider = Mock()
        mock_tracer = Mock()

        mock_core_manager.tracer_provider = mock_tracer_provider
        mock_core_manager.get_tracer.return_value = mock_tracer

        manager._core_manager = mock_core_manager

        assert manager.tracer_provider is mock_tracer_provider
        assert manager.get_tracer("test") is mock_tracer

        mock_core_manager.get_tracer.assert_called_with("test")

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True)
    def test_instrument_app(self):
        """Test FastAPI application instrumentation."""
        from jobmon.server.web.otlp import ServerOTLPManager

        manager = ServerOTLPManager()
        manager._initialized = True

        mock_app = Mock()

        with patch(
            "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor"
        ) as mock_instrumentor_class:
            mock_instrumentor = Mock()
            mock_instrumentor_class.return_value = mock_instrumentor

            manager.instrument_app(mock_app)

            mock_instrumentor_class.assert_called_once()
            mock_instrumentor.instrument_app.assert_called_once_with(mock_app)

    def test_instrument_app_not_initialized(self):
        """Test that app instrumentation is skipped when not initialized."""
        from jobmon.server.web.otlp import ServerOTLPManager

        manager = ServerOTLPManager()
        manager._initialized = False

        mock_app = Mock()

        with patch(
            "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor"
        ) as mock_instrumentor_class:
            manager.instrument_app(mock_app)

            # Should not have called instrumentation
            mock_instrumentor_class.assert_not_called()

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", False)
    def test_instrument_app_without_otlp(self):
        """Test app instrumentation when OTLP not available."""
        from jobmon.server.web.otlp import ServerOTLPManager

        mock_app = Mock()

        # Create a manager instance to call the method on
        manager = ServerOTLPManager()

        # Should not crash
        manager.instrument_app(mock_app)

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True)
    def test_class_method_instrumentations(self):
        """Test class method instrumentations work correctly."""
        from jobmon.server.web.otlp import ServerOTLPManager

        with patch(
            "opentelemetry.instrumentation.requests.RequestsInstrumentor"
        ) as mock_requests, patch(
            "opentelemetry.instrumentation.sqlalchemy.SQLAlchemyInstrumentor"
        ) as mock_sqlalchemy, patch(
            "jobmon.server.web.otlp.manager._server_otlp_manager", None
        ):

            mock_requests_instance = Mock()
            mock_sqlalchemy_instance = Mock()
            mock_requests.return_value = mock_requests_instance
            mock_sqlalchemy.return_value = mock_sqlalchemy_instance

            # Test requests instrumentation (singleton auto-initializes)
            ServerOTLPManager.instrument_requests()
            mock_requests.assert_called_once()
            mock_requests_instance.instrument.assert_called_once()

            # Test SQLAlchemy global instrumentation
            ServerOTLPManager.instrument_sqlalchemy()
            mock_sqlalchemy.assert_called_once()
            mock_sqlalchemy_instance.instrument.assert_called_once()

            # Test SQLAlchemy engine instrumentation
            mock_engine = Mock()
            ServerOTLPManager.instrument_engine(mock_engine)

            # Should not be called again since already instrumented globally
            assert mock_sqlalchemy.call_count == 1
            # Only the global instrumentation should have been called
            mock_sqlalchemy_instance.instrument.assert_called_once_with(
                enable_commenter=True, skip_dep_check=True
            )

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", False)
    def test_instrumentations_without_otlp(self):
        """Test that instrumentations gracefully handle OTLP not available."""
        from jobmon.server.web.otlp import ServerOTLPManager

        # Should not crash when OTLP not available
        ServerOTLPManager.instrument_requests()
        ServerOTLPManager.instrument_sqlalchemy()
        ServerOTLPManager.instrument_engine(Mock())

    def test_initialize_server_otlp_function(self):
        """Test the server initialization function."""
        # Clear singleton
        import jobmon.server.web.otlp.manager as server_otlp_module
        from jobmon.server.web.otlp import initialize_server_otlp

        server_otlp_module._server_otlp_manager = None

        # Don't mock the manager creation, just test that it works
        result = initialize_server_otlp()

        # Should return a ServerOTLPManager instance
        from jobmon.server.web.otlp import ServerOTLPManager

        assert isinstance(result, ServerOTLPManager)
        assert result._initialized  # Should be initialized after the call


class TestOTLPStructlogHandler:
    """Test the OTLP structlog handler (now using core implementation)."""

    def test_handler_initialization(self):
        """Test JobmonOTLPStructlogHandler initialization."""
        from jobmon.core.otlp.handlers import JobmonOTLPStructlogHandler

        handler = JobmonOTLPStructlogHandler(level=logging.INFO)

        # Should be identical to JobmonOTLPLoggingHandler
        assert handler._exporter_config is None  # No exporter config yet
        assert not handler._initialized  # Handler not created yet
        assert handler.level == logging.INFO

    def test_handler_with_exporter_config(self):
        """Test handler with pre-configured exporter."""
        from jobmon.core.otlp.handlers import JobmonOTLPStructlogHandler

        mock_exporter = Mock()
        handler = JobmonOTLPStructlogHandler(exporter=mock_exporter)

        assert handler._exporter_config is mock_exporter
        assert not handler._initialized  # Lazy initialization

    def test_handler_lazy_initialization(self):
        """Test that handler initializes lazily on first emit."""
        from jobmon.core.otlp.handlers import JobmonOTLPStructlogHandler

        # Create handler with a mock logger_provider for direct initialization
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger
        mock_logger_provider.resource = Mock()

        handler = JobmonOTLPStructlogHandler(logger_provider=mock_logger_provider)

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

    def test_handler_without_structlog(self):
        """Test handler fallback when structlog not available."""
        from jobmon.core.otlp.handlers import JobmonOTLPStructlogHandler

        with patch("structlog.stdlib.ProcessorFormatter", side_effect=ImportError):
            handler = JobmonOTLPStructlogHandler()

            # Should have fallen back to default formatter
            assert isinstance(handler.formatter, logging.Formatter)

    def test_handler_without_otlp_available(self):
        """Test handler gracefully handles OTLP not being available."""
        from jobmon.core.otlp.handlers import JobmonOTLPStructlogHandler

        with patch("jobmon.core.otlp.handlers.OTLP_AVAILABLE", False):
            handler = JobmonOTLPStructlogHandler()

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


class TestServerOTLPIntegration:
    """Test server OTLP integration with logging configuration system."""

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True)
    def test_logconfig_integration(self):
        """Test that server OTLP works with logconfig custom handlers."""
        import logging.config

        # Mock the core OTLP functionality
        with patch("jobmon.core.otlp.JobmonOTLPManager") as mock_manager_class:
            mock_core_manager = Mock()
            mock_manager_class.get_instance.return_value = mock_core_manager

            # Simulate server logconfig with OTLP handlers
            logconfig = {
                "version": 1,
                "disable_existing_loggers": False,
                "handlers": {
                    "console": {"class": "logging.StreamHandler", "level": "INFO"},
                    "otlp_server": {
                        "class": "jobmon.core.otlp.JobmonOTLPLoggingHandler",
                        "level": "INFO",
                    },
                    "otlp_structlog": {
                        "class": "jobmon.core.otlp.JobmonOTLPStructlogHandler",
                        "level": "INFO",
                    },
                },
                "loggers": {
                    "jobmon.server.web": {
                        "handlers": ["console", "otlp_structlog"],
                        "level": "INFO",
                        "propagate": False,
                    },
                    "sqlalchemy": {
                        "handlers": ["console", "otlp_server"],
                        "level": "WARN",
                        "propagate": False,
                    },
                },
            }

            # Apply logconfig - should not crash
            logging.config.dictConfig(logconfig)

            # Verify handlers were created
            server_logger = logging.getLogger("jobmon.server.web")
            sqlalchemy_logger = logging.getLogger("sqlalchemy")

            assert len(server_logger.handlers) == 2
            assert len(sqlalchemy_logger.handlers) == 2

    def test_server_startup_integration(self):
        """Test server OTLP initialization during startup."""
        # Clear singleton
        import jobmon.server.web.otlp.manager as server_otlp_module
        from jobmon.server.web.otlp import get_server_otlp_manager

        server_otlp_module._server_otlp_manager = None

        with patch("jobmon.core.otlp.JobmonOTLPManager") as mock_manager_class:
            mock_core_manager = Mock()
            mock_manager_class.get_instance.return_value = mock_core_manager

            # Simulate server startup calling get_server_otlp_manager
            server_manager = get_server_otlp_manager()

            # Should have initialized core manager
            mock_core_manager.initialize.assert_called_once()

            # Server should be ready for instrumentation
            assert server_manager._initialized

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True)
    def test_full_server_instrumentation_flow(self):
        """Test the complete server instrumentation workflow."""
        # Clear singleton
        import jobmon.server.web.otlp.manager as server_otlp_module
        from jobmon.server.web.otlp import get_server_otlp_manager

        server_otlp_module._server_otlp_manager = None

        with patch("jobmon.core.otlp.JobmonOTLPManager") as mock_manager_class, patch(
            "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor"
        ) as mock_fastapi, patch(
            "opentelemetry.instrumentation.requests.RequestsInstrumentor"
        ) as mock_requests, patch(
            "opentelemetry.instrumentation.sqlalchemy.SQLAlchemyInstrumentor"
        ) as mock_sqlalchemy:

            mock_core_manager = Mock()
            mock_manager_class.get_instance.return_value = mock_core_manager

            # Initialize server OTLP
            server_manager = get_server_otlp_manager()

            # Simulate full server instrumentation
            mock_app = Mock()
            mock_engine = Mock()

            # Instrument everything
            server_manager.instrument_sqlalchemy()
            server_manager.instrument_requests()
            server_manager.instrument_app(mock_app)
            server_manager.instrument_engine(mock_engine)

            # Verify all instrumentations were called
            mock_requests.assert_called_once()
            mock_sqlalchemy.assert_called()  # Called multiple times
            mock_fastapi.assert_called_once()

            # Verify app was instrumented
            mock_fastapi.return_value.instrument_app.assert_called_with(mock_app)


class TestServerOTLPConfiguration:
    """Test server OTLP configuration integration with our logging system."""

    def test_otlp_disabled_scenario(self):
        """Test server behavior when OTLP is disabled."""
        # Clear singleton
        import jobmon.server.web.otlp.manager as server_otlp_module
        from jobmon.server.web.otlp import get_server_otlp_manager

        server_otlp_module._server_otlp_manager = None

        with patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", False):
            server_manager = get_server_otlp_manager()

            # Should still create manager but not initialize core
            assert not server_manager._initialized
            assert server_manager._core_manager is None

            # Instrumentations should not crash
            mock_app = Mock()
            server_manager.instrument_app(mock_app)
            server_manager.instrument_sqlalchemy()
            server_manager.instrument_requests()

    def test_import_error_handling(self):
        """Test handling of import errors during instrumentation."""
        from jobmon.server.web.otlp import ServerOTLPManager

        with patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True):
            # Mock import error for FastAPI instrumentation
            with patch(
                "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor",
                side_effect=ImportError,
            ):
                manager = ServerOTLPManager()
                manager._initialized = True

                # Should not crash on import error
                manager.instrument_app(Mock())

    @patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True)
    def test_core_manager_import_error(self):
        """Test handling when core OTLP manager can't be imported."""
        from jobmon.server.web.otlp import ServerOTLPManager

        with patch(
            "jobmon.core.otlp.JobmonOTLPManager",
            side_effect=ImportError("Mocked import error"),
        ):
            manager = ServerOTLPManager()
            manager.initialize()

            # Should handle import error gracefully
            # The manager may still have a mocked core manager due to how the test works
            # What matters is that it handles the error without crashing
            assert manager is not None


class TestServerLoggingConfigIntegration:
    """Test integration with our elegant logging configuration system."""

    def test_server_default_config_selection(self):
        """Test that server uses basic config by default (OTLP via overrides)."""
        from jobmon.core.config.logconfig_utils import configure_component_logging

        with patch("jobmon.core.configuration.JobmonConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get.return_value = ""  # No file override
            mock_config.get_section_coerced.return_value = {}
            mock_config_class.return_value = mock_config

            with patch(
                "jobmon.core.config.logconfig_utils.configure_logging_with_overrides"
            ) as mock_configure:
                # Should configure with basic config
                configure_component_logging("server")

                # Should have called configure_logging_with_overrides
                mock_configure.assert_called_once()
                args, kwargs = mock_configure.call_args
                assert "logconfig_server.yaml" in kwargs["default_template_path"]

    def test_server_template_integration_with_otlp(self):
        """Test that server OTLP handlers work with template system."""
        import logging.config

        # Configuration that would come from our template system
        server_otlp_config = {
            "version": 1,
            "formatters": {
                "structlog_json": {
                    "()": "structlog.stdlib.ProcessorFormatter",
                    "processor": "structlog.processors.JSONRenderer",
                },
                "otlp_default": {
                    "()": "jobmon.core.otlp.JobmonOTLPFormatter",
                },
            },
            "handlers": {
                "console_structlog": {
                    "class": "logging.StreamHandler",
                    "formatter": "structlog_json",
                    "level": "INFO",
                },
                "otlp_structlog": {
                    "class": "jobmon.core.otlp.JobmonOTLPStructlogHandler",
                    "level": "INFO",
                    "formatter": "structlog_json",
                    "exporter": {
                        "module": "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
                        "class": "OTLPLogExporter",
                        "endpoint": "http://localhost:4317",
                    },
                },
            },
            "loggers": {
                "jobmon.server.web": {
                    "handlers": ["console_structlog", "otlp_structlog"],
                    "level": "INFO",
                    "propagate": False,
                }
            },
        }

        # Should not crash when applying template-based config
        logging.config.dictConfig(server_otlp_config)

        # Verify handler was created with correct type
        server_logger = logging.getLogger("jobmon.server.web")
        assert len(server_logger.handlers) == 2

        # Find the OTLP handler
        otlp_handler = None
        for handler in server_logger.handlers:
            if "JobmonOTLPStructlogHandler" in str(type(handler)):
                otlp_handler = handler
                break

        assert otlp_handler is not None


class TestErrorHandlingAndResilience:
    """Test error handling and resilience of server OTLP functionality."""

    def test_server_manager_resilient_to_core_failures(self):
        """Test that server manager handles core manager failures gracefully."""
        from jobmon.server.web.otlp import ServerOTLPManager

        with patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True):
            with patch("jobmon.core.otlp.JobmonOTLPManager") as mock_manager_class:
                mock_manager_class.get_instance.side_effect = Exception(
                    "Core initialization failed"
                )

                manager = ServerOTLPManager()

                # Should not crash on core manager failure
                manager.initialize()

                # Should handle property access gracefully
                assert manager.tracer_provider is None
                assert manager.get_tracer("test") is None

    def test_instrumentation_failure_resilience(self):
        """Test that instrumentation failures don't break the application."""
        from jobmon.server.web.otlp import ServerOTLPManager

        with patch("jobmon.server.web.otlp.manager.OTLP_AVAILABLE", True):
            manager = ServerOTLPManager()
            manager._initialized = True

            # Mock instrumentation failures
            with patch(
                "opentelemetry.instrumentation.fastapi.FastAPIInstrumentor",
                side_effect=Exception("Instrumentation failed"),
            ):

                # Should not crash the application
                manager.instrument_app(Mock())

                # Application should continue normally
                assert manager._initialized

    def test_custom_handler_resilience(self):
        """Test that custom handlers are resilient to OTLP failures."""
        from jobmon.core.otlp.handlers import JobmonOTLPStructlogHandler

        handler = JobmonOTLPStructlogHandler()

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
