"""Configure Logging for structlog, OpenTelemetry, etc."""

import logging
import logging.config
import os
from typing import Dict, List, Optional

import structlog

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError


def configure_structlog(extra_processors: Optional[List] = None) -> None:
    """Configure structlog processors."""
    if extra_processors is None:
        extra_processors = []
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            *extra_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def configure_logging(
    dict_config: Optional[Dict] = None, file_config: str = ""
) -> None:
    """Configure logging for the server.

    Args:
        dict_config: Logging configuration as a dictionary (highest precedence)
        file_config: Path to logging configuration file (second precedence)

    The configuration is selected in the following order:
    1. dict_config parameter (if provided)
    2. file_config parameter (if provided and file exists)
    3. JobmonConfig logging.server_logconfig_file setting
    4. Auto-selected template based on telemetry configuration
    """
    # Explicit dict config takes highest precedence
    if dict_config:
        _apply_logging_config(dict_config, "explicit dict config")
        return

    # Explicit file config takes second precedence
    if file_config and os.path.exists(file_config):
        from jobmon.core.config.template_loader import load_logconfig_with_templates

        logging_config = load_logconfig_with_templates(file_config)
        _apply_logging_config(logging_config, f"explicit file config: {file_config}")
        return

    # Check for JobmonConfig logging.server_logconfig_file setting (third precedence)
    try:
        config = JobmonConfig()

        # Check if user specified a custom logconfig file
        try:
            custom_logconfig_file = config.get("logging", "server_logconfig_file")
            if custom_logconfig_file:
                from jobmon.core.config.template_loader import (
                    load_logconfig_with_templates,
                )

                logging_config = load_logconfig_with_templates(custom_logconfig_file)
                _apply_logging_config(
                    logging_config, f"JobmonConfig setting: {custom_logconfig_file}"
                )
                return
        except (ConfigError, AttributeError, KeyError):
            pass  # Fall through to auto-select

        # Auto-select template based on telemetry configuration (fallback)
        otlp_enabled = _is_otlp_enabled(config)

        # Select appropriate template
        current_dir = os.path.dirname(__file__)
        template_name = (
            "logconfig_server_otlp.yaml" if otlp_enabled else "logconfig_server.yaml"
        )
        template_path = os.path.join(current_dir, "config", template_name)

        # Load with full template system support (handles user overrides, fallbacks, etc.)
        from jobmon.core.config.logconfig_utils import load_logconfig_with_overrides

        logging_config = load_logconfig_with_overrides(
            default_template_path=template_path,
            config_section="server",
            config=config,
        )

        _apply_logging_config(
            logging_config, f"auto-selected template: {template_name}"
        )

    except Exception as e:
        # Simple fallback - let the application fail gracefully if logging can't be configured
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        logging.getLogger(__name__).warning(
            f"Failed to configure advanced logging, using basic config: {e}"
        )


def _apply_logging_config(logging_config: Dict, source_description: str) -> None:
    """Apply logging configuration with validation and error reporting.

    Args:
        logging_config: The logging configuration dictionary
        source_description: Description of where the config came from for error reporting
    """
    # Validate OTLP configuration if enabled
    _validate_otlp_configuration(logging_config, source_description)

    # Apply the configuration
    try:
        logging.config.dictConfig(logging_config)

        # Log successful configuration
        logger = logging.getLogger(__name__)
        logger.info(f"Logging configured successfully from {source_description}")

        # Enable OTLP debug logging if requested
        if os.environ.get("JOBMON_OTLP_DEBUG", "").lower() in ("true", "1", "yes"):
            logger.info(
                "OTLP debug logging enabled via JOBMON_OTLP_DEBUG environment variable"
            )

    except Exception as e:
        # Fall back to basic logging if configuration fails
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        logger = logging.getLogger(__name__)
        logger.error(
            f"Failed to apply logging configuration from {source_description}: {e}"
        )
        raise


def _validate_otlp_configuration(logging_config: Dict, source_description: str) -> None:
    """Validate OTLP configuration and log any issues found.

    Args:
        logging_config: The logging configuration dictionary
        source_description: Description of where the config came from
    """
    try:
        from jobmon.core.otlp.validation import validate_and_log_otlp_config

        # Create a temporary logger for validation messages
        # (use basic config since main logging isn't configured yet)
        validation_logger = logging.getLogger("jobmon.otlp.validation")

        # Validate the configuration
        is_valid = validate_and_log_otlp_config(logging_config, validation_logger)

        if not is_valid:
            validation_logger.warning(
                f"OTLP configuration issues found in {source_description}. "
                "Review the validation errors above. OTLP logging may not work correctly."
            )

    except ImportError:
        # Validation module not available - continue without validation
        pass
    except Exception as e:
        # Don't fail configuration loading due to validation errors
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to validate OTLP configuration: {e}")


def _is_otlp_enabled(config: JobmonConfig) -> bool:
    """Check if OTLP is enabled for server logging."""
    try:
        telemetry_section = config.get_section_coerced("telemetry")
        tracing_config = telemetry_section.get("tracing", {})
        return tracing_config.get("server_enabled", False)
    except (ConfigError, AttributeError, KeyError):
        return False
