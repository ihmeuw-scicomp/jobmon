"""OTLP configuration validation utilities."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional


def validate_otlp_exporter_config(
    config: Dict[str, Any], exporter_type: str = "log"
) -> List[str]:
    """Validate OTLP exporter configuration and return list of issues.

    Args:
        config: Exporter configuration dictionary
        exporter_type: Type of exporter ('log', 'trace', 'metric')

    Returns:
        List of validation error messages. Empty list if valid.
    """
    issues = []

    # Check required fields
    if not config.get("module"):
        issues.append("Missing required field: module")
    if not config.get("class"):
        issues.append("Missing required field: class")

    # Define supported parameters by exporter type
    SUPPORTED_PARAMS = {
        "log": {
            "endpoint",
            "headers",
            "timeout",
            "compression",
            "insecure",
            "max_export_batch_size",
            "export_timeout_millis",
            "schedule_delay_millis",
            "max_queue_size",
        },
        "trace": {
            "endpoint",
            "headers",
            "timeout",
            "compression",
            "insecure",
            "options",
            "max_export_batch_size",
            "export_timeout_millis",
            "schedule_delay_millis",
            "max_queue_size",
        },
        "metric": {
            "endpoint",
            "headers",
            "timeout",
            "compression",
            "insecure",
            "options",
            "aggregation_temporality",
            "max_export_batch_size",
            "export_timeout_millis",
            "schedule_delay_millis",
            "max_queue_size",
        },
    }

    supported = SUPPORTED_PARAMS.get(exporter_type, SUPPORTED_PARAMS["log"])

    # Check for unsupported parameters
    config_keys = set(config.keys()) - {"module", "class"}  # Exclude metadata fields
    unsupported = config_keys - supported

    if unsupported:
        issues.append(
            f"Unsupported parameters for {exporter_type} exporter: {sorted(unsupported)}"
        )

    # Specific validation for known problematic parameters
    if "options" in config and exporter_type == "log":
        issues.append(
            "'options' parameter is not supported by OTLPLogExporter. Remove this parameter."
        )

    # Validate endpoint format
    endpoint = config.get("endpoint")
    if endpoint:
        if not isinstance(endpoint, str):
            issues.append("'endpoint' must be a string")
        elif not (endpoint.startswith("http://") or endpoint.startswith("https://")):
            issues.append("'endpoint' must start with http:// or https://")

    # Validate timeout
    timeout = config.get("timeout")
    if timeout is not None:
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            issues.append("'timeout' must be a positive number")

    # Validate batch size parameters
    for param in ["max_export_batch_size", "max_queue_size"]:
        value = config.get(param)
        if value is not None:
            if not isinstance(value, int) or value <= 0:
                issues.append(f"'{param}' must be a positive integer")

    # Validate timing parameters
    for param in ["export_timeout_millis", "schedule_delay_millis"]:
        value = config.get(param)
        if value is not None:
            if not isinstance(value, int) or value < 0:
                issues.append(f"'{param}' must be a non-negative integer")

    return issues


def validate_logging_config_otlp(config: Dict[str, Any]) -> Dict[str, List[str]]:
    """Validate OTLP configuration in a logging config dictionary.

    Args:
        config: Full logging configuration dictionary

    Returns:
        Dictionary mapping handler names to lists of validation issues
    """
    validation_results = {}

    handlers = config.get("handlers", {})
    otlp_handler_classes = {
        "jobmon.core.otlp.JobmonOTLPLoggingHandler",
        "jobmon.core.otlp.JobmonOTLPStructlogHandler",
        "jobmon.core.otlp.handlers.JobmonOTLPLoggingHandler",
        "jobmon.core.otlp.handlers.JobmonOTLPStructlogHandler",
    }

    for handler_name, handler_config in handlers.items():
        handler_class = handler_config.get("class", "")

        if handler_class in otlp_handler_classes:
            exporter_config = handler_config.get("exporter", {})
            if exporter_config:
                issues = validate_otlp_exporter_config(exporter_config, "log")
                if issues:
                    validation_results[handler_name] = issues

    return validation_results


def log_validation_results(
    validation_results: Dict[str, List[str]], logger: Optional[logging.Logger] = None
) -> None:
    """Log validation results using the provided logger.

    Args:
        validation_results: Dictionary mapping handler names to validation issues
        logger: Logger to use. If None, uses default logger.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if not validation_results:
        logger.info("OTLP configuration validation passed - no issues found")
        return

    logger.error("OTLP configuration validation failed:")
    for handler_name, issues in validation_results.items():
        logger.error(f"Handler '{handler_name}' has {len(issues)} issue(s):")
        for issue in issues:
            logger.error(f"  - {issue}")


def validate_and_log_otlp_config(
    config: Dict[str, Any], logger: Optional[logging.Logger] = None
) -> bool:
    """Validate OTLP configuration and log results.

    Args:
        config: Full logging configuration dictionary
        logger: Logger to use for validation results

    Returns:
        True if validation passed, False if issues were found
    """
    validation_results = validate_logging_config_otlp(config)
    log_validation_results(validation_results, logger)
    return len(validation_results) == 0
