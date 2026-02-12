"""Configuration management commands.

Commands for viewing and updating Jobmon configuration:
- Update configuration values
"""

from typing import Optional


def update_config_value(
    key: str,
    value: str,
    config_file: Optional[str] = None,
) -> str:
    """Update a configuration value in the defaults.yaml file using dot notation.

    Args:
        key: Dot-notated key (e.g., 'http.retries_attempts', 'distributor.poll_interval')
        value: New value to set
        config_file: Optional path to specific config file to update

    Returns:
        Success message indicating what was updated

    Raises:
        ValueError: If the key doesn't exist in the current configuration
    """
    from jobmon.core.configuration import JobmonConfig

    # Load current config
    config = JobmonConfig(filepath=config_file or "")

    # Split the dot-notated key into parts
    key_parts = key.split(".")
    if len(key_parts) < 2:
        raise ValueError(
            f"Key '{key}' must be in dot notation format (e.g., 'section.key'). "
            f"Valid sections are: {list(config._config.keys())}"
        )

    section = key_parts[0]
    nested_keys = key_parts[1:]

    # Validate that the section exists
    if section not in config._config:
        available_sections = list(config._config.keys())
        raise ValueError(
            f"Section '{section}' not found in configuration. "
            f"Available sections: {available_sections}"
        )

    # Navigate to the nested key and validate it exists
    current_dict = config._config[section]
    navigation_path = [section]

    for i, nested_key in enumerate(nested_keys[:-1]):  # All but the last key
        if not isinstance(current_dict, dict) or nested_key not in current_dict:
            available_keys = (
                list(current_dict.keys()) if isinstance(current_dict, dict) else []
            )
            full_path = ".".join(navigation_path)
            raise ValueError(
                f"Key '{nested_key}' not found in '{full_path}'. "
                f"Available keys: {available_keys}"
            )
        current_dict = current_dict[nested_key]
        navigation_path.append(nested_key)

    # Validate the final key exists
    final_key = nested_keys[-1]
    if not isinstance(current_dict, dict) or final_key not in current_dict:
        available_keys = (
            list(current_dict.keys()) if isinstance(current_dict, dict) else []
        )
        full_path = ".".join(navigation_path)
        raise ValueError(
            f"Key '{final_key}' not found in '{full_path}'. "
            f"Available keys: {available_keys}"
        )

    # Store the old value for the return message
    old_value = current_dict[final_key]

    # Coerce the new value to the appropriate type using JobmonConfig's method
    coerced_value = config._coerce_value(value)

    # Update the value in the config
    current_dict[final_key] = coerced_value

    # Write the updated config back to file
    config.write()

    return (
        f"Successfully updated '{key}' from '{old_value}' to '{coerced_value}' "
        f"in {config._filepath}"
    )
