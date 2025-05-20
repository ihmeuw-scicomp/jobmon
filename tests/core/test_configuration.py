from unittest.mock import mock_open

import pytest

from jobmon.core.configuration import JobmonConfig


@pytest.fixture
def temp_yaml_file(tmp_path):
    data = """
    http:
      request_timeout: 20
      retries_timeout: 300
    """
    file_path = tmp_path / "test_config.yaml"
    file_path.write_text(data)
    return file_path


def test_get_method(temp_yaml_file):
    config = JobmonConfig(filepath=str(temp_yaml_file))
    assert config.get("http", "request_timeout") == 20


def test_env_variable(monkeypatch, temp_yaml_file):
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "200")
    config = JobmonConfig()
    assert config.get_int("http", "request_timeout") == 200


def test_env_variable_interpolation(monkeypatch, temp_yaml_file):
    monkeypatch.setenv("TEST_VAR", "400")
    data_with_env = """
    http:
      request_timeout: "$TEST_VAR"
    """
    temp_yaml_file.write_text(data_with_env)
    config = JobmonConfig(filepath=str(temp_yaml_file))
    assert config.get_int("http", "request_timeout") == 400


def test_get_section(temp_yaml_file, monkeypatch):
    # Mocking an environment variable
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "25")
    monkeypatch.setenv("JOBMON__HTTP__MISSING", "foo")

    config = JobmonConfig(filepath=str(temp_yaml_file))

    section_data = config.get_section("http")

    # Assert that the value from the environment variable is used
    assert section_data["request_timeout"] == "25"

    # Assert other values remain unaffected
    assert section_data["retries_timeout"] == 300

    assert section_data["missing"] == "foo"


def test_config_dictionary_override(monkeypatch):
    # Assuming the original configuration file contains these values:
    original_config_content = """
    http:
        request_timeout: 20
        retries_timeout: 300
    """

    # Mock the file reading operation to return the original_config_content
    monkeypatch.setattr("builtins.open", mock_open(read_data=original_config_content))

    # Now, let's pretend there's also an environment variable set:
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "25")

    # Initialize the config with a dictionary that overrides the request_timeout
    overriding_dict = {"http": {"request_timeout": 30}}

    config = JobmonConfig(dict_config=overriding_dict)

    assert (
        config.get("http", "request_timeout") == 30
    )  # It should respect the dict over env vars and file content.
    assert (
        config.get("http", "retries_timeout") == 300
    )  # This should still come from the file as it wasn't overridden.


def test_nested_env_vars_basic(monkeypatch):
    """Test that nested environment variables correctly create nested dicts."""
    # Set up nested environment variables
    monkeypatch.setenv("JOBMON__DB__SQLALCHEMY_CONNECT_ARGS__SSL", "true")
    monkeypatch.setenv("JOBMON__DB__SQLALCHEMY_CONNECT_ARGS__SSL_VERIFY_CERT", "false")

    config = JobmonConfig()
    db_section = config.get_section("db")

    # Check that nested structure was created correctly
    assert "sqlalchemy_connect_args" in db_section
    assert isinstance(db_section["sqlalchemy_connect_args"], dict)
    assert db_section["sqlalchemy_connect_args"]["ssl"] == "true"
    assert db_section["sqlalchemy_connect_args"]["ssl_verify_cert"] == "false"


def test_deep_nested_env_vars(monkeypatch):
    """Test deeply nested environment variables (3+ levels)."""
    # Set up deeply nested environment variables
    monkeypatch.setenv("JOBMON__TEST__LEVEL1__LEVEL2__LEVEL3__DEEP_KEY", "deep_value")

    config = JobmonConfig()
    test_section = config.get_section("test")

    # Navigate through the nested structure
    assert "level1" in test_section
    assert "level2" in test_section["level1"]
    assert "level3" in test_section["level1"]["level2"]
    assert test_section["level1"]["level2"]["level3"]["deep_key"] == "deep_value"


def test_mixed_nested_and_flat_env_vars(monkeypatch):
    """Test mixing nested and non-nested environment variables."""
    # Set up mixed environment variables
    monkeypatch.setenv("JOBMON__MIXED__FLAT_KEY", "flat_value")
    monkeypatch.setenv("JOBMON__MIXED__NESTED__KEY", "nested_value")

    config = JobmonConfig()
    mixed_section = config.get_section("mixed")

    # Check both flat and nested keys
    assert mixed_section["flat_key"] == "flat_value"
    assert "nested" in mixed_section
    assert mixed_section["nested"]["key"] == "nested_value"


def test_nested_env_vars_with_config_file(monkeypatch, tmp_path):
    """Test integration of nested env vars with config from file."""
    # Create config file with nested structure
    config_content = """
    integration:
      outer_key: file_value
      nested:
        inner_key: file_inner_value
    """
    config_file = tmp_path / "nested_config.yaml"
    config_file.write_text(config_content)

    # Override with env vars
    monkeypatch.setenv("JOBMON__INTEGRATION__NESTED__INNER_KEY", "env_inner_value")
    monkeypatch.setenv("JOBMON__INTEGRATION__NESTED__NEW_KEY", "new_value")

    config = JobmonConfig(filepath=str(config_file))
    integration_section = config.get_section("integration")

    # Check merging of file and env values
    assert integration_section["outer_key"] == "file_value"  # Unchanged from file
    assert (
        integration_section["nested"]["inner_key"] == "env_inner_value"
    )  # Overridden by env
    assert integration_section["nested"]["new_key"] == "new_value"  # Added by env


def test_nested_env_vars_with_dict_override(monkeypatch):
    """Test precedence between nested env vars and dict_config."""
    # Set env vars
    monkeypatch.setenv("JOBMON__PRECEDENCE__NESTED__ENV_KEY", "env_value")

    # Create dict override with nested structure
    dict_override = {
        "precedence": {
            "nested": {
                "env_key": "dict_value",  # Should override env var
                "dict_only_key": "only_in_dict",  # Should be present
            }
        }
    }

    config = JobmonConfig(dict_config=dict_override)
    precedence_section = config.get_section("precedence")

    # Dict config should take precedence over env vars
    assert precedence_section["nested"]["env_key"] == "dict_value"
    assert precedence_section["nested"]["dict_only_key"] == "only_in_dict"
