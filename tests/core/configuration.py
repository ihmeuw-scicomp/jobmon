from pathlib import Path

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
    monkeypatch.setattr('builtins.open', mock_open(read_data=original_config_content))

    # Now, let's pretend there's also an environment variable set:
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "25")

    # Initialize the config with a dictionary that overrides the request_timeout
    overriding_dict = {
        'http': {
            'request_timeout': 30
        }
    }

    config = JobmonConfig(dict_config=overriding_dict)

    assert config.get("http", "request_timeout") == 30  # It should respect the dict over env vars and file content.
    assert config.get("http", "retries_timeout") == 300  # This should still come from the file as it wasn't overridden.

