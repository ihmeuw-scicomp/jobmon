from unittest.mock import mock_open

import pytest

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError


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


def test_basic_configuration_methods(temp_yaml_file):
    """Test basic configuration retrieval methods."""
    config = JobmonConfig(filepath=str(temp_yaml_file))

    # Test basic get method
    assert config.get("http", "request_timeout") == 20

    # Test typed get methods
    assert config.get_int("http", "request_timeout") == 20
    assert config.get_int("http", "retries_timeout") == 300


def test_environment_variable_overrides(monkeypatch, temp_yaml_file):
    """Test environment variable overrides and interpolation."""
    # Clear any existing environment variables that might interfere
    import os

    for key in list(os.environ.keys()):
        if key.startswith("JOBMON__"):
            monkeypatch.delenv(key, raising=False)

    # Test direct env var override
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "200")
    config = JobmonConfig()
    assert config.get_int("http", "request_timeout") == 200

    # Clear env var for interpolation test
    monkeypatch.delenv("JOBMON__HTTP__REQUEST_TIMEOUT", raising=False)

    # Test env var interpolation
    monkeypatch.setenv("TEST_VAR", "400")
    data_with_env = """
    http:
      request_timeout: "$TEST_VAR"
    """
    temp_yaml_file.write_text(data_with_env)
    config = JobmonConfig(filepath=str(temp_yaml_file))
    assert config.get_int("http", "request_timeout") == 400


def test_section_retrieval_with_overrides(temp_yaml_file, monkeypatch):
    """Test get_section with environment variable overlays."""
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "25")
    monkeypatch.setenv("JOBMON__HTTP__MISSING", "foo")

    config = JobmonConfig(filepath=str(temp_yaml_file))
    section_data = config.get_section("http")

    # Assert that the value from the environment variable is used
    assert section_data["request_timeout"] == "25"
    # Assert other values remain unaffected
    assert section_data["retries_timeout"] == 300
    # Assert new values from env vars are added
    assert section_data["missing"] == "foo"


def test_configuration_precedence(monkeypatch):
    """Test precedence: dict_config > env vars > file content."""
    original_config_content = """
    http:
        request_timeout: 20
        retries_timeout: 300
    """
    monkeypatch.setattr("builtins.open", mock_open(read_data=original_config_content))
    monkeypatch.setenv("JOBMON__HTTP__REQUEST_TIMEOUT", "25")

    # Dict config should override everything
    overriding_dict = {"http": {"request_timeout": 30}}
    config = JobmonConfig(dict_config=overriding_dict)

    assert config.get("http", "request_timeout") == 30  # Dict override wins
    assert config.get("http", "retries_timeout") == 300  # File value unchanged


def test_nested_environment_variables_comprehensive(monkeypatch, tmp_path):
    """Comprehensive test for nested environment variable handling."""
    # Clear any existing JOBMON environment variables
    import os

    for key in list(os.environ.keys()):
        if key.startswith("JOBMON__"):
            monkeypatch.delenv(key, raising=False)

    # Create config file with nested structure
    config_content = """
    integration:
      outer_key: file_value
      nested:
        inner_key: file_inner_value
    db:
      simple_key: simple_value
    """
    config_file = tmp_path / "nested_config.yaml"
    config_file.write_text(config_content)

    # Test various nesting scenarios
    monkeypatch.setenv("JOBMON__INTEGRATION__NESTED__INNER_KEY", "env_inner_value")
    monkeypatch.setenv("JOBMON__INTEGRATION__NESTED__NEW_KEY", "new_value")
    monkeypatch.setenv("JOBMON__DB__DEEP__LEVEL1__LEVEL2__DEEP_KEY", "deep_value")
    monkeypatch.setenv("JOBMON__DB__FLAT_KEY", "flat_value")

    config = JobmonConfig(filepath=str(config_file))

    # Test integration section
    integration_section = config.get_section("integration")
    assert integration_section["outer_key"] == "file_value"  # From file
    assert integration_section["nested"]["inner_key"] == "env_inner_value"  # Overridden
    assert integration_section["nested"]["new_key"] == "new_value"  # Added by env

    # Test db section with mixed flat and deep nesting
    db_section = config.get_section("db")
    assert db_section["simple_key"] == "simple_value"  # From file
    assert db_section["flat_key"] == "flat_value"  # From env var
    assert (
        db_section["deep"]["level1"]["level2"]["deep_key"] == "deep_value"
    )  # Deep nesting


def test_db_pool_configuration_comprehensive(tmp_path, monkeypatch):
    """Comprehensive test for database pool configuration."""
    # Clear any existing JOBMON environment variables
    import os

    for key in list(os.environ.keys()):
        if key.startswith("JOBMON__"):
            monkeypatch.delenv(key, raising=False)

    config_content = """
    db:
      sqlalchemy_database_uri: 'sqlite:///test.db'
      pool:
        size: 10
        max_overflow: 20
        timeout: 25
        recycle: 300
        pre_ping: true
    """
    config_file = tmp_path / "pool_config.yaml"
    config_file.write_text(config_content)

    # Test environment variable overrides
    monkeypatch.setenv("JOBMON__DB__POOL__SIZE", "15")
    monkeypatch.setenv("JOBMON__DB__POOL__NEW_SETTING", "true")

    config = JobmonConfig(filepath=str(config_file))

    # Test basic nested structure
    db_section = config.get_section("db")
    assert "pool" in db_section
    assert isinstance(db_section["pool"], dict)

    # Test values and overrides
    assert db_section["pool"]["size"] == "15"  # Overridden by env var
    assert db_section["pool"]["max_overflow"] == 20  # From file
    assert db_section["pool"]["timeout"] == 25  # From file
    assert db_section["pool"]["recycle"] == 300  # From file
    assert db_section["pool"]["pre_ping"] is True  # From file
    assert db_section["pool"]["new_setting"] == "true"  # Added by env var

    # Test empty/missing pool sections in a separate scenario without env vars
    monkeypatch.delenv("JOBMON__DB__POOL__SIZE", raising=False)
    monkeypatch.delenv("JOBMON__DB__POOL__NEW_SETTING", raising=False)

    empty_config = """
    db:
      sqlalchemy_database_uri: 'sqlite:///test.db'
    """
    config_file.write_text(empty_config)
    config = JobmonConfig(filepath=str(config_file))
    db_section = config.get_section("db")
    # Pool section should not exist when not defined in config
    assert "pool" not in db_section or db_section.get("pool") is None


def test_type_coercion_comprehensive(tmp_path):
    """Comprehensive test for type coercion functionality."""
    config_content = """
    test:
      # Boolean variations
      bool_true: "true"
      bool_false: "no"
      bool_int_1: "1" 
      bool_int_0: "0"
      
      # Numeric variations
      int_string: "42"
      int_actual: 100
      float_string: "3.14159"
      float_int: 42
      float_actual: 2.718
      
      # Nested structure with mixed types
      nested:
        inner_bool: "yes"
        inner_int: "123"
        inner_float: "45.67"
    """
    config_file = tmp_path / "coerce_test.yaml"
    config_file.write_text(config_content)

    config = JobmonConfig(filepath=str(config_file))

    # Test individual typed methods
    assert config.get_boolean("test", "bool_true") is True
    assert config.get_boolean("test", "bool_false") is False
    assert config.get_boolean("test", "bool_int_1") is True
    assert config.get_boolean("test", "bool_int_0") is False

    assert config.get_int("test", "int_string") == 42
    assert config.get_int("test", "int_actual") == 100
    assert isinstance(config.get_int("test", "int_string"), int)

    assert config.get_float("test", "float_string") == 3.14159
    assert config.get_float("test", "float_int") == 42.0  # int converted to float
    assert config.get_float("test", "float_actual") == 2.718
    assert isinstance(config.get_float("test", "float_int"), float)

    # Test section-wide coercion
    test_section = config.get_section_coerced("test")
    assert test_section["bool_true"] is True  # String "true" → bool True
    assert test_section["int_string"] == 42  # String "42" → int 42
    assert test_section["float_string"] == 3.14159  # String "3.14159" → float 3.14159
    assert test_section["nested"]["inner_bool"] is True  # Nested coercion
    assert test_section["nested"]["inner_int"] == 123  # Nested coercion


def test_type_coercion_with_environment_variables(monkeypatch, tmp_path):
    """Test that environment variable values are properly coerced."""
    # Clear any existing JOBMON environment variables
    import os

    for key in list(os.environ.keys()):
        if key.startswith("JOBMON__"):
            monkeypatch.delenv(key, raising=False)

    config_content = """
    test:
      pool:
        size: 5
    """
    config_file = tmp_path / "env_coerce_test.yaml"
    config_file.write_text(config_content)

    # Set environment variables with string values that should be coerced
    monkeypatch.setenv("JOBMON__TEST__POOL__SIZE", "15")
    monkeypatch.setenv("JOBMON__TEST__POOL__ENABLED", "true")
    monkeypatch.setenv("JOBMON__TEST__POOL__TIMEOUT", "30.5")

    config = JobmonConfig(filepath=str(config_file))
    test_section = config.get_section_coerced("test")

    # Environment variables should override and be coerced to correct types
    assert test_section["pool"]["size"] == 15  # String "15" from env → int 15
    assert test_section["pool"]["enabled"] is True  # String "true" from env → bool True
    assert (
        test_section["pool"]["timeout"] == 30.5
    )  # String "30.5" from env → float 30.5


def test_engine_integration_with_coerced_configuration(tmp_path, monkeypatch):
    """Test the actual engine logic using coerced configuration."""
    # Clear any existing JOBMON environment variables
    import os

    for key in list(os.environ.keys()):
        if key.startswith("JOBMON__"):
            monkeypatch.delenv(key, raising=False)

    config_content = """
    db:
      sqlalchemy_database_uri: 'sqlite:///test.db'
      pool:
        size: "8"        # String that should be coerced to int
        pre_ping: "true" # String that should be coerced to bool
        timeout: "25"    # String that should be coerced to int
        recycle: "300"   # String that should be coerced to int
    """
    config_file = tmp_path / "real_engine_test.yaml"
    config_file.write_text(config_content)

    monkeypatch.setenv("JOBMON__CONFIG_FILE", str(config_file))

    import jobmon.server.web.config
    from jobmon.server.web.config import get_jobmon_config

    jobmon.server.web.config._jobmon_config = None

    cfg = get_jobmon_config()

    # Use the actual consolidated engine logic
    try:
        db_config = cfg.get_section_coerced("db")
        pool_config = db_config.get("pool", {})
        pool_param_mapping = {
            "recycle": "pool_recycle",
            "pre_ping": "pool_pre_ping",
            "timeout": "pool_timeout",
            "size": "pool_size",
            "max_overflow": "max_overflow",
        }

        pool_kwargs = {}
        for config_key, sqlalchemy_param in pool_param_mapping.items():
            if config_key in pool_config:
                pool_kwargs[sqlalchemy_param] = pool_config[config_key]

    except Exception:
        pool_kwargs = {}

    # Verify types are correctly coerced
    assert pool_kwargs["pool_size"] == 8  # Should be int, not string
    assert pool_kwargs["pool_pre_ping"] is True  # Should be bool, not string
    assert pool_kwargs["pool_timeout"] == 25  # Should be int, not string
    assert pool_kwargs["pool_recycle"] == 300  # Should be int, not string
    assert isinstance(pool_kwargs["pool_size"], int)
    assert isinstance(pool_kwargs["pool_pre_ping"], bool)
    assert isinstance(pool_kwargs["pool_timeout"], int)
    assert isinstance(pool_kwargs["pool_recycle"], int)


def test_error_handling(tmp_path):
    """Test error handling for invalid configuration values."""
    config_content = """
    test:
      not_bool: "maybe"
      not_int: "not_a_number"
      not_float: "also_not_a_number"
    """
    config_file = tmp_path / "error_test.yaml"
    config_file.write_text(config_content)

    config = JobmonConfig(filepath=str(config_file))

    # Test error handling for invalid types
    with pytest.raises(ConfigError, match="Failed to convert value to bool"):
        config.get_boolean("test", "not_bool")

    with pytest.raises(ConfigError, match="Failed to convert value to int"):
        config.get_int("test", "not_int")

    with pytest.raises(ConfigError, match="Failed to convert value to float"):
        config.get_float("test", "not_float")


def test_empty_pool_configuration_engine_compatibility(tmp_path, monkeypatch):
    """Test that empty/null pool configurations don't break engine initialization."""
    # Clear any existing JOBMON environment variables
    import os

    for key in list(os.environ.keys()):
        if key.startswith("JOBMON__"):
            monkeypatch.delenv(key, raising=False)

    # Test various empty pool configurations
    test_configs = [
        # Completely missing pool section
        """
        db:
          sqlalchemy_database_uri: 'sqlite:///test.db'
        """,
        # Empty pool section (becomes None in YAML)
        """
        db:
          sqlalchemy_database_uri: 'sqlite:///test.db'
          pool:
        """,
        # Empty pool section with explicit null
        """
        db:
          sqlalchemy_database_uri: 'sqlite:///test.db'
          pool: null
        """,
        # Empty pool object
        """
        db:
          sqlalchemy_database_uri: 'sqlite:///test.db'
          pool: {}
        """,
    ]

    for i, config_content in enumerate(test_configs):
        config_file = tmp_path / f"empty_pool_test_{i}.yaml"
        config_file.write_text(config_content)

        monkeypatch.setenv("JOBMON__CONFIG_FILE", str(config_file))

        # Reset singleton to pick up new config
        import jobmon.server.web.config

        jobmon.server.web.config._jobmon_config = None

        try:
            # This should not raise a TypeError about NoneType not being iterable
            from jobmon.server.web.db.engine import get_engine

            engine = get_engine()

            # Verify we can create the engine successfully
            assert engine is not None

        except Exception as e:
            pytest.fail(f"Empty pool config test case {i} failed: {e}")

        finally:
            # Clean up
            monkeypatch.delenv("JOBMON__CONFIG_FILE", raising=False)
