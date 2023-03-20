"""Parse configuration options and set them to be used throughout the Jobmon Architecture."""
import argparse
import configparser
import importlib
import os
from pathlib import Path
import pkgutil
from typing import Dict, Mapping, MutableMapping, Optional

from jobmon.core.cli import CLI
from jobmon.core.exceptions import ConfigError

DEFAULTS_FILE_NAME = "defaults.ini"
DEFAULTS_FILE = Path(__file__).parent / DEFAULTS_FILE_NAME
ENV_VAR_PREFIX = "JOBMON__"


class EnvInterpolation(configparser.BasicInterpolation):
    """Interpolation which expands environment variables in values."""

    def before_get(
        self,
        parser: MutableMapping[str, Mapping[str, str]],
        section: str,
        option: str,
        value: str,
        defaults: Mapping[str, str],
    ) -> str:
        """Expand env variables when they are read."""
        value = super().before_get(parser, section, option, value, defaults)
        return os.path.expandvars(value)


class JobmonConfig:
    """Default config setup."""

    def __init__(self, filepath: str = "", dict_config: Optional[Dict] = None) -> None:
        """Jobmon config class.

        Args:
            filepath: where to read defaults from.
            dict_config: dictionary of values to override
        """
        if filepath:
            self._filepath = filepath
        else:
            self._filepath = os.getenv("JOBMON__CONFIG_FILE", "")
        self._ini_config = configparser.ConfigParser(interpolation=EnvInterpolation())
        self._ini_config.read(str(DEFAULTS_FILE))
        if self._filepath:
            self._ini_config.read(self._filepath)
        if dict_config is not None:
            self._ini_config.read_dict(dict_config)

    def _get_env_var_name(self, section: str, key: str) -> str:
        return f"{ENV_VAR_PREFIX}{section.upper()}__{key.upper()}"

    def _get_environment_variable(self, section: str, key: str) -> Optional[str]:
        # must have format JOBMON__{SECTION}__{KEY} (note double underscore)
        env_var = self._get_env_var_name(section, key)
        if env_var in os.environ:
            return os.environ[env_var]
        return None

    def _get_ini_variable(self, section: str, key: str) -> Optional[str]:
        if self._ini_config.has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return self._ini_config.get(section, key)
        return None

    def _wrapped_get(self, section: str, key: str) -> str:
        section = str(section).lower()
        key = str(key).lower()

        val = self._get_environment_variable(section, key)
        if val is not None:
            return val

        val = self._get_ini_variable(section, key)
        if val is not None:
            return val

        raise ConfigError(
            f'"{key}" key not found in "{section}" section of {self._filepath}. Fallback '
            f'option using environment var "{self._get_env_var_name(section, key)}" was not '
            "found."
        )

    def get(self, section: str, key: str) -> str:
        """Get the configuration value for the section and key. Raise if key not found.

        Args:
            section: the section of the ini file to search.
            key: the key within the section to retrieve

        Raises: ConfigError, RuntimeError
        """
        try:
            val = self._wrapped_get(section, key)
        except ConfigError as e:
            print(
                "Jobmon client not configured. Attempting to install configuration from "
                "installer plugin."
            )
            # try and import any installers
            plugins = [
                plugin_name
                for finder, plugin_name, ispkg in pkgutil.iter_modules()
                if plugin_name.startswith("jobmon_installer")
            ]
            if len(plugins) == 1:
                plugin_name = plugins[0]
                print(f"Found one plugin: {plugin_name}")
                module = importlib.import_module(plugin_name)
                config_installer = getattr(module, "install_config")
                config_installer()
                print(f"Successfully ran installer from {module}.")
            elif len(plugins) > 1:
                raise RuntimeError(
                    "Found multiple plugins while installing config from plugin, but only one "
                    f'is allowed. Got "{plugins}".'
                ) from e
            else:
                raise e

        val = self._wrapped_get(section, key)
        return val

    def get_boolean(self, section: str, key: str) -> bool:
        """Get the configuration value for the section and key as bool. Raise if key not found.

        Args:
            section: the section of the ini file to search.
            key: the key within the section to retrieve

        Raises: ConfigError, RuntimeError
        """
        val = str(self.get(section, key)).lower().strip()
        if val in ("t", "true", "1"):
            return True
        elif val in ("f", "false", "0"):
            return False
        else:
            raise ConfigError(
                f'Failed to convert value to bool. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}" Current '
                f'value: "{val}".'
            )

    def get_int(self, section: str, key: str) -> int:
        """Get the configuration value for the section and key as int. Raise if key not found.

        Args:
            section: the section of the ini file to search.
            key: the key within the section to retrieve

        Raises: ConfigError, RuntimeError
        """
        val = self.get(section, key)
        if val is None:
            raise ConfigError(
                f'Failed to convert None to int. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}".'
            )
        try:
            return int(val)
        except ValueError:
            raise ConfigError(
                f'Failed to convert value to int. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}" Current '
                f'value: "{val}".'
            )

    def get_float(self, section: str, key: str) -> float:
        """Get the configuration value for the section/key as float. Raise if key not found.

        Args:
            section: the section of the ini file to search.
            key: the key within the section to retrieve

        Raises: ConfigError, RuntimeError
        """
        val = self.get(section, key)
        if val is None:
            raise ConfigError(
                f'Failed to convert None to int. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}".'
            )
        try:
            return float(val)
        except ValueError:
            raise ConfigError(
                f'Failed to convert value to float. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}" Current '
                f'value: "{val}".'
            )

    def set(self, section: str, key: str, val: str) -> None:
        """Set the configuration value for the section/key. Must call write() to persist."""
        if not self._ini_config.has_section(section):
            self._ini_config.add_section(section)
        self._ini_config.set(section, key, val)

    def write(self, filepath: str = "") -> None:
        """Persist the current config to disc.

        Args:
            filepath: the location to write the config to.
        """
        if not filepath:
            filepath = str(self._filepath)
        if not filepath:
            filepath = str(DEFAULTS_FILE)
        with open(filepath, "w") as configfile:
            self._ini_config.write(configfile)


class ConfigCLI(CLI):
    """Client command line interface for update the config."""

    def __init__(self) -> None:
        """Initialization of client CLI."""
        super().__init__()
        self._subparsers = self.parser.add_subparsers(
            dest="sub_command", parser_class=argparse.ArgumentParser
        )
        self._add_update_config_subparser()

    @staticmethod
    def update_config(args: argparse.Namespace) -> None:
        """Update .jobmon.ini.

        Args:
            args: only --web_service_fqdn --web_service_port are expected.
        """
        config = JobmonConfig()
        config.set(
            "http",
            "service_url",
            f"http://{args.web_service_fqdn}:{args.web_service_port}",
        )
        config.write()

    def _add_update_config_subparser(self) -> None:
        update_config_parser = self._subparsers.add_parser("update")
        update_config_parser.set_defaults(func=self.update_config)
        update_config_parser.add_argument(
            "--web_service_fqdn", type=str, help="The fqdn of the web service."
        )
        update_config_parser.add_argument(
            "--web_service_port",
            type=str,
            help="The port for the web service..",
        )


def main(argstr: Optional[str] = None) -> None:
    """Create CLI."""
    cli = ConfigCLI()
    cli.main(argstr)
