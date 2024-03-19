"""Parse configuration options and set them to be used throughout the Jobmon Architecture."""

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from jobmon.core import CONFIG_FILE_FROM_INSTALLER_PLUGIN
from jobmon.core.cli import CLI
from jobmon.core.exceptions import ConfigError


DEFAULTS_FILE_NAME = "defaults.yaml"
DEFAULTS_FILE = Path(__file__).parent / DEFAULTS_FILE_NAME
ENV_VAR_PREFIX = "JOBMON__"


class JobmonConfig:
    """Default config setup using YAML."""

    def __init__(self, filepath: str = "", dict_config: Optional[Dict] = None) -> None:
        """Jobmon config class.

        Args:
            filepath: where to read defaults from.
            dict_config: dictionary of values to override

        Config file priority:
            1. user specified file passed in
            2. environment variable JOBMON__CONFIG_FILE (backdoor for testing):q!
            3. config file from installer
            4. default config file in core
        """
        if filepath:
            self._filepath = filepath
        else:
            # Allow the user to specify a different config file using an environment variable
            self._filepath = os.getenv("JOBMON__CONFIG_FILE", "")
            # if the env not set, check if the installer plugin exists
            if self._filepath == "":
                # if the installer plugin exists, use the config file form the plugin
                self._filepath = CONFIG_FILE_FROM_INSTALLER_PLUGIN

        if self._filepath:
            with open(self._filepath, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f)
        else:
            # when no config file in env and not installer plug-in,
            # use the default yaml in core
            self._filepath = DEFAULTS_FILE  # type: ignore
            with open(DEFAULTS_FILE, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f)

        self._dict_config = dict_config

    def _merge_dicts(self, base: Dict, override: Dict) -> Dict:
        """Utility function to merge two dictionaries."""
        for key, value in override.items():
            if isinstance(value, dict):
                base[key] = self._merge_dicts(base.get(key, {}), value)
            else:
                base[key] = value
        return base

    def _get_env_var_name(self, section: str, key: str) -> str:
        return f"{ENV_VAR_PREFIX}{section.upper()}__{key.upper()}"

    def _get_environment_variable(self, section: str, key: str) -> Optional[str]:
        # must have format JOBMON__{SECTION}__{KEY} (note double underscore)
        env_var = self._get_env_var_name(section, key)
        return os.environ.get(env_var)

    def _interpolate_env_vars(self, value: Any) -> Any:
        if isinstance(value, str):
            return os.path.expandvars(value)
        return value

    def _get_yaml_variable(self, section: str, key: str) -> Optional[str]:
        return self._config.get(section, {}).get(key)

    def _get_dict_config_variable(self, section: str, key: str) -> Optional[str]:
        if self._dict_config:
            return self._dict_config.get(section, {}).get(key)
        return None

    def _wrapped_get(self, section: str, key: str) -> str:
        # First check in the dict_config
        val = self._get_dict_config_variable(section, key)
        if val is not None:
            return self._interpolate_env_vars(val)

        # Then check environment variable
        val = self._get_environment_variable(section, key)
        if val is not None:
            return val

        # Then check in the merged _config
        val = self._get_yaml_variable(section, key)
        if val is not None:
            return self._interpolate_env_vars(val)

        raise ConfigError(
            f'"{key}" key not found in "{section}" section of {self._filepath}. Fallback '
            f'option using environment var "{self._get_env_var_name(section, key)}" was not '
            "found."
        )

    def get(self, section: str, key: str) -> str:
        """Get the configuration value for the section and key. Raise if key not found.

        Args:
            section: the section of the yaml to search.
            key: the key within the section to retrieve

        Raises: ConfigError
        """
        try:
            val = self._wrapped_get(section, key)
            return self._interpolate_env_vars(val)
        except ConfigError as e:
            raise e

    def get_section(self, section: str) -> Dict[str, Any]:
        """Returns a dictionary of all key-value pairs in the given section.

        The order of precedence: dict_config > Environment Variable > YAML File.
        """
        # Start with the section from the YAML file, or an empty dict if the section
        # doesn't exist yet.
        section_dict = self._config.get(section, {}).copy()

        # Overlay values from dict_config, if any
        if self._dict_config:
            section_dict.update(self._dict_config.get(section, {}))

        # Check environment for variables related to this section and overlay them
        prefix = f"{ENV_VAR_PREFIX}{section.upper()}__"
        for env_key in os.environ.keys():
            if env_key.startswith(prefix):
                # Extract the original key name by removing the prefix
                key = env_key[len(prefix) :].lower()
                section_dict[key] = os.environ[env_key]

        return section_dict

    def get_boolean(self, section: str, key: str) -> bool:
        """Get the configuration value for the section and key as bool.

        Raise if key not found.
        """
        val = str(self.get(section, key)).lower().strip()
        if val in ("t", "true", "1", "yes"):
            return True
        elif val in ("f", "false", "0", "no"):
            return False
        else:
            raise ConfigError(
                f'Failed to convert value to bool. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}". '
                f'Current value: "{val}".'
            )

    def get_int(self, section: str, key: str) -> int:
        """Get the configuration value for the section and key as int.

        Raise if key not found.
        """
        val = self.get(section, key)
        try:
            return int(val)
        except ValueError as exc:
            raise ConfigError(
                f'Failed to convert value to int. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}". '
                f'Current value: "{val}".'
            ) from exc

    def get_float(self, section: str, key: str) -> float:
        """Get the configuration value for the section/key as float. Raise if key not found."""
        val = self.get(section, key)
        try:
            return float(val)
        except ValueError as exc:
            raise ConfigError(
                f'Failed to convert value to float. Please check "{key}" key in "{section}" '
                f'section or environment var "{self._get_env_var_name(section, key)}". '
                f'Current value: "{val}".'
            ) from exc

    def set(self, section: str, key: str, val: str) -> None:
        """Set the configuration value for the section/key."""
        if section not in self._config:
            self._config[section] = {}
        self._config[section][key] = val

    def write(self, filepath: Union[str, Path] = "") -> None:
        """Persist the current config to disk."""
        if not filepath:
            filepath = self._filepath
        if not filepath:
            filepath = DEFAULTS_FILE
        with open(filepath, "w") as f:
            yaml.safe_dump(self._config, f)


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
