# create a singleton of jobmon_config
from typing import Optional

from jobmon.core.configuration import JobmonConfig

# a singleton to holp jobmon config on the server side
class _JobmonConfigSingleton:
    _config = None

    @classmethod
    def get_instance(cls, config: Optional[JobmonConfig] = None) -> JobmonConfig:
        """
        Returns the singleton instance of JobmonConfig. If no instance exists, use the input `config`
        to create one. If an instance already exists, the input `config` is ignored.

        :param config: An instance of JobmonConfig to be used if no instance exists
        :return: The singleton instance of JobmonConfig
        """
        if cls._config is None:
            if config is None:
                # use default config
                cls._config = JobmonConfig()
            else:
                cls._config = config
        return cls._config


def get_jobmon_config(config: Optional[JobmonConfig] = None) -> JobmonConfig:
    """Get the jobmon config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    if config:
        _JobmonConfigSingleton._config = config
        return config
    else:
        existing_config = _JobmonConfigSingleton.get_instance()
        return existing_config
