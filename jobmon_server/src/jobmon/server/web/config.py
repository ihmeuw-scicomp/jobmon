# create a singleton on jobmon_config
from typing import Optional

from jobmon.core.configuration import JobmonConfig


# a singleton to holp jobmon config that enables testing
_jobmon_config = None


def get_jobmon_config(config: Optional[JobmonConfig] = None) -> JobmonConfig:
    """Get the jobmon config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    global _jobmon_config
    if config is None:
        # allow testing to override the config
        if _jobmon_config is None:
            # create from default
            _jobmon_config = JobmonConfig()
        else:
            # do nothing; use the existing config instance
            pass
    else:
        # leave an entry for testing
        _jobmon_config = config
    return _jobmon_config
