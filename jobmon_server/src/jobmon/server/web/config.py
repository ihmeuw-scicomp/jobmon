# create a singleton of jobmon_config
from typing import Optional

from jobmon.core.configuration import JobmonConfig

# a singleton to holp jobmon config on the server side
# it seems that server config is not changed after the server is started
_jobmon_config = None

def get_jobmon_config(config: Optional[JobmonConfig] = None) -> JobmonConfig:
    """Get the jobmon config. If no config is provided, defaults are used.

    Args:
        config: The jobmon config to use when creating the app.
    """
    global _jobmon_config
    if _jobmon_config is None:
        if config is None:
            _jobmon_config = JobmonConfig()
        else:
            # leave an entry for testing
            _jobmon_config = config
    return _jobmon_config
