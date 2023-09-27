import logging
from logging.handlers import SysLogHandler

from jobmon.core.configuration import JobmonConfig
from jobmon.core.exceptions import ConfigError

try:
    from syslog import LOG_USER
except ImportError:
    # Support for windows
    LOG_USER = 8

JOBMON_SYSLOG_DISTRIBUTOR_TAG: str = "jobmon_distributor"
JOBMON_SYSLOG_WORKER_NODE_TAG: str = "jobmon_worker_node"
JOBMON_SYSLOG_CLIENT_TAG: str = "jobmon_client"


class JobmonSyslogHandler(SysLogHandler):

    def __init__(self, syslog_tag, syslog_address='/dev/log', syslog_facility: int = LOG_USER,
                 log_level: int = logging.DEBUG):
        # Default to debug if no level was set
        SysLogHandler.__init__(self, address=syslog_address, facility=syslog_facility)
        fmt = syslog_tag + ': %(asctime)s | %(levelname)s | %(filename)s:%(funcName)s:%(lineno)d | %(name)s | %(message)s'
        formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
        self.setFormatter(formatter)
        self.setLevel(log_level)
        self.name = "syslog"

class JobmonStdErrorHandler(logging.StreamHandler):

    def __init__(self, log_level: int = logging.DEBUG):
        logging.StreamHandler.__init__(self)
        fmt = '%(asctime)s | %(levelname)s | %(filename)s:%(funcName)s:%(lineno)d | %(name)s | %(message)s'
        formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
        self.setFormatter(formatter)
        self.setLevel(log_level)
        self.name = "stderr"


def jobmon_logger(module_name: str, syslog_tag: str = None, log_level: int = logging.DEBUG):
    """
    Get a python logger that logs to stderr, and syslog if syslog is enabled
    """
    logger = logging.getLogger(module_name)

    config = JobmonConfig()
    syslog_enabled = True
    # Attempt to get enable_syslog config, but catch the exception and default to enabled if it is not configured
    try:
        syslog_enabled = config.get_boolean("logging", "enable_syslog")
    except ConfigError:
        pass

    if syslog_tag and syslog_enabled:
        logger.addHandler(JobmonSyslogHandler(syslog_tag=syslog_tag, log_level=log_level))

    logger.addHandler(JobmonStdErrorHandler(log_level=log_level))
    logger.setLevel(log_level)

    return logger
