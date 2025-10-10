import logging
import logging.config

# Test if dictConfig creates duplicate handlers
config = {
    'version': 1,
    'handlers': {
        'otlp1': {
            'class': 'jobmon.core.otlp.JobmonOTLPLoggingHandler',
            'level': 'INFO',
        },
        'otlp2': {
            'class': 'jobmon.core.otlp.JobmonOTLPStructlogHandler',
            'level': 'INFO',
        },
    },
    'loggers': {
        'test_logger': {
            'handlers': ['otlp1'],  # Only one handler
            'level': 'INFO',
            'propagate': False,
        }
    }
}

logging.config.dictConfig(config)

# Check the manager's processor count
from jobmon.core.otlp import JobmonOTLPManager
manager = JobmonOTLPManager.get_instance()
if manager.logger_provider:
    processors = manager.logger_provider._log_record_processors
    print(f"Number of processors: {len(processors)}")
    for i, proc in enumerate(processors):
        print(f"  Processor {i}: {type(proc).__name__}")

# Check handler count on logger
logger = logging.getLogger('test_logger')
print(f"\nNumber of handlers on test_logger: {len(logger.handlers)}")
for i, h in enumerate(logger.handlers):
    print(f"  Handler {i}: {type(h).__name__}")
