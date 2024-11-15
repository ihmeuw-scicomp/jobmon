import sys

import uvicorn

from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.api import configure_logging, get_app
from jobmon.server.web.config import get_jobmon_config
from jobmon.server.web.db_admin import init_db
from jobmon.server.web.models import load_model


def main(test: bool) -> None:
    """Run the web server."""
    if test:
        import logging

        logging.basicConfig()
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
        print("Running in test mode.")
        # jobmon_cli string
        database_uri = "sqlite:///test.db"

        config = JobmonConfig(
            dict_config={
                "db": {"sqlalchemy_database_uri": database_uri},
                "otlp": {
                    "web_enabled": "false",
                    "span_exporter": "",
                    "log_exporter": "",
                },
            }
        )
        config = get_jobmon_config(config)
        print(f"Using database URI: {config.get('db', 'sqlalchemy_database_uri')}")
        init_db()
        load_model()
        configure_logging(
            loggers_dict={
                "jobmon.server.web": {
                    "handlers": ["console_text"],
                    "level": "INFO",
                },
                # enable SQL debug
                "sqlalchemy": {
                    "handlers": ["console_text"],
                    "level": "WARNING",
                },
            }
        )

    app = get_app()

    for route in app.routes:
        print(f"Path: {route.path}, Methods: {route.methods}")  # type: ignore

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8088,
    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        main(True)
    else:
        main(False)
