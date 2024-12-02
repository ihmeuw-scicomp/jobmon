"""Initialize Web services."""
from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.db_admin import get_jobmon_config, init_db

print("This script creates a sqlite db for jobmon_gui testing. You can ignore the error output.")

sql_file = "/tmp/tests.sqlite"

database_uri = f"sqlite:///{sql_file}"
config = JobmonConfig(
    dict_config={
        "db": {"sqlalchemy_database_uri": database_uri},
    }
)
get_jobmon_config(config)
init_db()
