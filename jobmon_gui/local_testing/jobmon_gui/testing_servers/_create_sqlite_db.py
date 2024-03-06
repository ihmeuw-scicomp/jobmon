"""Initialize Web services."""
from jobmon.server.web.db_admin import init_db

print("This script creates a sqlite db for jobmon_gui testing. You can ignor the error output.")

sql_file = "/tmp/tests.sqlite"

database_uri = f"sqlite:///{sql_file}"

init_db(database_uri)
