"""Initialize Web services."""
from jobmon.server.web.db_admin import apply_migrations, init_db

print("This script creates a sqlite db for jobmon_gui testing. You can ignore the error output.")

sql_file = "/tmp/tests.sqlite"

database_uri = f"sqlite:///{sql_file}"

init_db()
apply_migrations(database_uri)
