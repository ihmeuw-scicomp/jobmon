"""Initialize Web services."""
from sqlalchemy_utils import create_database, database_exists

from jobmon.server.web.db_admin import apply_migrations

print("This script creates a sqlite db for jobmon_gui testing. You can ignore the error output.")

sql_file = "/tmp/tests.sqlite"

database_uri = f"sqlite:///{sql_file}"

if not database_exists(database_uri):
    create_database(database_uri)

apply_migrations(database_uri)
