"""Initialize Web services."""
import os
import multiprocessing as mp
from time import sleep
from jobmon.server.web.app_factory import AppFactory  # noqa F401
from flask_cors import CORS
from jobmon.server.web.models import init_db
from sqlalchemy import create_engine
from jobmon.client.api import Tool

print("This script creates a sqlite db for jobmon_gui testing. You can ignor the error output.")

sql_file = "/tmp/tests.sqlite"

database_uri = f"sqlite:///{sql_file}"

init_db(create_engine(database_uri))
