"""Web API subpackage."""
from sqlalchemy import orm

from jobmon.server.web.log_config import configure_structlog

configure_structlog()

# configurable session factory. add an engine using session_factory.configure(bind=eng)
session_factory = orm.sessionmaker(future=True)
