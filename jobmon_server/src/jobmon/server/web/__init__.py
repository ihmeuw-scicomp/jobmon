"""Web API subpackage."""

from sqlalchemy import orm

# configurable session factory. add an engine using session_factory.configure(bind=eng)
session_factory = orm.sessionmaker(future=True)
