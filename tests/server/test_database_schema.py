import unittest

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from jobmon.server.web.db import get_engine


def test_arg_name_collation(web_server_in_memory):
    """test both lowercase and uppercase have no conflict."""
    app, engine = web_server_in_memory
    with Session(bind=engine) as session:
        result = session.execute(
            text(
                """
                INSERT INTO arg(name)
                VALUES
                    ('r'),
                    ('R'),
                    ('test_case'),
                    ('TEST_CASE');
                """
            )
        )
        session.commit()
        assert result.rowcount == 4


class TestDatabase(unittest.TestCase):
    def test_database_tables(self):
        # get a connection to the database
        engine = get_engine()

        # validate a few tables we know should exist
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "workflow" in tables
        assert "task" in tables
        assert "task_instance" in tables

    def test_supported_dialect(self):
        """Test that the dialect detection returns a supported dialect."""
        from jobmon.server.web.db import get_dialect_name

        assert get_dialect_name() in {"mysql", "sqlite", "postgresql"}
