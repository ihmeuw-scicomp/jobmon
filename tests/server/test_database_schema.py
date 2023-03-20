from sqlalchemy.orm import Session


def test_arg_name_collation(web_server_in_memory):
    """test both lowercase and uppercase have no conflict."""
    app, engine = web_server_in_memory
    with Session(bind=engine) as session:
        result = session.execute(
            """
            INSERT INTO arg(name)
            VALUES
                ('r'),
                ('R'),
                ('test_case'),
                ('TEST_CASE');
            """
        )
        session.commit()
        assert result.rowcount == 4
