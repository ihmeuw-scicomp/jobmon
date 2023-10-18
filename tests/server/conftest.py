import pytest


@pytest.fixture(scope="function")
def web_server_in_memory(tmp_path):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server.web.api import AppFactory
    from jobmon.server.web.models import init_db

    # The create_app call sets up database connections
    d = tmp_path / "jobmon.db"
    url = "sqlite:///" + str(d)
    app_factory = AppFactory(url)
    init_db(app_factory.engine)
    app = app_factory.get_app()
    app.config["TESTING"] = True
    with app.app_context():
        client = app.test_client()
        yield client, app_factory.engine


def get_test_content(response):
    """The function called by the no_request_jsm_jqs to query the fake
    test_client for a response
    """
    if "application/json" in response.headers.get("Content-Type"):
        content = response.json
    elif "text/html" in response.headers.get("Content-Type"):
        content = response.data
    else:
        content = response.content
    return response.status_code, content


@pytest.fixture(scope="function")
def requester_in_memory(monkeypatch, web_server_in_memory):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests
    from jobmon.core import requester

    monkeypatch.setenv("JOBMON__HTTP__SERVICE_URL", "1")

    app, engine = web_server_in_memory

    def get_in_mem(url, params, data, headers, **kwargs):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return app.get(path=url, query_string=params, data=data, headers=headers)

    def post_in_mem(url, params, json, headers, **kwargs):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return app.post(url, query_string=params, json=json, headers=headers)

    def put_in_mem(url, params, json, headers, **kwargs):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return app.put(url, query_string=params, json=json, headers=headers)

    monkeypatch.setattr(requests, "get", get_in_mem)
    monkeypatch.setattr(requests, "post", post_in_mem)
    monkeypatch.setattr(requests, "put", put_in_mem)
    monkeypatch.setattr(requester, "get_content", get_test_content)
