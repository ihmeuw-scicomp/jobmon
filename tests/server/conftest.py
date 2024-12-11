import pytest


@pytest.fixture(scope="function")
def web_server_in_memory(db_engine):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    # The create_app call sets up database connections

    from jobmon.server.web.api import get_app
    from fastapi.testclient import TestClient

    app = get_app(versions=["v2"])
    client = TestClient(app)
    yield client, db_engine


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
def requester_in_memory(monkeypatch, web_server_in_memory, api_prefix):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests
    from jobmon.core import requester

    monkeypatch.setenv("JOBMON__HTTP__ROUTE_PREFIX", api_prefix)
    monkeypatch.setenv("JOBMON__HTTP__SERVICE_URL", "1")

    client, engine = web_server_in_memory

    def get_in_mem(url, params=None, data=None, headers=None, **kwargs):
        # Reformat the URL
        url = "/" + url.split(":")[-1].split("/", 1)[1]

        # FastAPI uses `params` for query strings
        return client.get(url, params=params, headers=headers)

    def post_in_mem(url, params=None, json=None, headers=None, **kwargs):
        # Reformat the URL
        url = "/" + url.split(":")[-1].split("/", 1)[1]

        # FastAPI uses `params` for query strings and `json` for JSON body
        return client.post(url, params=params, json=json, headers=headers)

    def put_in_mem(url, params=None, json=None, headers=None, **kwargs):
        # Reformat the URL
        url = "/" + url.split(":")[-1].split("/", 1)[1]

        # FastAPI uses `params` for query strings and `json` for JSON body
        return client.put(url, params=params, json=json, headers=headers)

    monkeypatch.setattr(requests, "get", get_in_mem)
    monkeypatch.setattr(requests, "post", post_in_mem)
    monkeypatch.setattr(requests, "put", put_in_mem)
    monkeypatch.setattr(requester, "get_content", get_test_content)
