"""This is the test the HTTP 400 errors.
"""

import pytest

from jobmon.core.exceptions import InvalidResponse
from jobmon.core.requester import Requester


def test_add_tool(requester_in_memory):
    # @jobmon_client.route('/tool', methods=['POST'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(app_route="/tool", message={}, request_type="post")
        assert "Unexpected status code 400" in str(error.value)


def test_get_tool_versions(requester_in_memory):
    # @jobmon_client.route('/tool/<tool_id>/tool_versions', methods=['GET'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/tool/abc/tool_versions", message={}, request_type="get"
        )
        assert "Unexpected status code 400" in str(error.value)


def test_add_tool_version(requester_in_memory):
    # @jobmon_client.route('/tool_version', methods=['POST'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/tool_version", message={"tool_id": "abc"}, request_type="post"
        )
        assert "Unexpected status code 400" in str(error.value)


def test_add_task_template(requester_in_memory):
    # @jobmon_client.route('/task_template', methods=['POST'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/task_template",
            message={"paramter_does_not_exist": "abc"},
            request_type="post",
        )
        assert "Unexpected status code 400" in str(error.value)

    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/task_template",
            message={"tool_version_id": "abc"},
            request_type="post",
        )
        assert "Unexpected status code 400" in str(error.value)


def test_add_task_template_version(requester_in_memory):
    # @jobmon_client.route('/task_template/<task_template_id>/add_version', methods=['POST'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/task_template/abc/add_version", message={}, request_type="post"
        )
        assert "Unexpected status code 400" in str(error.value)


def test_add_workflow(requester_in_memory):
    # @jobmon_client.route('/workflow', methods=['POST'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow", message={"dag_id": "abc"}, request_type="post"
        )
        assert "Unexpected status code 400" in str(error.value)

    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow",
            message={"tool_version_id": "abc"},
            request_type="post",
        )
        assert "Unexpected status code 400" in str(error.value)
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow",
            message={"workflow_args_hash": "abc"},
            request_type="post",
        )
        assert "Unexpected status code 400" in str(error.value)

    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow", message={"task_hash": "abc"}, request_type="post"
        )
        assert "Unexpected status code 400" in str(error.value)


def test_get_matching_workflows_by_workflow_args(requester_in_memory):
    # @jobmon_client.route('/workflow/<workflow_args_hash>', methods=['GET'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow/abcdefg", message={}, request_type="get"
        )
        assert "Unexpected status code 400" in str(error.value)


def test_workflow_attributes(requester_in_memory):
    # @jobmon_client.route('/workflow/<workflow_id>/workflow_attributes', methods=['PUT'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow/abc/workflow_attributes",
            message={},
            request_type="put",
        )
        assert "Unexpected status code 400" in str(error.value)


def test_add_workflow_rund(requester_in_memory):
    # @jobmon_client.route('/workflow_run', methods=['POST'])
    requester = Requester.from_defaults()
    with pytest.raises(InvalidResponse) as error:
        requester.send_request(
            app_route="/workflow_run", message={}, request_type="post"
        )
        assert "Unexpected status code 400" in str(error.value)
