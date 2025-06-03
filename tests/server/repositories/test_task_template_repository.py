from typing import Dict, List, Optional, Union
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session as SQLAlchemySession

# Import models from your application
from jobmon.server.web.models.arg import Arg
from jobmon.server.web.models.dag import Dag
from jobmon.server.web.models.node import Node
from jobmon.server.web.models.node_arg import NodeArg
from jobmon.server.web.models.task import Task
from jobmon.server.web.models.task_instance import TaskInstance
from jobmon.server.web.models.task_instance_status import TaskInstanceStatus
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.task_template import TaskTemplate
from jobmon.server.web.models.task_template_version import TaskTemplateVersion
from jobmon.server.web.models.tool import Tool
from jobmon.server.web.models.tool_version import ToolVersion
from jobmon.server.web.models.workflow import Workflow
from jobmon.server.web.models.workflow_run import WorkflowRun

# Import the repository to be tested
from jobmon.server.web.repositories.task_template_repository import (
    TaskTemplateRepository,
)

# Import schemas from your application
from jobmon.server.web.schemas.task_template import (  # If RequestedResourcesModel is indeed used or intended to be, ensure it's imported; RequestedResourcesModel,
    TaskResourceDetailItem,
    TaskTemplateResourceUsageRequest,
)

# Mock logger to prevent actual logging during tests if desired, or to assert calls
# For now, we'll let it log if the code does, but you could patch it:
# from jobmon.server.web.repositories import task_template_repository as ttr_module
# logger_mock = MagicMock()
# ttr_module.logger = logger_mock


@pytest.fixture
def mock_session() -> MagicMock:
    """Fixture for a mock SQLAlchemy session."""
    session = MagicMock()
    # session.execute.return_value.all.return_value = [] # Default to no results
    return session


@pytest.fixture  # Changed from function scope to be more flexible or overridable if needed
def task_template_repo(mock_session: MagicMock) -> TaskTemplateRepository:
    """Fixture for TaskTemplateRepository with a mock session."""
    return TaskTemplateRepository(session=mock_session)


# Fixture for TaskTemplateRepository with a real dbsession
@pytest.fixture
def task_template_repo_real_session(
    dbsession: SQLAlchemySession,
) -> TaskTemplateRepository:
    return TaskTemplateRepository(session=dbsession)


@pytest.fixture
def resource_details_db_data(dbsession: SQLAlchemySession) -> dict:
    """Fixture to populate the database with data for resource detail tests."""
    # Tool
    tool = Tool(name="test_tool")
    dbsession.add(tool)
    dbsession.commit()

    # ToolVersion
    tool_version = ToolVersion(tool_id=tool.id)
    dbsession.add(tool_version)
    dbsession.commit()

    # Task Template
    task_template = TaskTemplate(name="test_tt", tool_version_id=tool_version.id)
    dbsession.add(task_template)
    dbsession.commit()

    # Task Template Version
    ttv1 = TaskTemplateVersion(
        task_template_id=task_template.id,
        command_template="echo hello",
        arg_mapping_hash="dummy_hash_123",
    )
    ttv2 = TaskTemplateVersion(  # For testing with multiple TTVs if needed later
        task_template_id=task_template.id,
        command_template="echo world",
        arg_mapping_hash="dummy_hash_456",
    )
    dbsession.add_all([ttv1, ttv2])
    dbsession.commit()

    # Nodes for ttv1
    node1_ttv1 = Node(
        task_template_version_id=ttv1.id,
        node_args_hash="node_hash_ttv1_1",
    )
    node2_ttv1 = Node(
        task_template_version_id=ttv1.id,
        node_args_hash="node_hash_ttv1_2",
    )
    # Node for ttv2 (for future isolation tests or different scenarios)
    node1_ttv2 = Node(
        task_template_version_id=ttv2.id,
        node_args_hash="node_hash_ttv2_1",
    )
    dbsession.add_all([node1_ttv1, node2_ttv1, node1_ttv2])
    dbsession.commit()

    # Args for node_args filtering
    arg1 = Arg(name="country")
    arg2 = Arg(name="city")
    dbsession.add_all([arg1, arg2])
    dbsession.commit()

    # NodeArgs for node1_ttv1
    node_arg1_n1 = NodeArg(node_id=node1_ttv1.id, arg_id=arg1.id, val="USA")
    node_arg2_n1 = NodeArg(node_id=node1_ttv1.id, arg_id=arg2.id, val="NewYork")
    # NodeArgs for node2_ttv1
    node_arg1_n2 = NodeArg(node_id=node2_ttv1.id, arg_id=arg1.id, val="UK")
    node_arg2_n2 = NodeArg(node_id=node2_ttv1.id, arg_id=arg2.id, val="London")
    dbsession.add_all([node_arg1_n1, node_arg2_n1, node_arg1_n2, node_arg2_n2])
    dbsession.commit()

    # Dag
    dag1 = Dag(hash="dag_hash_1")
    dag2 = Dag(hash="dag_hash_2")  # For workflow filtering
    dbsession.add_all([dag1, dag2])
    dbsession.commit()

    # Workflows
    workflow1 = Workflow(
        tool_version_id=tool_version.id,
        dag_id=dag1.id,
        workflow_args_hash="wf_args_hash_1",
        task_hash="task_hash_wf1",
        name="wf1",
        max_concurrently_running=5,
    )
    workflow2 = Workflow(  # For workflow filtering
        tool_version_id=tool_version.id,
        dag_id=dag2.id,
        workflow_args_hash="wf_args_hash_2",
        task_hash="task_hash_wf2",
        name="wf2",
        max_concurrently_running=5,
    )
    dbsession.add_all([workflow1, workflow2])
    dbsession.commit()

    # TaskResources must be defined before Tasks that reference them
    tr1_mem1g = TaskResources(
        requested_resources='{"memory": "1g", "cores": 1}',
        task_resources_type_id="O",
    )
    tr2_mem2g = TaskResources(
        requested_resources='{"memory": "2g", "cores": 2}',
        task_resources_type_id="O",
    )
    tr3_mem05g = TaskResources(
        requested_resources='{"memory": "0.5g"}', task_resources_type_id="O"
    )
    tr4_mem02g = TaskResources(
        requested_resources='{"memory": "0.2g"}', task_resources_type_id="O"
    )
    tr5_mem3g_ttv2 = TaskResources(
        requested_resources='{"memory": "3g"}', task_resources_type_id="O"
    )
    dbsession.add_all([tr1_mem1g, tr2_mem2g, tr3_mem05g, tr4_mem02g, tr5_mem3g_ttv2])
    dbsession.commit()

    # Tasks (associated with nodes of ttv1)
    task1_n1_ttv1 = Task(
        node_id=node1_ttv1.id,
        workflow_id=workflow1.id,  # Explicitly set workflow_id
        task_resources_id=tr1_mem1g.id,  # Link to TaskResources
        status="D",
        name="task100",
        command="cmd100",
        max_attempts=3,
        task_args_hash="task_hash_100",
    )
    task2_n2_ttv1 = Task(
        node_id=node2_ttv1.id,
        workflow_id=workflow2.id,  # Explicitly set workflow_id (or workflow1 if intended for same wf)
        task_resources_id=tr2_mem2g.id,  # Link to TaskResources
        status="D",
        name="task101",
        command="cmd101",
        max_attempts=2,
        task_args_hash="task_hash_101",
    )
    task_with_only_running_ti = Task(
        node_id=node1_ttv1.id,
        workflow_id=workflow1.id,  # Explicitly set workflow_id
        task_resources_id=tr4_mem02g.id,  # Link to appropriate TaskResources
        status="R",
        name="task102_running",
        command="cmd102",
        max_attempts=1,
        task_args_hash="task_hash_102",
    )
    # Task for node1_ttv2 (should not appear in ttv1 results)
    task1_n1_ttv2 = Task(
        node_id=node1_ttv2.id,
        workflow_id=workflow1.id,  # Explicitly set workflow_id (adjust if needed for test logic)
        task_resources_id=tr5_mem3g_ttv2.id,  # Link to TaskResources
        status="D",
        name="task103_ttv2",
        command="cmd103",
        max_attempts=1,
        task_args_hash="task_hash_103",
    )
    dbsession.add_all(
        [task1_n1_ttv1, task2_n2_ttv1, task_with_only_running_ti, task1_n1_ttv2]
    )
    dbsession.commit()

    # WorkflowRuns
    wfr1_wf1 = WorkflowRun(workflow_id=workflow1.id, user="user1", status="D")
    wfr2_wf2 = WorkflowRun(workflow_id=workflow2.id, user="user2", status="D")
    wfr3_wf1_done = WorkflowRun(workflow_id=workflow1.id, user="user1", status="G")
    dbsession.add_all([wfr1_wf1, wfr2_wf2, wfr3_wf1_done])
    dbsession.commit()

    # TaskInstances
    # For task1_n1_ttv1 (id=100)
    ti1_task100_attempt1_done = TaskInstance(
        task_id=task1_n1_ttv1.id,
        workflow_run_id=wfr1_wf1.id,
        status=TaskInstanceStatus.DONE,
        wallclock=120.5,
        maxrss=2048,
        task_resources_id=tr1_mem1g.id,
        process_group_id=1,
        array_id=1,
        array_step_id=1,
        array_batch_num=1,
    )
    ti2_task100_attempt2_error = TaskInstance(
        task_id=task1_n1_ttv1.id,
        workflow_run_id=wfr1_wf1.id,
        status=TaskInstanceStatus.ERROR,
        wallclock=50.0,
        maxrss=1000,
        task_resources_id=tr1_mem1g.id,
        process_group_id=2,
        array_id=2,
        array_step_id=1,
        array_batch_num=1,
    )
    ti3_task100_attempt3_done_wfr3 = TaskInstance(
        task_id=task1_n1_ttv1.id,
        workflow_run_id=wfr3_wf1_done.id,
        status=TaskInstanceStatus.DONE,
        wallclock=150.0,
        maxrss=2560,
        task_resources_id=tr1_mem1g.id,
        process_group_id=3,
        array_id=3,
        array_step_id=1,
        array_batch_num=1,
    )

    # For task2_n2_ttv1 (id=101)
    ti4_task101_attempt1_done_wfr2 = TaskInstance(
        task_id=task2_n2_ttv1.id,
        workflow_run_id=wfr2_wf2.id,
        status=TaskInstanceStatus.DONE,
        wallclock=60.0,
        maxrss=1024,
        task_resources_id=tr2_mem2g.id,
        process_group_id=4,
        array_id=4,
        array_step_id=1,
        array_batch_num=1,
    )
    ti5_task101_attempt2_resource_error_wfr2 = TaskInstance(
        task_id=task2_n2_ttv1.id,
        workflow_run_id=wfr2_wf2.id,
        status=TaskInstanceStatus.RESOURCE_ERROR,
        wallclock=30.0,
        maxrss=512,
        task_resources_id=tr2_mem2g.id,
        process_group_id=5,
        array_id=5,
        array_step_id=1,
        array_batch_num=1,
    )

    # For task_with_only_running_ti (id=102) - should NOT be picked up
    ti_running_task102 = TaskInstance(
        task_id=task_with_only_running_ti.id,
        workflow_run_id=wfr1_wf1.id,
        status=TaskInstanceStatus.RUNNING,
        wallclock=10.0,
        maxrss=512,
        task_resources_id=tr4_mem02g.id,
        process_group_id=6,
        array_id=6,
        array_step_id=1,
        array_batch_num=1,
    )

    # TI for task on ttv2 (should not appear in ttv1 results)
    ti_task103_ttv2_wfr1 = TaskInstance(
        task_id=task1_n1_ttv2.id,
        workflow_run_id=wfr1_wf1.id,
        status=TaskInstanceStatus.DONE,
        wallclock=30.0,
        maxrss=500,
        task_resources_id=tr5_mem3g_ttv2.id,
        process_group_id=7,
        array_id=7,
        array_step_id=1,
        array_batch_num=1,
    )

    # TI for task1_n1_ttv1 which is fatal, should be picked up
    ti6_task100_attempt4_fatal = TaskInstance(
        task_id=task1_n1_ttv1.id,
        workflow_run_id=wfr1_wf1.id,
        status=TaskInstanceStatus.ERROR_FATAL,
        wallclock=10.0,
        maxrss=300,
        task_resources_id=tr1_mem1g.id,
        process_group_id=8,
        array_id=8,
        array_step_id=1,
        array_batch_num=1,
    )

    dbsession.add_all(
        [
            ti1_task100_attempt1_done,
            ti2_task100_attempt2_error,
            ti3_task100_attempt3_done_wfr3,
            ti4_task101_attempt1_done_wfr2,
            ti5_task101_attempt2_resource_error_wfr2,
            ti_running_task102,
            ti_task103_ttv2_wfr1,
            ti6_task100_attempt4_fatal,
        ]
    )
    dbsession.commit()

    return {
        "ttv1": ttv1,
        "ttv2": ttv2,
        "node1_ttv1": node1_ttv1,
        "node2_ttv1": node2_ttv1,
        "task1_n1_ttv1": task1_n1_ttv1,
        "task2_n2_ttv1": task2_n2_ttv1,
        "workflow1": workflow1,
        "workflow2": workflow2,
        "arg_country": arg1,
        "arg_city": arg2,
        "tr1_req_res": tr1_mem1g.requested_resources,
        "tr2_req_res": tr2_mem2g.requested_resources,
        "expected_task_details_ttv1_no_filter": [
            TaskResourceDetailItem(
                r=120.5,
                m=2048,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=1,
            ),
            TaskResourceDetailItem(
                r=50.0,
                m=1000,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=2,
            ),
            TaskResourceDetailItem(
                r=150.0,
                m=2560,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=3,
            ),
            TaskResourceDetailItem(
                r=10.0,
                m=300,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=4,
            ),
            TaskResourceDetailItem(
                r=60.0,
                m=1024,
                node_id=node2_ttv1.id,
                task_id=task2_n2_ttv1.id,
                requested_resources=tr2_mem2g.requested_resources,
                attempt_number_of_instance=1,
            ),
            TaskResourceDetailItem(
                r=30.0,
                m=512,
                node_id=node2_ttv1.id,
                task_id=task2_n2_ttv1.id,
                requested_resources=tr2_mem2g.requested_resources,
                attempt_number_of_instance=2,
            ),
        ],
        "expected_task_details_ttv1_wf1_filter": [
            TaskResourceDetailItem(
                r=120.5,
                m=2048,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=1,
            ),
            TaskResourceDetailItem(
                r=50.0,
                m=1000,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=2,
            ),
            TaskResourceDetailItem(
                r=150.0,
                m=2560,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=3,
            ),
            TaskResourceDetailItem(
                r=10.0,
                m=300,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=4,
            ),
        ],
        "expected_task_details_ttv1_node_args_USA_NewYork": [
            TaskResourceDetailItem(
                r=120.5,
                m=2048,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=1,
            ),
            TaskResourceDetailItem(
                r=50.0,
                m=1000,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=2,
            ),
            TaskResourceDetailItem(
                r=150.0,
                m=2560,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=3,
            ),
            TaskResourceDetailItem(
                r=10.0,
                m=300,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=4,
            ),
        ],
        "expected_task_details_ttv1_wf1_and_node_args_USA_NewYork": [
            TaskResourceDetailItem(
                r=120.5,
                m=2048,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=1,
            ),
            TaskResourceDetailItem(
                r=50.0,
                m=1000,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=2,
            ),
            TaskResourceDetailItem(
                r=150.0,
                m=2560,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=3,
            ),
            TaskResourceDetailItem(
                r=10.0,
                m=300,
                node_id=node1_ttv1.id,
                task_id=task1_n1_ttv1.id,
                requested_resources=tr1_mem1g.requested_resources,
                attempt_number_of_instance=4,
            ),
        ],
        "expected_task_details_ttv1_node_args_UK_London": [
            TaskResourceDetailItem(
                r=60.0,
                m=1024,
                node_id=node2_ttv1.id,
                task_id=task2_n2_ttv1.id,
                requested_resources=tr2_mem2g.requested_resources,
                attempt_number_of_instance=1,
            ),
            TaskResourceDetailItem(
                r=30.0,
                m=512,
                node_id=node2_ttv1.id,
                task_id=task2_n2_ttv1.id,
                requested_resources=tr2_mem2g.requested_resources,
                attempt_number_of_instance=2,
            ),
        ],
    }


class TestGetTaskResourceDetails:

    def test_basic_fetch_no_filters(
        self,
        task_template_repo_real_session: TaskTemplateRepository,
        resource_details_db_data: dict,
    ) -> None:
        ttv1 = resource_details_db_data["ttv1"]
        expected_results = resource_details_db_data[
            "expected_task_details_ttv1_no_filter"
        ]

        # Call the repository method
        repo = task_template_repo_real_session
        result = repo.get_task_resource_details(
            task_template_version_id=ttv1.id, workflows=None, node_args=None
        )

        # Assert the results
        assert len(result) == len(expected_results)
        # Sort results by a consistent key for comparison
        result_sorted = sorted(
            result,
            key=lambda x: (x.task_id, x.attempt_number_of_instance or 0, x.r or 0),
        )
        expected_sorted = sorted(
            expected_results,
            key=lambda x: (x.task_id, x.attempt_number_of_instance or 0, x.r or 0),
        )

        for res_item, exp_item in zip(result_sorted, expected_sorted):
            assert res_item.r == exp_item.r
            assert res_item.m == exp_item.m
            assert res_item.node_id == exp_item.node_id
            assert res_item.task_id == exp_item.task_id
            assert res_item.requested_resources == exp_item.requested_resources
            assert (
                res_item.attempt_number_of_instance
                == exp_item.attempt_number_of_instance
            )

    @pytest.mark.parametrize(
        "filter_type, workflow_id_key, node_args_filter, expected_key",
        [
            (
                "workflow_filter",
                "workflow1",  # Key to get workflow1 from fixture
                None,
                "expected_task_details_ttv1_wf1_filter",
            ),
            (
                "node_args_filter_usa_ny",
                None,
                {"country": ["USA"], "city": ["NewYork"]},
                "expected_task_details_ttv1_node_args_USA_NewYork",
            ),
            (
                "node_args_filter_uk_london",
                None,
                {"country": ["UK"], "city": ["London"]},
                "expected_task_details_ttv1_node_args_UK_London",
            ),
            (
                "workflow_and_node_args_filter",
                "workflow1",  # Key to get workflow1 from fixture
                {"country": ["USA"], "city": ["NewYork"]},
                "expected_task_details_ttv1_wf1_and_node_args_USA_NewYork",
            ),
            (
                "workflow_filter_no_match",
                999,  # Non-existent workflow ID (literal value)
                None,
                [],  # Expect empty list
            ),
            (
                "node_args_filter_no_match_value",
                None,
                {"country": ["Canada"]},  # No nodes with this country
                [],  # Expect empty list
            ),
            (
                "node_args_filter_no_match_arg_name",
                None,
                {"non_existent_arg": ["value"]},
                [],  # Expect empty list
            ),
            (
                "empty_node_args_dict",  # Test with an empty dict for node_args
                None,
                {},
                "expected_task_details_ttv1_no_filter",  # Should behave like no filter
            ),
            (
                "node_args_partial_match_one_arg_exists_one_not",
                None,
                {
                    "country": ["USA"],
                    "non_existent_arg": ["NewYork"],
                },  # country matches, but non_existent_arg does not
                [],  # Expect empty: current logic is AND for multiple node_args keys in the filter
            ),
        ],
    )
    def test_get_task_resource_details_with_filters(
        self,
        task_template_repo_real_session: TaskTemplateRepository,
        resource_details_db_data: dict,
        filter_type: str,
        workflow_id_key: Union[str, int, None],
        node_args_filter: Optional[Dict[str, List[str]]],
        expected_key: Union[str, list],  # Can be a key or an empty list
    ) -> None:
        ttv1 = resource_details_db_data["ttv1"]

        # Resolve workflow IDs dynamically
        if workflow_id_key is None:
            workflow_ids = None
        elif isinstance(workflow_id_key, str):
            # Get workflow ID from fixture data
            workflow = resource_details_db_data[workflow_id_key]
            workflow_ids = [workflow.id]
        else:
            # Use literal value (like 999 for non-existent workflow)
            workflow_ids = [workflow_id_key]

        if isinstance(expected_key, str):
            expected_results = resource_details_db_data[expected_key]
        else:  # it's an empty list for no match scenarios
            expected_results = expected_key

        repo = task_template_repo_real_session
        result = repo.get_task_resource_details(
            task_template_version_id=ttv1.id,
            workflows=workflow_ids,
            node_args=node_args_filter,
        )

        assert len(result) == len(
            expected_results
        ), f"Failed for {filter_type}: expected {len(expected_results)} results, got {len(result)}"
        result_sorted = sorted(
            result,
            key=lambda x: (x.task_id, x.attempt_number_of_instance or 0, x.r or 0),
        )
        expected_sorted = sorted(
            expected_results,
            key=lambda x: (x.task_id, x.attempt_number_of_instance or 0, x.r or 0),
        )

        for res_item, exp_item in zip(result_sorted, expected_sorted):
            assert res_item.r == exp_item.r
            assert res_item.m == exp_item.m
            assert res_item.node_id == exp_item.node_id
            assert res_item.task_id == exp_item.task_id
            assert res_item.requested_resources == exp_item.requested_resources
            assert (
                res_item.attempt_number_of_instance
                == exp_item.attempt_number_of_instance
            )


# TestCalculateResourceStatistics class and its tests are removed.
# TestGetTaskTemplateResourceUsage class and its tests are removed.

# Add more focused integration tests here as needed.
