import threading
import time

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from jobmon.client.array import Array
from jobmon.client.tool import Tool
from jobmon.client.workflow_run import WorkflowRunFactory


@pytest.fixture
def tool(client_env):
    tool = Tool()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    return tool


@pytest.fixture
def task_template(tool):
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


@pytest.fixture
def task_template_dummy(tool):
    tt = tool.get_task_template(
        template_name="dummy_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )
    return tt


def test_create_array(task_template):
    tasks = task_template.create_tasks(arg="echo 1")
    array = tasks[0].array
    assert (
        array.compute_resources
        == task_template.default_compute_resources_set["sequential"]
    )
    # test assigned name
    assert "simple_template" in array.name
    # test given name
    tasks = task_template.create_tasks(name="test_array", arg="echo 2")
    array = tasks[0].array
    assert "test_array" in array.name


def test_array_bind(db_engine, client_env, task_template_dummy, tool):
    task_template = task_template_dummy

    tasks = task_template.create_tasks(
        arg="echo 10", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    for t in tasks:
        assert t.task_id is not None

    assert hasattr(wf.arrays["dummy_template"], "_array_id")

    # Check that array is set
    array_id = wf.arrays["dummy_template"].array_id
    assert array_id is not None
    # get the array using the db engine
    with Session(db_engine) as session:
        query = text("SELECT * FROM array WHERE id = :array_id")
        result = session.execute(query, {"array_id": array_id})
        r = result.fetchall()
        assert len(r) == 1


def test_array_bind_cluster_override(db_engine, client_env, task_template_dummy, tool):
    """Test that we can override the cluster after tasks are created."""
    task_template = task_template_dummy

    tasks = task_template.create_tasks(
        arg="echo 10", compute_resources={"queue": "null.q"}
    )

    # override the cluster
    for t in tasks:
        t.cluster_name = "sequential"

    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    for t in tasks:
        assert t.task_id is not None

    assert hasattr(wf.arrays["dummy_template"], "_array_id")


def test_node_args_expansion():
    node_args = {"location_id": [1, 2, 3], "sex": ["m", "f"]}

    expanded_node_args = Array.expand_node_args(**node_args)

    assert len(expanded_node_args) == 6

    assert {"location_id": 1, "sex": "m"} in expanded_node_args
    assert {"location_id": 1, "sex": "f"} in expanded_node_args
    assert {"location_id": 2, "sex": "m"} in expanded_node_args
    assert {"location_id": 2, "sex": "f"} in expanded_node_args
    assert {"location_id": 3, "sex": "m"} in expanded_node_args
    assert {"location_id": 3, "sex": "f"} in expanded_node_args

    node_args = {"location_id": [1], "sex": ["m", "f"]}

    expanded_node_args = Array.expand_node_args(**node_args)

    assert len(expanded_node_args) == 2
    assert {"location_id": 1, "sex": "m"} in expanded_node_args
    assert {"location_id": 1, "sex": "f"} in expanded_node_args


def test_create_tasks(db_engine, client_env, tool):
    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    # setting of default resources is done by the tool
    tasks = rich_task_template.create_tasks(
        arg="echo 1", compute_resources={"queue": "null.q"}
    )
    array = tasks[0].array

    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    wf._bind_tasks()
    for t in tasks:
        assert t.task_id is not None
    assert array.array_id is not None

    # get the task using the db engine
    with Session(db_engine) as session:
        query = text("SELECT * FROM task WHERE id = :task_id")
        result = session.execute(query, {"task_id": tasks[0].task_id})
        r = result.fetchall()
        assert len(r) == 1

    # get the array using the db engine
    with Session(db_engine) as session:
        query = text("SELECT * FROM array WHERE id = :array_id")
        result = session.execute(query, {"array_id": array.array_id})
        r = result.fetchall()
        assert len(r) == 1


def test_create_tasks_with_range(db_engine, client_env, tool):
    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg1} {arg2}",
        node_args=["arg1", "arg2"],
        task_args=[],
        op_args=[],
    )
    # setting of default resources is done by the tool
    tasks = rich_task_template.create_tasks(
        arg1=range(0, 5), arg2=["tom"], compute_resources={"queue": "null.q"}
    )
    assert len(tasks) == 5


def test_create_tasks_with_lists(db_engine, client_env, tool):
    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg1} {arg2}",
        node_args=["arg1", "arg2"],
        task_args=[],
        op_args=[],
    )
    # setting of default resources is done by the tool
    tasks = rich_task_template.create_tasks(
        arg1=[0, 1, 2, 3, 4],
        arg2=["tom", "jerry"],
        compute_resources={"queue": "null.q"},
    )
    assert len(tasks) == 10


def test_create_tasks_with_tuples(db_engine, client_env, tool):
    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg1} {arg2}",
        node_args=["arg1", "arg2"],
        task_args=[],
        op_args=[],
    )
    # setting of default resources is done by the tool
    tasks = rich_task_template.create_tasks(
        arg1=(0, 1, 2, 3, 4), arg2=["tom"], compute_resources={"queue": "null.q"}
    )
    assert len(tasks) == 5


def test_empty_array(client_env, tool):
    """Check that an empty array raises the appropriate error."""

    task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )

    with pytest.raises(ValueError, match="Cannot create an array with 0 tasks"):
        task_template.create_tasks(arg=[], compute_resources={"queue": "null.q"})


def test_array_max_attempts(client_env, tool):
    """Check the wf default_max_attempts pass down"""
    tt = tool.get_task_template(
        template_name="simple_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    tasks1 = tt.create_tasks(arg=[1, 2])
    wf1 = tool.create_workflow()
    wf1.add_tasks(tasks1)
    array1 = tasks1[0].array
    assert array1.max_attempts is None

    # no default max attempts
    tasks2 = tt.create_tasks(arg=[10, 20])
    wf2 = tool.create_workflow(default_max_attempts=1000)
    wf2.add_tasks(tasks2)
    array2 = tasks2[0].array
    assert (
        array2.max_attempts == 1000
    ), f"Expected array max_attempts to be 1000, got {array2.max_attempts}"


def test_queue_task_batch_deadlock_prevention(db_engine, tool):
    """Test that concurrent queue_task_batch requests don't cause deadlocks."""
    from jobmon.core.constants import TaskStatus

    # Create a task template and workflow
    tt = tool.get_task_template(
        template_name="deadlock_test_template",
        command_template="echo {arg} && echo jerry",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    # Create multiple tasks for the same array
    tasks = tt.create_tasks(arg=[1, 2, 3, 4, 5], compute_resources={"queue": "null.q"})
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    wf._bind_tasks()

    # Get the array ID and create a workflow run
    array_id = tasks[0].array.array_id
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    workflow_run_id = wfr.workflow_run_id
    task_resources_id = 1  # Default task resources ID

    # Prepare the request data
    request_data = {
        "task_ids": [task.task_id for task in tasks],
        "task_resources_id": task_resources_id,
        "workflow_run_id": workflow_run_id,
    }

    # Results storage
    results = []
    errors = []

    def make_request(thread_id):
        """Make a concurrent request to queue_task_batch using wf.requester."""
        try:
            return_code, msg = wf.requester.send_request(
                app_route=f"/array/{array_id}/queue_task_batch",
                message=request_data,
                request_type="post",
            )
            results.append(
                {"thread_id": thread_id, "status_code": return_code, "response": msg}
            )
        except Exception as e:
            errors.append({"thread_id": thread_id, "error": str(e)})

    # Create and start 2 threads (as requested)
    threads = []
    num_threads = 2

    for i in range(num_threads):
        thread = threading.Thread(target=make_request, args=(i,))
        threads.append(thread)

    # Start all threads simultaneously
    for thread in threads:
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Verify results
    assert (
        len(results) == num_threads
    ), f"Expected {num_threads} successful requests, got {len(results)}"
    assert len(errors) == 0, f"Expected no errors, got: {errors}"

    # All requests should succeed (no deadlocks)
    for result in results:
        assert (
            result["status_code"] == 200
        ), f"Expected status 200, got {result['status_code']}"

    # Verify that tasks were queued and task instances were created
    with Session(db_engine) as session:
        # Check task status
        for task in tasks:
            query = text("SELECT status FROM task WHERE id = :task_id")
            result = session.execute(query, {"task_id": task.task_id})
            status = result.scalar()
            assert status == TaskStatus.QUEUED, f"Expected task status 'Q', got {status}"

        # Check task instances were created
        query = text(
            "SELECT COUNT(*) FROM task_instance WHERE workflow_run_id = :workflow_run_id"
        )
        result = session.execute(query, {"workflow_run_id": workflow_run_id})
        count = result.scalar()
        assert (
            count == len(tasks)
        ), f"Expected {len(tasks)} task instances, got {count}"


def test_concurrent_array_operations_simulation():
    """Test that simulates concurrent array operations to verify deadlock prevention logic."""
    import threading
    from collections import defaultdict
    from unittest.mock import MagicMock

    # Mock the database locks
    locks = defaultdict(threading.Lock)

    def mock_with_for_update(query, array_id):
        """Simulate acquiring a lock on array_batch_num."""
        with locks[array_id]:
            # Simulate some work
            time.sleep(0.001)
            return 1

    # Simulate multiple concurrent operations on the same array
    results = []
    errors = []

    def simulate_queue_batch(thread_id, array_id):
        try:
            # Simulate acquiring lock for batch number
            batch_num = mock_with_for_update(None, array_id)
            results.append({"thread_id": thread_id, "batch_num": batch_num})
        except Exception as e:
            errors.append({"thread_id": thread_id, "error": str(e)})

    # Create threads
    threads = []
    num_threads = 5
    array_id = 1

    for i in range(num_threads):
        thread = threading.Thread(target=simulate_queue_batch, args=(i, array_id))
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for completion
    for thread in threads:
        thread.join()

    # Verify no errors occurred
    assert len(errors) == 0, f"Expected no errors, got: {errors}"
    assert len(results) == num_threads


def test_queue_task_batch_and_log_done_concurrently(db_engine, tool):
    """Call queue_task_batch and log_done concurrently to ensure no deadlocks.

    This test ensures that operations on different tables (Task and TaskInstance)
    don't cause deadlocks when executed concurrently.
    """
    from jobmon.core.constants import TaskStatus

    # Create a task template and workflow
    tt = tool.get_task_template(
        template_name="concurrent_ops_test",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    # Create tasks
    tasks = tt.create_tasks(arg=[1, 2, 3, 4, 5], compute_resources={"queue": "null.q"})
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    wf._bind_tasks()

    # Get the array ID and create a workflow run
    array_id = tasks[0].array.array_id
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    workflow_run_id = wfr.workflow_run_id
    task_resources_id = 1

    # Prepare the request data
    request_data = {
        "task_ids": [task.task_id for task in tasks],
        "task_resources_id": task_resources_id,
        "workflow_run_id": workflow_run_id,
    }

    # Results storage
    results = []
    errors = []

    def queue_batch(thread_id):
        """Queue the task batch."""
        try:
            return_code, msg = wf.requester.send_request(
                app_route=f"/array/{array_id}/queue_task_batch",
                message=request_data,
                request_type="post",
            )
            results.append(
                {
                    "thread_id": thread_id,
                    "operation": "queue",
                    "status_code": return_code,
                }
            )
        except Exception as e:
            errors.append({"thread_id": thread_id, "operation": "queue", "error": str(e)})

    def read_status(thread_id):
        """Read task status concurrently."""
        try:
            with Session(db_engine) as session:
                for task in tasks:
                    query = text("SELECT status FROM task WHERE id = :task_id")
                    session.execute(query, {"task_id": task.task_id})
            results.append(
                {"thread_id": thread_id, "operation": "read", "status_code": 200}
            )
        except Exception as e:
            errors.append({"thread_id": thread_id, "operation": "read", "error": str(e)})

    # Create threads
    threads = []
    threads.append(threading.Thread(target=queue_batch, args=(0,)))
    threads.append(threading.Thread(target=read_status, args=(1,)))
    threads.append(threading.Thread(target=queue_batch, args=(2,)))

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for completion
    for thread in threads:
        thread.join()

    # Verify no errors occurred
    assert len(errors) == 0, f"Expected no errors, got: {errors}"
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"


def test_queue_task_batch_returns_status_for_already_queued_tasks(db_engine, tool):
    """Test that queue_task_batch returns task status even when tasks are already QUEUED.
    
    This tests the fix for the production bug where workflows would stop mid-execution
    because queue_task_batch returned an empty response when tasks were already in QUEUED status.
    
    Steps:
    1. Create a workflow with array tasks (not running it)
    2. Directly update tasks' status to 'Q' (QUEUED) in the database
    3. Call queue_task_batch route
    4. Verify it returns tasks with their current status (not empty response)
    """
    from jobmon.core.constants import TaskStatus

    # Create a task template and workflow (but don't run it)
    tt = tool.get_task_template(
        template_name="already_queued_test",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    # Create tasks
    tasks = tt.create_tasks(arg=[1, 2, 3, 4, 5], compute_resources={"queue": "null.q"})
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    wf._bind_tasks()

    # Get the array ID and create a workflow run
    array_id = tasks[0].array.array_id
    factory = WorkflowRunFactory(wf.workflow_id)
    wfr = factory.create_workflow_run()
    workflow_run_id = wfr.workflow_run_id
    task_resources_id = 1

    # Directly update tasks to QUEUED status in the database
    with Session(db_engine) as session:
        for task in tasks:
            update_query = text(
                "UPDATE task SET status = :status WHERE id = :task_id"
            )
            session.execute(
                update_query, {"status": TaskStatus.QUEUED, "task_id": task.task_id}
            )
        session.commit()

    # Verify tasks are now in QUEUED status
    with Session(db_engine) as session:
        for task in tasks:
            query = text("SELECT status FROM task WHERE id = :task_id")
            result = session.execute(query, {"task_id": task.task_id})
            status = result.scalar()
            assert status == TaskStatus.QUEUED, f"Expected status 'Q', got {status}"

    # Call queue_task_batch route
    request_data = {
        "task_ids": [task.task_id for task in tasks],
        "task_resources_id": task_resources_id,
        "workflow_run_id": workflow_run_id,
    }

    return_code, response = wf.requester.send_request(
        app_route=f"/array/{array_id}/queue_task_batch",
        message=request_data,
        request_type="post",
    )

    # Verify response is not empty and contains task status
    assert return_code == 200, f"Expected status code 200, got {return_code}"
    assert "tasks_by_status" in response, "Response should contain 'tasks_by_status'"
    
    tasks_by_status = response["tasks_by_status"]
    
    # The critical assertion: response should NOT be empty
    assert len(tasks_by_status) > 0, (
        "BUG: queue_task_batch returned empty response for already-queued tasks! "
        "This would cause workflows to stop mid-execution."
    )
    
    # Verify all tasks are returned with QUEUED status
    assert TaskStatus.QUEUED in tasks_by_status, (
        f"Expected QUEUED status in response, got statuses: {list(tasks_by_status.keys())}"
    )
    
    queued_task_ids = tasks_by_status.get(TaskStatus.QUEUED, [])
    assert len(queued_task_ids) == len(tasks), (
        f"Expected {len(tasks)} tasks in QUEUED status, got {len(queued_task_ids)}"
    )
    
    # Verify all task IDs are present
    expected_task_ids = set(task.task_id for task in tasks)
    returned_task_ids = set(queued_task_ids)
    assert returned_task_ids == expected_task_ids, (
        f"Returned task IDs don't match. Expected: {expected_task_ids}, Got: {returned_task_ids}"
    )
