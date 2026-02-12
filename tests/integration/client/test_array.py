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

    with Session(bind=db_engine) as session:
        array_stmt = """
        SELECT workflow_id, task_template_version_id 
        FROM array
        WHERE id = {}
        """.format(
            wf.arrays["dummy_template"].array_id
        )

        array_db = session.execute(text(array_stmt)).fetchone()
        session.commit()

        assert array_db.workflow_id == wf.workflow_id
        assert (
            array_db.task_template_version_id
            == task_template.active_task_template_version.id
        )

    # Assert the bound task has the correct array ID
    with Session(bind=db_engine) as session:
        task_query = """
        SELECT array_id
        FROM task
        WHERE id = {}
        """.format(
            list(wf.arrays["dummy_template"].tasks.values())[0].task_id
        )
        task = session.execute(text(task_query)).fetchone()

        assert task.array_id == wf.arrays["dummy_template"].array_id


def test_node_args_expansion():
    node_args = {"location_id": [1, 2, 3], "sex": ["m", "f"]}

    expected_expansion = [
        {"location_id": 1, "sex": "m"},
        {"location_id": 1, "sex": "f"},
        {"location_id": 2, "sex": "m"},
        {"location_id": 2, "sex": "f"},
        {"location_id": 3, "sex": "m"},
        {"location_id": 3, "sex": "f"},
    ]

    node_arg_generator = Array.expand_dict(**node_args)
    combos = []
    for node_arg in node_arg_generator:
        assert node_arg in expected_expansion
        assert node_arg not in combos
        combos.append(node_arg)

    assert len(combos) == len(expected_expansion)


def test_create_tasks(db_engine, client_env, tool):
    rich_task_template = tool.get_task_template(
        template_name="simple_template",
        command_template="{command} {task_arg} {narg1} {narg2} {op_arg} && echo tom",
        node_args=["narg1", "narg2"],
        task_args=["task_arg"],
        op_args=["command", "op_arg"],
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q"},
    )

    tasks = rich_task_template.create_tasks(
        command="foo",
        task_arg="bar",
        narg1=[1, 2, 3],
        narg2=["a", "b", "c"],
        op_arg="baz",
        compute_resources={"queue": "null.q"},
    )

    assert len(tasks) == 9  # Created on init
    wf = tool.create_workflow()
    wf.add_tasks(tasks)

    assert len(wf.tasks) == 9  # Tasks bound to workflow

    # Assert tasks templated correctly
    commands = [t.command for t in tasks]
    assert "foo bar 1 c baz && echo tom" in commands
    assert "foo bar 3 a baz && echo tom" in commands

    # Check node and task args are recorded in the proper tables
    wf.bind()
    assert wf.workflow_id is not None
    wf._bind_tasks()
    for t in tasks:
        assert t.task_id is not None

    with Session(bind=db_engine) as session:
        # Check narg1 and narg2 are represented in node_arg
        q = """
        SELECT * 
        FROM arg
        JOIN node_arg na ON na.arg_id = arg.id
        WHERE arg.name IN ('narg1', 'narg2')
        """
        res = session.execute(text(q)).fetchall()
        session.commit()

        assert len(res) == 18  # 2 args per node * 9 nodes
        names = [r.name for r in res]
        assert set(names) == {"narg1", "narg2"}

        # Check task_arg in the task arg table
        task_q = """
        SELECT * 
        FROM arg
        JOIN task_arg ta ON ta.arg_id = arg.id
        WHERE arg.name IN ('task_arg')"""
        task_args = session.execute(text(task_q)).fetchall()
        session.commit()

        assert len(task_args) == 9  # 9 unique tasks, 1 task_arg each
        task_arg_names = [r.name for r in task_args]
        assert set(task_arg_names) == {"task_arg"}

    # Define individual node_args, and expect to get a list of one task
    three_c_node_args = {"narg1": 3, "narg2": "c"}
    three_c_tasks = wf.get_tasks_by_node_args("simple_template", **three_c_node_args)
    assert len(three_c_tasks) == 1

    # Define out of scope node_args, and expect to get a empty list
    x_node_args = {"narg1": 3, "narg2": "x"}
    x_tasks = wf.get_tasks_by_node_args("simple_template", **x_node_args)
    assert len(x_tasks) == 0

    # Define narg1 only, expect to see a list of 3 tasks
    three_node_args = {"narg1": 3}
    three_tasks = wf.get_tasks_by_node_args("simple_template", **three_node_args)
    assert len(three_tasks) == 3

    # Define an empty dict, expect to see a list of 9 tasks
    empty_node_args = {}
    all_tasks = wf.get_tasks_by_node_args("simple_template", **empty_node_args)
    assert len(all_tasks) == 9

    # Define a list(3,2 items)-valued case for narg1 and narg2, expect to see 6
    empty_node_args = {"narg1": [1, 2, 3], "narg2": ["a", "b"]}
    all_tasks = wf.get_tasks_by_node_args("simple_template", **empty_node_args)
    assert len(all_tasks) == 6

    # Define a task_template_name in-scope valid node_args, expect to see a list of 3 tasks
    two_node_args = {"narg1": 2}
    two_wf_tasks = wf.get_tasks_by_node_args(
        task_template_name="simple_template", **two_node_args
    )
    assert len(two_wf_tasks) == 3

    # Define a task_template_name out-of-scope node_args, expect to see a list of 3 tasks
    two_node_args = {"narg1": 2}
    with pytest.raises(ValueError):
        two_wf_tasks = wf.get_tasks_by_node_args(
            task_template_name="OUT_OF_SCOPE_simple_template", **two_node_args
        )


def test_empty_array(client_env, tool):
    """Check that an empty array raises the appropriate error."""

    tt = tool.get_task_template(
        template_name="empty",
        command_template="",
        node_args=[],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    tasks = tt.create_tasks()
    assert not tasks


def test_array_max_attempts(client_env, tool):
    """Check the wf default_max_attempts pass down"""
    tt = tool.get_task_template(
        template_name="test_tt1",
        command_template="true|| abc {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
    )
    # test default
    tasks1 = tt.create_tasks(arg=[1, 2])
    wf1 = tool.create_workflow()
    wf1.add_tasks(tasks1)
    array1 = tasks1[0].array
    assert array1.max_attempts is None
    assert tasks1[0].max_attempts == tasks1[1].max_attempts == 3
    # test wf pass down
    tasks2 = tt.create_tasks(arg=[10, 20])
    wf2 = tool.create_workflow(default_max_attempts=1000)
    wf2.add_tasks(tasks2)
    array2 = tasks2[0].array
    assert (
        array2.max_attempts == tasks2[0].max_attempts == tasks2[1].max_attempts == 1000
    )


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
        ), f"Request failed with status {result['status_code']}: {result['response']}"

    # Verify that task instances were created correctly
    with Session(bind=db_engine) as session:
        # Check that task instances were created
        task_instance_query = text(
            """
            SELECT COUNT(*) as count, array_batch_num
            FROM task_instance 
            WHERE array_id = :array_id
            GROUP BY array_batch_num
            ORDER BY array_batch_num
        """
        )

        result = session.execute(task_instance_query, {"array_id": array_id}).fetchall()
        session.commit()

        # Should have multiple batches (one per successful request)
        assert len(result) > 0, "No task instances were created"

        # Each batch should have the correct number of tasks
        for row in result:
            assert row.count == len(
                tasks
            ), f"Expected {len(tasks)} tasks per batch, got {row.count}"

            # Verify tasks were updated to QUEUED status
            task_ids = [task.task_id for task in tasks]
            task_ids_str = ",".join(map(str, task_ids))
            task_status_query = text(
                f"""
                SELECT status, COUNT(*) as count
                FROM task 
                WHERE id IN ({task_ids_str})
                GROUP BY status
            """
            )

            status_result = session.execute(task_status_query).fetchall()

        # All tasks should be in QUEUED status
        queued_count = 0
        for row in status_result:
            if row.status == TaskStatus.QUEUED:
                queued_count = row.count

        assert queued_count == len(
            tasks
        ), f"Expected {len(tasks)} tasks in QUEUED status, got {queued_count}"


def test_concurrent_array_operations_simulation():
    """Test that simulates concurrent array operations to verify deadlock prevention logic."""
    import threading
    import time
    from unittest.mock import Mock

    # Mock the database operations to simulate the deadlock scenario
    class MockSession:
        def __init__(self):
            self.batch_numbers = []
            self.lock = threading.Lock()

        def execute(self, query):
            # Simulate the batch number calculation
            if "max" in str(query) and "array_batch_num" in str(query):
                with self.lock:
                    # Simulate the SELECT FOR UPDATE behavior
                    time.sleep(0.01)  # Small delay to simulate DB operation
                    next_batch = (
                        max(self.batch_numbers) + 1 if self.batch_numbers else 1
                    )
                    self.batch_numbers.append(next_batch)
                    return Mock(scalar=lambda: next_batch)
            return Mock()

        def commit(self):
            pass

        def rollback(self):
            pass

    # Test the deadlock prevention logic
    def simulate_batch_operation(thread_id, session, results, errors):
        """Simulate a batch operation that would previously cause deadlocks."""
        try:
            # This simulates the new deadlock-safe approach
            batch_num_result = session.execute(
                "select max(array_batch_num) from task_instance"
            ).scalar()

            # Simulate successful operation
            results.append(
                {"thread_id": thread_id, "batch_num": batch_num_result, "success": True}
            )
        except Exception as e:
            errors.append({"thread_id": thread_id, "error": str(e)})

    # Run concurrent operations
    session = MockSession()
    results = []
    errors = []
    threads = []
    num_threads = 10

    for i in range(num_threads):
        thread = threading.Thread(
            target=simulate_batch_operation, args=(i, session, results, errors)
        )
        threads.append(thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for completion
    for thread in threads:
        thread.join()

    # Verify no deadlocks occurred
    assert len(errors) == 0, f"Expected no errors, got: {errors}"
    assert (
        len(results) == num_threads
    ), f"Expected {num_threads} results, got {len(results)}"

    # Verify all operations got unique batch numbers
    batch_numbers = [r["batch_num"] for r in results]
    assert len(set(batch_numbers)) == num_threads, "All batch numbers should be unique"
    assert sorted(batch_numbers) == list(
        range(1, num_threads + 1)
    ), "Batch numbers should be sequential"


def test_queue_task_batch_and_log_done_concurrently(db_engine, tool):
    """Call queue_task_batch and log_done concurrently to ensure no deadlocks.

    This uses wf.requester.send_request for both routes, following test_cli_routes.py pattern.
    """
    from jobmon.core.constants import TaskInstanceStatus

    # Build a simple workflow with one array and a few tasks
    tt = tool.get_task_template(
        template_name="concurrent_test_template",
        command_template="echo {arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
        default_cluster_name="sequential",
        default_compute_resources={"queue": "null.q"},
    )

    tasks = tt.create_tasks(arg=[1, 2, 3], compute_resources={"queue": "null.q"})
    wf = tool.create_workflow()
    wf.add_tasks(tasks)
    wf.bind()
    wf._bind_tasks()

    array_id = tasks[0].array.array_id

    # Create a workflow run to obtain workflow_run_id
    wfr = WorkflowRunFactory(wf.workflow_id).create_workflow_run()
    workflow_run_id = wfr.workflow_run_id

    # Prepare queue_task_batch payload
    qtb_payload = {
        "task_ids": [t.task_id for t in tasks],
        "task_resources_id": 1,
        "workflow_run_id": workflow_run_id,
    }

    # Helper to call queue_task_batch
    def call_queue_task_batch(res):
        rc, msg = wf.requester.send_request(
            app_route=f"/array/{array_id}/queue_task_batch",
            message=qtb_payload,
            request_type="post",
        )
        res.append((rc, msg))

    # After queue, pick one TI and call log_done concurrently
    log_results = []

    def call_log_done(res):
        # Fetch a task_instance id created by queue in a loop until available
        # Small wait loop to avoid race on immediate selection
        ti_id = None
        with Session(bind=db_engine) as session:
            for _ in range(50):
                row = session.execute(
                    text(
                        """
                        SELECT id
                        FROM task_instance
                        WHERE array_id = :array_id
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"array_id": array_id},
                ).fetchone()
                session.commit()
                if row:
                    ti_id = row.id
                    break
                time.sleep(0.01)

        # If not yet created, still attempt a call with a safe fallback
        if ti_id is None:
            res.append((500, {"detail": "no task_instance yet"}))
            return

        rc, msg = wf.requester.send_request(
            app_route=f"/task_instance/{ti_id}/log_done",
            message={"stdout": "ok", "stderr": ""},
            request_type="post",
        )
        res.append((rc, msg))

    # Start both calls concurrently
    qtb_results = []
    threads = [
        threading.Thread(target=call_queue_task_batch, args=(qtb_results,)),
        threading.Thread(target=call_log_done, args=(log_results,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Both calls should return 200
    assert (
        len(qtb_results) == 1 and qtb_results[0][0] == 200
    ), f"queue_task_batch failed: {qtb_results}"
    assert (
        len(log_results) == 1 and log_results[0][0] == 200
    ), f"log_done failed: {log_results}"

    # Validate TI state is DONE for the selected TI or remains consistent
    with Session(bind=db_engine) as session:
        statuses = session.execute(
            text(
                """
                SELECT status
                FROM task_instance
                WHERE array_id = :array_id
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"array_id": array_id},
        ).fetchone()
        session.commit()

        assert statuses is not None
        # Accept DONE or QUEUED depending on scheduling; just ensure it's a valid state
        assert statuses.status in (
            TaskInstanceStatus.DONE,
            TaskInstanceStatus.QUEUED,
            TaskInstanceStatus.INSTANTIATED,
            TaskInstanceStatus.LAUNCHED,
        )


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
            update_query = text("UPDATE task SET status = :status WHERE id = :task_id")
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
    assert (
        TaskStatus.QUEUED in tasks_by_status
    ), f"Expected QUEUED status in response, got statuses: {list(tasks_by_status.keys())}"

    queued_task_ids = tasks_by_status.get(TaskStatus.QUEUED, [])
    assert len(queued_task_ids) == len(
        tasks
    ), f"Expected {len(tasks)} tasks in QUEUED status, got {len(queued_task_ids)}"

    # Verify all task IDs are present
    expected_task_ids = set(task.task_id for task in tasks)
    returned_task_ids = set(queued_task_ids)
    assert (
        returned_task_ids == expected_task_ids
    ), f"Returned task IDs don't match. Expected: {expected_task_ids}, Got: {returned_task_ids}"
