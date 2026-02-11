"""16-task workflow with variable runtime, memory, and errors.

Modeled after six_job_test.py but with 13 tasks in phase_2/phase_3
to produce rich data for the Task Template Details dashboard:

    phase_1 (2 tasks):  setup, both succeed
    phase_2 (7 tasks):  processing with variable workloads
        - 5 succeed with different runtime/memory
        - 1 fails permanently  (error cluster 1)
        - 1 fails once then retries (error cluster 2)
    phase_3 (6 tasks):  aggregation
        - 3 succeed
        - 1 blocked by upstream failure
        - 2 fail permanently   (error clusters 3 & 4)

Resulting scatter plot data:
    - 11 DONE points with runtime 3-12s and variable memory
    - 3 ERROR points with runtime 1-4s
    - 1 REGISTERING task (never runs)
    - 4 distinct error clusters in the Errors tab

Usage:
    python dashboard_test.py <cluster_name> [wf_id_file]

    # Inside Docker:
    python dashboard_test.py sequential

Note: Use the 'multiprocess' cluster for better memory variation;
the sequential distributor accumulates peak memory across tasks.
"""

import os
import sys
import tempfile
import uuid

from jobmon.client.api import Tool
from jobmon.client.logging import configure_client_logging


thisdir = os.path.dirname(os.path.realpath(__file__))
python = sys.executable


def get_task_template(tool, template_name):
    tt = tool.get_task_template(
        template_name=template_name,
        command_template="{command}",
        node_args=["command"],
    )
    return tt


# ---------------------------------------------------------------------------
# Command builders
# ---------------------------------------------------------------------------

def _workload_cmd(sleep_secs, memory_mb):
    """Succeed after allocating *memory_mb* and sleeping."""
    return (
        f"{python} -c '"
        f"import time; "
        f"_ = bytearray({memory_mb} * 1024 * 1024); "
        f"time.sleep({sleep_secs})"
        f"'"
    )


def _fail_cmd(sleep_secs, error_msg):
    """Fail with *error_msg* after sleeping."""
    return (
        f"bash -c '"
        f"sleep {sleep_secs}; "
        f"echo \"{error_msg}\" >&2; "
        f"exit 1"
        f"'"
    )


def _retry_cmd(marker_path, error_msg, sleep_secs, memory_mb):
    """Fail on first attempt, succeed with workload on retry."""
    return (
        f"bash -c '"
        f"if [ ! -f {marker_path} ]; then "
        f"  touch {marker_path}; "
        f"  echo \"{error_msg}\" >&2; "
        f"  exit 1; "
        f"else "
        f"  rm -f {marker_path}; "
        f"  {python} -c "
        f"\"import time; "
        f"_ = bytearray({memory_mb} * 1024 * 1024); "
        f"time.sleep({sleep_secs})\"; "
        f"fi'"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def dashboard_test(cluster_name, wf_id_file=None):
    """Create and run the 16-task dashboard test workflow."""
    configure_client_logging()

    tool = Tool(name=f"Dashboard test - {cluster_name}")
    marker_dir = tempfile.mkdtemp(prefix="jobmon_dashboard_")

    # -- Phase 1: 2 setup tasks (both succeed) -------------------
    phase_1 = get_task_template(tool, "phase_1")

    t1 = phase_1.create_task(
        name="setup_data",
        command=_workload_cmd(sleep_secs=8, memory_mb=10),
    )
    t2 = phase_1.create_task(
        name="setup_config",
        command=_workload_cmd(sleep_secs=12, memory_mb=15),
    )

    # -- Phase 2: 7 processing tasks ----------------------------
    phase_2 = get_task_template(tool, "phase_2")

    t3 = phase_2.create_task(
        name="process_alpha",
        command=_workload_cmd(sleep_secs=3, memory_mb=15),
        upstream_tasks=[t1, t2],
    )
    t4 = phase_2.create_task(
        name="process_beta",
        command=_workload_cmd(sleep_secs=7, memory_mb=30),
        upstream_tasks=[t1, t2],
    )
    # t5 always fails — error cluster 1
    t5 = phase_2.create_task(
        name="process_gamma",
        command=_fail_cmd(
            sleep_secs=1,
            error_msg=(
                "ERROR: Data validation failed - input file "
                "contains NaN values in column 'mortality_rate'"
            ),
        ),
        upstream_tasks=[t1, t2],
        max_attempts=3,
    )
    t6 = phase_2.create_task(
        name="process_delta",
        command=_workload_cmd(sleep_secs=9, memory_mb=25),
        upstream_tasks=[t1, t2],
    )
    # t7 fails once, succeeds on retry — error cluster 2
    t7 = phase_2.create_task(
        name="process_epsilon",
        command=_retry_cmd(
            marker_path=os.path.join(marker_dir, "t7_attempted"),
            error_msg=(
                "ERROR: ConnectionError - Timeout after 30s "
                "connecting to database server at db.cluster:5432"
            ),
            sleep_secs=5,
            memory_mb=20,
        ),
        upstream_tasks=[t1, t2],
        max_attempts=3,
    )
    t8 = phase_2.create_task(
        name="process_zeta",
        command=_workload_cmd(sleep_secs=11, memory_mb=40),
        upstream_tasks=[t1, t2],
    )
    t9 = phase_2.create_task(
        name="process_eta",
        command=_workload_cmd(sleep_secs=6, memory_mb=35),
        upstream_tasks=[t1, t2],
    )

    # -- Phase 3: 6 aggregation tasks ---------------------------
    phase_3 = get_task_template(tool, "phase_3")

    t10 = phase_3.create_task(
        name="aggregate_north",
        command=_workload_cmd(sleep_secs=4, memory_mb=8),
        upstream_tasks=[t3, t4],
    )
    t11 = phase_3.create_task(
        name="aggregate_south",
        command=_workload_cmd(sleep_secs=6, memory_mb=18),
        upstream_tasks=[t4, t6],
    )
    # t12 blocked — depends on t5 which fails
    t12 = phase_3.create_task(
        name="aggregate_east",
        command=_workload_cmd(sleep_secs=3, memory_mb=12),
        upstream_tasks=[t5, t3],
    )
    # t13 always fails — error cluster 3
    t13 = phase_3.create_task(
        name="aggregate_west",
        command=_fail_cmd(
            sleep_secs=4,
            error_msg=(
                "ERROR: RuntimeError - Memory allocation overflow "
                "during aggregation of partition 'west' "
                "(requested 8.1 GiB, available 4.0 GiB)"
            ),
        ),
        upstream_tasks=[t6, t8],
        max_attempts=3,
    )
    t14 = phase_3.create_task(
        name="aggregate_central",
        command=_workload_cmd(sleep_secs=5, memory_mb=20),
        upstream_tasks=[t7, t9],
    )
    # t15 always fails — error cluster 4
    t15 = phase_3.create_task(
        name="aggregate_islands",
        command=_fail_cmd(
            sleep_secs=2,
            error_msg=(
                "ERROR: OSError - Disk quota exceeded while "
                "writing output to /mnt/share/results/islands.parquet"
            ),
        ),
        upstream_tasks=[t8, t9],
        max_attempts=3,
    )

    # -- Resource config & run -----------------------------------
    tool.set_default_compute_resources_from_yaml(
        default_cluster_name=cluster_name,
        yaml_file=os.path.join(thisdir, "six_job_test_resources.yaml"),
        set_task_templates=True,
        ignore_missing_keys=True,
    )

    wf = tool.create_workflow(
        workflow_args="dashboard-test-{}".format(uuid.uuid4()),
        name="dashboard_test",
    )
    all_tasks = [t1, t2, t3, t4, t5, t6, t7, t8, t9,
                 t10, t11, t12, t13, t14, t15]
    wf.add_tasks(all_tasks)

    print(
        "Running dashboard test workflow "
        f"({len(all_tasks)} tasks, expect ~30-60s with errors)"
    )
    wfr_status = wf.run()

    print(f"workflow_id={wf.workflow_id}")
    if wf_id_file is not None:
        with open(wf_id_file, "w") as f:
            f.write(str(wf.workflow_id))

    if wfr_status == "D":
        raise ValueError(
            "Workflow finished DONE but should have errors"
        )
    else:
        print(
            f"Workflow ended with status '{wfr_status}' "
            f"(expected — t5, t13, t15 fail; t12 blocked)."
        )
        print(
            f"Open the GUI at /workflow/{wf.workflow_id} "
            f"to verify dashboard filtering."
        )


if __name__ == "__main__":
    cluster_name = sys.argv[1]
    wf_id_file = None
    if len(sys.argv) > 2:
        wf_id_file = sys.argv[2]
    dashboard_test(cluster_name, wf_id_file)
