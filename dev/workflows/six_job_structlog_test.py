"""Workflow smoke test that mimics the FHS structlog configuration."""

from __future__ import annotations

import logging
import os
import sys
import uuid
from pathlib import Path

import structlog

from jobmon.client.api import Tool
from jobmon.client.logging import configure_client_logging


THIS_DIR = Path(__file__).resolve().parent


def _configure_structlog_like_fhs() -> None:
    """Emulate the structlog setup used by Forecasting Health Systems."""

    structlog.reset_defaults()
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def _get_task_template(tool: Tool, template_name: str):  # pragma: no cover - shim
    return tool.get_task_template(
        template_name=template_name,
        command_template="{command}",
        node_args=["command"],
    )


def six_job_structlog_test(cluster_name: str, wf_id_file: str | None = None) -> None:
    """Run the six job workflow under a direct-render structlog configuration."""

    _configure_structlog_like_fhs()

    configure_client_logging()

    tool = Tool(name=f"Jackalope structlog testing - {cluster_name}")

    t1 = _get_task_template(tool, "phase_1").create_task(
        name="t1",
        command="sleep 10",
    )

    phase_2 = _get_task_template(tool, "phase_2")
    t2 = phase_2.create_task(name="t2", command="sleep 20", upstream_tasks=[t1])
    t3 = phase_2.create_task(name="t3", command="sleep 25", upstream_tasks=[t1])

    phase_3 = _get_task_template(tool, "phase_3")
    t4 = phase_3.create_task(name="t4", command="sleep 17", upstream_tasks=[t2, t3])
    t5 = phase_3.create_task(name="t5", command="sleep 13", upstream_tasks=[t2, t3])

    phase_4 = _get_task_template(tool, "phase_4")
    t6 = phase_4.create_task(name="t6", command="sleep 19", upstream_tasks=[t4, t5])

    tool.set_default_compute_resources_from_yaml(
        default_cluster_name=cluster_name,
        yaml_file=str(THIS_DIR / "six_job_test_resources.yaml"),
        set_task_templates=True,
        ignore_missing_keys=True,
    )

    workflow = tool.create_workflow(
        workflow_args=f"six-job-structlog-{uuid.uuid4()}",
        name="six_job_structlog_test",
    )
    workflow.add_tasks([t1, t2, t3, t4, t5, t6])

    print("Running structlog workflow (approximately 70 seconds)...")
    status = workflow.run()
    print(f"workflow_id={workflow.workflow_id}")

    if wf_id_file is not None:
        Path(wf_id_file).write_text(str(workflow.workflow_id), encoding="utf-8")

    if status != "D":
        raise ValueError(f"Workflow should be successful, not state {status}")


if __name__ == "__main__":  # pragma: no cover - CLI helper
    if len(sys.argv) < 2:
        raise SystemExit("Usage: six_job_structlog_test.py <cluster> [wf_id_file]")

    cluster = sys.argv[1]
    wf_file = sys.argv[2] if len(sys.argv) > 2 else None
    six_job_structlog_test(cluster, wf_file)

