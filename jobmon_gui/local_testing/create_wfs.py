import os
import random
import subprocess
from time import sleep
from pathlib import Path

import argparse


def parse_arguments():
    """
    Function to parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    # Create the parser
    parser = argparse.ArgumentParser(description="Example script with optional arguments.")

    # Add the first optional argument
    parser.add_argument('--server-url',
                        type=str,
                        help="Description for the first optional argument.",
                        default="http://localhost:8070/api/v3")

    # Add the second optional argument
    parser.add_argument('--wf',
                        type=int,
                        help="Number of workflows to create. 0 means continuesly creating workflows.",
                        default=0)

    # Add the --wf-type argument with limited options
    parser.add_argument(
        '--wf-type',
        type=str,
        help="Type of workflows: simple, tired, huge, or random.",
        choices=["simple", "tired", "random", "huge"],  # Specify the allowed values
        default="simple"  # Set the default value
    )

    # Parse the arguments
    return parser.parse_args()


def create_simple_wf():
    """Use the task_generator_wf.py script to create a simple workflow."""
    wf_script_path = Path(__file__).parent.parent.parent / "tests/worker_node/task_generator_wf.py"
    # Run the command and wait for it to finish
    result = subprocess.check_output(["python", str(wf_script_path), "1"])
    # This line will only run after the command above finishes
    print("Simple workflow completed!")


def create_tired_wf(size_tier1=10, size_tier3=100):
    """Use dummy cluster to create a fake tired workflow."""
    from jobmon.client.api import Tool
    tool = Tool("multiprocess")
    C = "multiprocess"
    Q = "null.q"
    tt = tool.get_task_template(
        template_name="tired_task1",
        command_template="echo {arg} || true || {arg_filler}",
        node_args=["arg"],
        task_args=["arg_filler"]
    )
    tt2 = tool.get_task_template(
        template_name="tired_task2",
        command_template="echo {arg}",
        node_args=["arg"]
    )
    tt3 = tool.get_task_template(
        template_name="tired_task3",
        command_template="echo {arg} || {arg_filler} || true",
        node_args=["arg"],
        task_args=["arg_filler"]
    )
    tt4 = tool.get_task_template(
        template_name="tired_task4",
        command_template="echo {arg} || true || true",
        node_args=["arg"]
    )
    tier1 = []
    for i in range(size_tier1):
        task = tt.create_task(
            name=f"tired_task_{i}",
            arg=i,
            arg_filler=f"Task {i} is tired",
            compute_resources={"queue": Q, "num_cores": 1},
        )
        tier1.append(task)
    tier2 = tt2.create_task(
        name=f"tired_task_second_tier",
        arg="I am the second task",
        upstream_tasks=tier1,
        compute_resources={"queue": Q, "num_cores": 1},
    )
    tasks = tier1 + [tier2]
    tier3 = []
    for i in range(size_tier3):
        task = tt3.create_task(
            name=f"tired3_task_{i}",
            arg=i,
            upstream_tasks=[tier2],
            arg_filler=f"Task {i} is tired",
            compute_resources={"queue": Q, "num_cores": 1},
        )
        tier3.append(task)
    tasks = tasks + tier3
    tier4 = tt4.create_task(
        name=f"tired_task_second_tier",
        arg="I am the last task",
        upstream_tasks=tier3,
        compute_resources={"queue": Q, "num_cores": 1},
    )
    tasks = tasks + [tier4]
    wf = tool.create_workflow(
        name=f"wf",
        default_cluster_name=C,
        default_compute_resources_set={"queue": Q, "num_cores": 1},
        workflow_attributes={"test_attribute": "test"}
    )
    wf.add_tasks(tasks)
    wf.run(configure_logging=True)


def create_wf(total, wf_type):
    print(f"Creating {total} workflows of type {wf_type}")
    created = 0
    # create #total of workflows; if total is 0, create workflows continuously
    while total == 0 or created < total:
        print("Creating workflow")
        created += 1
        this_wf_type = wf_type
        print(f"Workflow type: {this_wf_type}")
        if this_wf_type == "random":
            this_wf_type = random.choice(["simple", "tired"])
        if this_wf_type == "simple":
            create_simple_wf()
        elif this_wf_type == "tired":
            create_tired_wf()
        elif this_wf_type == "huge":
            create_tired_wf(100, 90000)
        sleep(10)


# Example usage of the function
if __name__ == "__main__":
    args = parse_arguments()

    # Access the arguments
    url = args.server_url
    os.environ["JOBMON__HTTP__SERVICE_URL"] = "http://localhost:8070"
    os.environ["JOBMON__HTTP__ROUTE_PREFIX"] = "/api/v2"
    wfs = args.wf
    wf_type = args.wf_type
    create_wf(wfs, wf_type)
