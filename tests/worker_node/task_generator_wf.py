import os
import sys
import importlib
import importlib.util
import importlib.machinery

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

# Get the full path of the current script
script_path = os.path.abspath(__file__)

# Resolve any symbolic links (if necessary)
full_script_path = os.path.realpath(script_path)

# get the task_generator_funcs.py path
task_generator_funcs_path = os.path.join(
    os.path.dirname(script_path), "task_generator_funcs.py"
)
fhs_generator_funcs_path = os.path.join(
    os.path.dirname(script_path), "task_generator_fhs.py"
)


def simple_tasks_seq() -> None:
    """Simple task."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "null.q"}

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function
    for i in range(5):
        task = simple_function.create_task(compute_resources=compute_resources, foo=i, bar=["a", "b"])
        wf.add_tasks([task])
    r = wf.run(configure_logging=True)
    assert r == "D"


def simple_tasks_slurm() -> None:
    """Simple task."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function
    for i in range(5):
        task = simple_function.create_task(compute_resources=compute_resources, foo=i, bar=["a", "b"])
        wf.add_tasks([task])
    r = wf.run(configure_logging=True)
    assert r == "D"


def simple_tasks_serializer_seq() -> None:
    """Simple task."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "null.q"}

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function_with_serializer
    test_year = task_generator_funcs.TestYear
    for i in range(2020, 2024):
        task = simple_function.create_task(
            compute_resources=compute_resources, year=test_year(i)
        )
        wf.add_tasks([task])
    r = wf.run(configure_logging=True)
    assert r == "D"


def simple_tasks_serializer_slurm() -> None:
    """Simple task."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function_with_serializer
    test_year = task_generator_funcs.TestYear
    for i in range(2020, 2024):
        task = simple_function.create_task(
            compute_resources=compute_resources, year=test_year(i)
        )
        wf.add_tasks([task])
    r = wf.run(configure_logging=True)
    assert r == "D"


def simple_tasks_array() -> None:
    """Simple tasks in array with list input."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function
    tasks = simple_function.create_tasks(compute_resources=compute_resources, foo=[1, 2], bar=[["a", "b"]])
    wf.add_tasks(tasks)
    r = wf.run(configure_logging=True)
    assert r == "D"


def simple_tasks_serializer_array() -> None:
    """Simple tasks in array."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function_with_serializer
    test_year = task_generator_funcs.TestYear
    tasks = simple_function.create_tasks(
            compute_resources=compute_resources, year=[test_year(2023), test_year(2024)]
        )
    wf.add_tasks(tasks)
    r = wf.run(configure_logging=True)
    assert r == "D"


def main():
    if len(sys.argv) > 1:
        try:
            input_value = int(sys.argv[1])
        except ValueError:
            input_value = None
    else:
        input_value = None

    if input_value == 2:
        simple_tasks_slurm()
    elif input_value == 3:
        simple_tasks_serializer_seq()
    elif input_value == 4:
        simple_tasks_serializer_slurm()
    elif input_value == 5:
        simple_tasks_array()
    elif input_value == 6:
        simple_tasks_serializer_array()
    else:
        simple_tasks_seq()

if __name__ == "__main__":
    main()
