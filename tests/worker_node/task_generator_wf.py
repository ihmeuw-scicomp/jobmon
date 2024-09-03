import os
import sys
import importlib
import importlib.util
import importlib.machinery

from typing import Optional

from jobmon.core.task_generator import task_generator, TaskGeneratorModuleDocumenter, TaskGeneratorDocumenter
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
        task = simple_function.create_task(
            cluster_name="sequential",
            compute_resources=compute_resources,
            foo=i,
            bar=["a a", "b\"c\"b"])
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
        task = simple_function.create_task(
            cluster_name="slurm",
            compute_resources=compute_resources,
            foo=i,
            bar=["a", "b"])
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


def simple_tasks_serializer_slurm_src() -> None:
    """Simple task."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()

    # Import the task_generator_funcs.py module
    spec = importlib.util.spec_from_file_location(
        "task_generator_funcs", task_generator_funcs_path
    )
    task_generator_funcs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_generator_funcs)
    simple_function = task_generator_funcs.simple_function_with_serializer_rsc
    test_year = task_generator_funcs.TestYear
    for i in range(2020, 2024):
        task = simple_function.create_task(
           year=test_year(i)
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


def special_char_tasks_serializer_seq() -> None:
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
    simple_function = task_generator_funcs.special_chars_function
    task = simple_function.create_task(compute_resources=compute_resources, foo=f"\'aaa\'")
    wf.add_task(task)
    r = wf.run(configure_logging=True)
    assert r == "D"


## ------------FHS tests-----------------
spec = importlib.util.spec_from_file_location(
        "task_generator_fhs", fhs_generator_funcs_path
    )
task_generator_fhs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(task_generator_fhs)
YearRange = task_generator_fhs.YearRange
Versions = task_generator_fhs.Versions
FHSFileSpec = task_generator_fhs.FHSFileSpec
FHSDirSpec = task_generator_fhs.FHSDirSpec
VersionMetadata = task_generator_fhs.VersionMetadata
Quantiles = task_generator_fhs.Quantiles
versions_to_list = task_generator_fhs.versions_to_list
versions_from_list = task_generator_fhs.versions_from_list
quantiles_to_list = task_generator_fhs.quantiles_to_list
quantiles_from_list = task_generator_fhs.quantiles_from_list

fhs_serializer = {
        YearRange: (str, YearRange.parse_year_range),
        Versions: (versions_to_list, versions_from_list),
        FHSFileSpec: (str, FHSFileSpec.parse),
        FHSDirSpec: (str, FHSDirSpec.parse),
        VersionMetadata: (str, VersionMetadata.parse_version),
        Quantiles: (quantiles_to_list, quantiles_from_list),
    }

@task_generator(tool_name="test_tool", naming_args=["version"], serializers=fhs_serializer, module_source_path=fhs_generator_funcs_path)
def fhs_simple_function(yr: YearRange, v: Versions, fSpec: FHSFileSpec, dSpec: FHSDirSpec, vm: VersionMetadata, q: Optional[Quantiles]) -> None:
    """Simple task_function."""
    print(f"YearRange: {yr}")
    print(f"Version: {v}")
    print(f"FHSFileSpec: {fSpec}")
    print(f"FHSDirSpec: {dSpec}")
    print(f"VersionMetadata: {vm}")
    print(f"Quantiles: {q}")


def fhs_seq():
    tool = Tool("test_tool")
    wf = tool.create_workflow()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "null.q"}
    # Import the task_generator_funcs.py module

    task = fhs_simple_function.create_task(
            compute_resources=compute_resources,
            yr=YearRange(2020, 2021),
            v=Versions("1.0", "2.0"),
            fSpec=FHSFileSpec("/path/to/file"),
            dSpec=FHSDirSpec("/path/to/dir"),
            vm=VersionMetadata("1.0"),
            q=Quantiles(0.1, 0.9)
        )
    wf.add_tasks([task])
    s = wf.run()
    assert s == "D"


def fhs_slurm():
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}
    # Import the task_generator_funcs.py module

    task = fhs_simple_function.create_task(
            compute_resources=compute_resources,
            yr=YearRange(2020, 2021),
            v=Versions("1.0", "2.0"),
            fSpec=FHSFileSpec("/path/to/file"),
            dSpec=FHSDirSpec("/path/to/dir"),
            vm=VersionMetadata("1.0"),
        )
    wf.add_tasks([task])
    s = wf.run()
    assert s == "D"


def fhs_slurmz_rsc():
    tool = Tool("test_tool")
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}
    # Import the task_generator_funcs.py module

    task = fhs_simple_function.create_task(
            cluster_name="slurm",
            compute_resources=compute_resources,
            yr=YearRange(2020, 2021),
            v=Versions("1.0", "2.0"),
            fSpec=FHSFileSpec("/path/to/file"),
            dSpec=FHSDirSpec("/path/to/dir"),
            vm=VersionMetadata("1.0"),
        )
    wf.add_tasks([task])
    s = wf.run()
    assert s == "D"


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
    elif input_value == 7:
        fhs_seq()
    elif input_value == 8:
        fhs_slurm()
    elif input_value == 9:
        special_char_tasks_serializer_seq()
    elif input_value == 10:
        fhs_slurmz_rsc()
    else:
        simple_tasks_seq()

if __name__ == "__main__":
    main()
