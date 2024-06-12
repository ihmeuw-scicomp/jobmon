import os

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

# Get the full path of the current script
script_path = os.path.abspath(__file__)

# Resolve any symbolic links (if necessary)
full_script_path = os.path.realpath(script_path)

tool = Tool()
tool.set_default_compute_resources_from_dict(
    cluster_name="sequential", compute_resources={"queue": "null.q"}
)

@task_generator.task_generator(serializers={}, tool=tool, module_source_path=full_script_path, max_attempts=1)
def simple_function(foo: int) -> None:
    """Simple task_function."""
    print(f"foo: {foo}")


def simple_tasks() -> None:
    """Simple task."""
    wf = tool.create_workflow()
    computer_resource = {"queue": "null.q"}
    for i in range(5):
        task = simple_function.create_task(computer_resource=computer_resource, foo=i)
        wf.add_tasks([task])
    r = wf.run()
    assert r == "D"

def main():
    simple_tasks()

if __name__ == "__main__":
    main()