import ast
import base64
import html
import os
import pickle
from typing import List

from jobmon.core import task_generator
from jobmon.client.api import Tool

class child:
    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other: "child") -> bool:
        return self.name == other.name

    def __str__(self) -> str:
        return self.name


def pickle_serializer(input: child) -> str:
    """Serialize input to json."""
    pr =  pickle.dumps(input)
    return base64.b64encode(pr).decode('utf-8')


def pickle_deserializer(input: str) -> child:
    """Deserialize input from json."""
    byte_input = base64.b64decode(input.encode('utf-8'))
    return child(pickle.loads(byte_input)


@task_generator.task_generator(
    serializers={child: (pickle_serializer, pickle_deserializer)},
    tool_name="test_tool",
    max_attempts=1,
    naming_args=["foo"],
)
def pickle_function(foo: child) -> None:
    """Simple task_function."""
    print(f"foo: {foo.name}")


def pickle_serializer_seq() -> None:
    """Simple task."""
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "null.q"}

    task = pickle_function.create_task(compute_resources=compute_resources, foo=child("test"))
    wf.add_task(task)
    r = wf.run(configure_logging=True)
    assert r == "D"


if __name__ == "__main__":
    pickle_serializer_seq()

