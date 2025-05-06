import pytest
from typing import Any, Dict, List, Optional, Tuple, Union
from unittest.mock import Mock
from random import randint

from jobmon.core import task_generator
from jobmon.core.task_generator import (
    TaskGeneratorDocumenter,
    TaskGeneratorModuleDocumenter,
)
from jobmon.client.api import Tool


def test_simple_task(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get a good looking command string.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#642
    """
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={}, tool_name="test_tool", default_cluster_name="sequential"
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="b a z"
    )

    # Verify task name
    assert task.name == "simple_function:foo=1:bar=b_a_z"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " foo='1'"
        " bar='b a z'"
    )

    assert task.command == expected_command
    assert task.compute_resources == compute_resources


def test_upstream_task(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get expected upstream task."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={}, tool_name="test_tool", default_cluster_name="sequential"
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Create an upstream task
    upstream_task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="b a z"
    )

    # Create a downstream task
    downstream_task = simple_function.create_task(
        compute_resources=compute_resources,
        foo=2,
        bar="b u z z",
        upstream_tasks=[upstream_task],
    )

    assert downstream_task.upstream_tasks == {upstream_task}


@pytest.mark.parametrize(
    ["task_attributes"], [{"fake_attr": "fake_value"}, ["fake_attr"], Dict()]
)
def test_task_attributes(
    client_env,
    monkeypatch: pytest.fixture,
    task_attributes: Union[List[str], Dict[str, Any]],
) -> None:
    """Verify that we get expected task attributes."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={}, tool_name="test_tool", default_cluster_name="sequential"
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    task = simple_function.create_task(
        compute_resources=compute_resources,
        foo=1,
        bar="b a z",
        task_attributes=task_attributes,
    )

    if isinstance(task_attributes, Dict):
        assert task.task_attributes == task_attributes
    elif isinstance(task_attributes, List):
        assert set(task.task_attributes.keys()) == set(task_attributes)


def test_list_args(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify we can properly pass args that serialize as lists.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#672
    """
    # Set up functino
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={}, tool_name="test_tool", default_cluster_name="sequential"
    )
    def list_function(foo: List[str], bar: List[str]) -> None:
        """Example task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = list_function.create_task(
        compute_resources=compute_resources, foo=["a", "b b"], bar=['c"']
    )

    # Verify task name
    assert task.name == "list_function:foo=a,b_b:bar=c_"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name list_function"
        " foo='[a,b b]'"
        " bar='[c\"]'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources


@pytest.mark.parametrize(
    ["naming_args", "expected_name"],
    [
        [["foo", "bar"], "simple_function:foo=1:bar=baz"],
        [None, "simple_function:foo=1:bar=baz"],
        [["foo"], "simple_function:foo=1"],
        [[], "simple_function"],
    ],
)
def test_naming_args(
    client_env, naming_args, expected_name, monkeypatch: pytest.fixture
) -> None:
    """Verify that the name only includes the expected naming args.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#711
    """
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={},
        tool_name="test_tool",
        naming_args=naming_args,
        default_cluster_name="sequential",
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="baz"
    )
    # Verify task name matches the expected
    assert task.name == expected_name

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " foo='1'"
        " bar='baz'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources


def test_max_attempts(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we pass max_attempts correctly to the task.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#745
    """
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    max_attempts = 40
    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={}, tool_name="test_tool", max_attempts=max_attempts
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        cluster_name="sequential", compute_resources=compute_resources, foo=1, bar="baz"
    )

    assert task.max_attempts == max_attempts


def my_func() -> None:
    """A simple function.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#243
    """
    pass


@pytest.mark.parametrize(
    "simple_type", ["something", 10, 1.5, True]  # Test instances of the SIMPLE_TYPES
)
def test_simple_type(client_env, simple_type: Any) -> None:
    """Ensure a known simple type is properly serialized.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#243
    """
    # Instantiate the TaskGenerator

    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={},
        tool_name="test_tool",
        default_cluster_name="sequential",
    )

    # Exercise by calling serialize
    result = task_gen.serialize(simple_type, type(simple_type))

    # Verify the result is simply the stringified simple_type
    assert result == str(simple_type)


def test_serialize_empty_string(client_env) -> None:
    """Ensure the empty string is properly serialized."""
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={},
        tool_name="test_tool",
        default_cluster_name="sequential",
    )

    # Exercise by calling serialize
    result = task_gen.serialize("", str)

    # Verify the result is the serialized empty string
    assert result == task_generator.SERIALIZED_EMPTY_STRING


class FakeYearRange:
    """A fake YearRange class for testing"""

    def __init__(self, year: int) -> None:
        self.year = year

    @staticmethod
    def parse_year_range(year: str) -> "FakeYearRange":
        """Parse a year range."""
        return FakeYearRange(int(year.split(":")[0]))

    def __str__(self) -> str:
        return str(self.year)

    def __eq__(self, other):
        return self.year == other.year


def test_serializer_specified_type(client_env) -> None:
    """Ensure a serializer-specified type is properly serialized.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#255
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Instantiate a serializer-specified type
    my_obj = FakeYearRange.parse_year_range("2010:2020:2030")

    # Exercise by calling serialize
    result = task_gen.serialize(my_obj, FakeYearRange)
    # Verify the result is the stringified object
    assert result == str(my_obj)


def test_unknown_type_raises_error(client_env) -> None:
    """Ensure an unknown type raises an error.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#269
    """
    # Instantiate the TaskGenerator without a serializer for YearRange
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Instantiate an unknown, non-simple type
    my_obj = FakeYearRange.parse_year_range("2010:2020:2030")
    # Exercise by calling serialize & Verify an error is raised
    with pytest.raises(TypeError, match="Cannot serialize unknown type FakeYearRange"):
        task_gen.serialize(my_obj, FakeYearRange)


def test_built_in_collections(client_env) -> None:
    """Ensure the built-in collection types can be serialized.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#292
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Cast the items as the collection_type
    items_to_serialize = [
        1.0,
        False,
        "something",
        FakeYearRange.parse_year_range("2010:2020:2030"),
    ]

    for item_to_serialize in items_to_serialize:
        # Define the expected serialized result
        expected_result = str(item_to_serialize)

        # Exercise
        result = task_gen.serialize(
            obj=item_to_serialize, expected_type=type(item_to_serialize)
        )

        assert result == expected_result


def test_multidimensional_collection_raises_error(client_env) -> None:
    """Ensure an error is raised if a multi-dimensional collection is passed.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#318
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Define the items to serialize
    items_to_serialize = ["1.0", "False", "something", "2010:2020:2030"]

    # Cast the items as a multi-dimensional collection
    items_to_serialize = [items_to_serialize]

    # Exercise & Verify an error is raised
    with pytest.raises(
        TypeError,
        match="This version of Task Generator cannot serialize multi-dimensional collection",
    ):
        task_gen.serialize(obj=items_to_serialize, expected_type=List[List[str]])


def test_serialize_optional(client_env) -> None:
    """Ensure an optional type can be serialized.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#335
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Exercise by calling serialize
    result = task_gen.serialize(None, Optional[int])

    # Verify the result is simply the stringified None
    assert result == "None"


def test_empty_collection(client_env) -> None:
    """Ensure empty collections are returned as an empty list.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#349
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Define the expected result
    expected_result = "[]"

    # Exercise
    result = task_gen.serialize(list(), List[str])

    # Verify
    assert result == expected_result


def test_optional_collection(client_env) -> None:
    """Ensure an optional collection with data is serialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#363
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Define the items to serialize
    items_to_serialize = [1, 2, 3]

    # Exercise
    result = task_gen.serialize(items_to_serialize, Optional[List[int]])

    # Verify the result matches the expected result
    assert result == "[1,2,3]"
    # verify the serialized obj can be deserialized back
    de_result = task_gen.deserialize(result, Optional[List[int]])
    assert de_result == items_to_serialize

    # Exercise
    result = task_gen.serialize(None, Optional[List[int]])

    # Verify the result is simply the stringified None
    assert result == "None"
    # verify the serialized obj can be deserialized back
    de_result = task_gen.deserialize(result, Optional[List[int]])
    assert de_result == None


def test_no_internal_type_raises_error(client_env) -> None:
    """Ensure a collection with no internal type raises an error (``tuple``).

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#398
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Exercise & Verify an error is raised
    with pytest.raises(TypeError, match="Cannot serialize collection with.*"):
        task_gen.serialize(obj=(1.0, "this"), expected_type=tuple)


@pytest.mark.parametrize(
    "simple_type, expected_result",
    [
        ["something", "something"],  # Test instances of the SIMPLE_TYPES
        ["10", 10],
        ["1.5", 1.5],
        ["True", True],
    ],
)
def test_deserialize_simple_type(
    client_env, simple_type: str, expected_result: Any
) -> None:
    """Ensure a known simple type is properly deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#410
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Exercise by calling deserialize
    result = task_gen.deserialize(obj=simple_type, obj_type=type(expected_result))

    # Verify the result matches the expected result
    assert result == expected_result


def test_deserialize_empty_string(client_env) -> None:
    """Ensure the empty string is properly deserialized."""
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Exercise by calling deserialize
    result = task_gen.deserialize(
        obj=task_generator.SERIALIZED_EMPTY_STRING, obj_type=str
    )

    # Verify the result is the empty string
    assert result == ""


def test_deserializer_specified_type(client_env) -> None:
    """Ensure a serializer-specified type is properly deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#423
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Instantiate a serializer-specified type
    my_obj = FakeYearRange.parse_year_range("2010:2020:2030")

    # Exercise by calling deserialize
    result = task_gen.deserialize(obj=str(my_obj), obj_type=FakeYearRange)

    # Verify the result is the same as the original object
    assert result == my_obj


def test_deserializer_unknown_type_raises_error(client_env) -> None:
    """Ensure an unknown type raises an error.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#455
    """
    # Instantiate the TaskGenerator without a serializer for YearRange
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Instantiate an unknown, non-simple type
    my_obj = FakeYearRange.parse_year_range("2010:2020:2030")

    # Exercise by calling deserialize & Verify an error is raised
    with pytest.raises(
        TypeError, match="Cannot deserialize unknown type FakeYearRange"
    ):
        task_gen.deserialize(obj=str(my_obj), obj_type=FakeYearRange)


@pytest.mark.parametrize(
    "item_type, items_to_deserialize, expected_result",
    [
        [int, '["1", "3", "5"]', [1, 3, 5]],
        [float, '["1.0", "3.0", "5.0"]', [1.0, 3.0, 5.0]],
        [str, '["one", "three", "five"]', ["one", "three", "five"]],
        [bool, "[True, False]", [True, False]],
        [
            FakeYearRange,
            '["2010:2020:2030", "2040:2050:2060"]',
            [
                FakeYearRange.parse_year_range("2010:2020:2030"),
                FakeYearRange.parse_year_range("2040:2050:2060"),
            ],
        ],
    ],
)
def test_deserialize_built_in_collections(
    client_env,
    item_type: Any,
    items_to_deserialize: List[str],
    expected_result: List[Any],
) -> None:
    """Ensure the built-in collection types can be deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#469
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Exercise by calling deserialize on the items_to_deserialize, having cast them to
    # the collection_type
    result = task_gen.deserialize(obj=items_to_deserialize, obj_type=List[item_type])

    # Verify the result matches the expected result (cast as the collection type)
    assert result == expected_result


def test_deserialize_multi_annotated_collection(client_env) -> None:
    """Ensure a multi-annotated collection can be deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#511
    """
    # this is no longer supported as deserialize will only take str
    pass


@pytest.mark.parametrize(
    ["input", "expected"],
    [["None", None], ["1", [1]], ['["1", "2"]', [1, 2]], ["[]", []]],
)
def test_deserialize_optional_collection(
    client_env, input, expected: Optional[List[int]]
) -> None:
    """Ensure an optional collection can be deserialzed.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#538
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Exercise by calling deserialize
    result = task_gen.deserialize(obj=input, obj_type=Optional[List[int]])

    # Verify the result matches the expected result
    assert result == expected


def test_deserialize_multi_dimensional_collection_raises_error(client_env) -> None:
    """Ensure an error is raised if a multi-dimensional collection is passed.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#552
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Define the items to deserialize
    items_to_deserialize = '[("0", "1"), ("2", "3")]'

    # Exercise & Verify an error is raised
    with pytest.raises(
        TypeError,
        match="This version of Task Generator cannot serialize multi-dimensional collection",
    ):
        task_gen.deserialize(
            obj=items_to_deserialize, obj_type=List[Tuple[int, ...]]
        )  # pytype: disable=wrong-arg-types


def test_deserialize_collection_without_item_annotation_raises_error(
    client_env,
) -> None:
    """Ensure a collection without an item annotation like ``list`` raises an error.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#572
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Exercise by calling deserialize & Verify an error is raised
    with pytest.raises(
        TypeError,
        match=f"annotation ``<class 'list'>`` does not provide enough information",
    ):
        task_gen.deserialize(obj='["1", "2", "3"]', obj_type=list)


@pytest.mark.parametrize(
    ["optional_type", "serialized", "expected"],
    [
        [Optional[int], "None", None],
        [Optional[int], "1", 1],
        [Optional[str], "None", None],
        [Optional[str], "foo", "foo"],
        [Optional[float], "None", None],
        [Optional[float], "1.0", 1.0],
        [type(None), "None", None],
        [Optional[List[int]], "None", None],
        [Optional[List[int]], '["1", "2"]', [1, 2]],
        [Optional[FakeYearRange], "None", None],
        [Optional[FakeYearRange], "1990:2020:2050", FakeYearRange(1990)],
    ],
)
def test_deserialize_optional(
    client_env, optional_type: Any, serialized: str, expected: Any
) -> None:
    """Ensure an optional type can be deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#588
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Exercise by calling deserialize
    result = task_gen.deserialize(obj=serialized, obj_type=optional_type)

    # Verify the result was deserialized correctly
    assert result == expected


def test_deserialize_empty_collection(client_env) -> None:
    """Ensure empty collections are returned still empty.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#602
    """
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool_name="test_tool"
    )

    # Define the expected result
    expected_result = list()

    # Exercise
    result = task_gen.deserialize(obj=str(list()), obj_type=List[str])

    # Verify
    assert result == expected_result


def test_deserialize_built_in_collections_in_str(
    client_env,
) -> None:
    """Ensure the built-in collection types can be deserialized."""
    # Instantiate the TaskGenerator
    tool = Tool("test_tool")
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # Exercise by calling deserialize on the items_to_deserialize, having cast them to
    # the collection_type
    result = task_gen.deserialize(obj='["a","b"]', obj_type=List[str])
    # Verify the result matches the expected result (cast as the collection type)
    assert result == ["a", "b"]

    result = task_gen.deserialize(obj="[a,b]", obj_type=List[str])
    assert result == ["a", "b"]

    result = task_gen.deserialize(obj="[1,2]", obj_type=List[int])
    assert result == [1, 2]

    result = task_gen.deserialize(
        obj='["1990:2020:2050", "1990:2020:2050"]', obj_type=List[FakeYearRange]
    )
    assert result == [FakeYearRange(1990), FakeYearRange(1990)]


def test_simple_task_array(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get a good looking command string for an array task."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool_name = f"test_tool_array_{randint(0, 1000)}"

    @task_generator.task_generator(serializers={}, tool_name=tool_name)
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    tasks = simple_function.create_tasks(
        cluster_name="sequential",
        compute_resources=compute_resources,
        foo=[1, 2],
        bar="baz",
    )
    # verify there are two tasks
    assert len(tasks) == 2

    # Verify task name
    for i in range(1, 2):
        # Verify command
        expected_command = (
            f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
            f" --module_name tests.worker_node.test_task_generator"
            " --func_name simple_function"
            f" foo='{i}'"
            " bar='baz'"
        )

        assert tasks[i - 1].command == expected_command


def test_upstream_task_array(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get expected upstream tasks."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool_name = f"test_tool_array_{randint(0, 1000)}"

    @task_generator.task_generator(serializers={}, tool_name=tool_name)
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    upstream_tasks = simple_function.create_tasks(
        cluster_name="sequential",
        compute_resources=compute_resources,
        foo=[1, 2],
        bar="baz",
    )

    downstream_tasks = simple_function.create_tasks(
        cluster_name="sequential",
        compute_resources=compute_resources,
        foo=[1, 2],
        bar="buzz",
        upstream_tasks=upstream_tasks,
    )

    for downstream_task in downstream_tasks:
        assert downstream_task.upstream_tasks == set(upstream_tasks)


def test_array_list_arg(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get a good looking command string for an array task."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool_name = f"test_tool_array_{randint(0, 1000)}"

    @task_generator.task_generator(serializers={}, tool_name=tool_name)
    def simple_function(foo: int, bar: List[str]) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    tasks = simple_function.create_tasks(
        cluster_name="sequential",
        compute_resources=compute_resources,
        foo=[1, 2],
        bar=[["a", "b"]],
    )
    # verify there are two tasks
    assert len(tasks) == 2

    # Verify task name
    for i in range(1, 2):
        # Verify command
        expected_command = (
            f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
            f" --module_name tests.worker_node.test_task_generator"
            " --func_name simple_function"
            f" foo='{i}'"
            " bar='[a,b]'"
        )

        assert tasks[i - 1].command == expected_command


def test_fhs_serializers(client_env) -> None:
    """Test the serializers for the FHS task generator."""
    from tests.worker_node.task_generator_fhs import (
        YearRange,
        Versions,
        FHSFileSpec,
        FHSDirSpec,
        VersionMetadata,
        Quantiles,
        versions_to_list,
        versions_from_list,
        quantiles_to_list,
        quantiles_from_list,
    )

    yr = YearRange(2020, 2021)
    v = v = Versions("1.0", "2.0")
    fSpec = FHSFileSpec("/path/to/file")
    dSpec = FHSDirSpec("/path/to/dir")
    vm = VersionMetadata("1.0")
    q = Quantiles(0.1, 0.9)

    tool = Tool("test_tool")
    testing_serializer = {
        YearRange: (str, YearRange.parse_year_range),
        Versions: (versions_to_list, versions_from_list),
        FHSFileSpec: (str, FHSFileSpec.parse),
        FHSDirSpec: (str, FHSDirSpec.parse),
        VersionMetadata: (str, VersionMetadata.parse_version),
        Quantiles: (quantiles_to_list, quantiles_from_list),
    }

    def simple_function(
        yr: YearRange,
        v: Versions,
        fSpec: FHSFileSpec,
        dSpec: FHSDirSpec,
        vm: VersionMetadata,
        q: Optional[Quantiles],
    ) -> None:
        """Simple task_function."""
        pass

    tg = task_generator.TaskGenerator(
        task_function=my_func,
        serializers=testing_serializer,
        tool_name="test_tool",
        default_cluster_name="sequential",
    )
    r1 = tg.serialize(yr, YearRange)
    assert r1 == "2020-2021"
    r2 = tg.serialize(v, Versions)
    assert r2 == "[1.0, 2.0]"
    r3 = tg.serialize(fSpec, FHSFileSpec)
    assert r3 == "/path/to/file"
    r4 = tg.serialize(dSpec, FHSDirSpec)
    assert r4 == "/path/to/dir"
    r5 = tg.serialize(vm, VersionMetadata)
    assert r5 == "1.0"
    r6 = tg.serialize(q, Quantiles)
    assert r6 == "[0.1, 0.9]"

    # verify deserialization
    assert yr == tg.deserialize(r1, YearRange)
    assert v == tg.deserialize(r2, Versions)
    assert fSpec == tg.deserialize(r3, FHSFileSpec)
    assert dSpec == tg.deserialize(r4, FHSDirSpec)
    assert vm == tg.deserialize(r5, VersionMetadata)
    assert q == tg.deserialize(r6, Quantiles)


def test_fhs_deserizalizers(client_env, monkeypatch) -> None:
    """Test special case for FHS after forcing deserizatizer to take str only."""
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    tool = Tool("test_tool")

    from tests.worker_node.task_generator_fhs import (
        YearRange,
        Versions,
        FHSFileSpec,
        FHSDirSpec,
        VersionMetadata,
        Quantiles,
        versions_to_list,
        versions_from_list,
        quantiles_to_list,
        quantiles_from_list,
    )

    testing_serializer = {
        YearRange: (str, YearRange.parse_year_range),
        Versions: (versions_to_list, versions_from_list),
        FHSFileSpec: (str, FHSFileSpec.parse),
        FHSDirSpec: (str, FHSDirSpec.parse),
        VersionMetadata: (str, VersionMetadata.parse_version),
        Quantiles: (quantiles_to_list, quantiles_from_list),
    }

    def test_function(
        yr: YearRange,
        v: Versions,
        fSpec: FHSFileSpec,
        dSpec: FHSDirSpec,
        vm: VersionMetadata,
        q: Optional[Quantiles],
    ) -> None:
        """Simple task_function."""
        pass

    tg = task_generator.TaskGenerator(
        task_function=test_function,
        serializers=testing_serializer,
        tool_name="test_tool",
    )

    q = Quantiles(0.1, 0.9)
    q_s = tg.serialize(q, Quantiles)
    assert q_s == "[0.1, 0.9]"
    assert q == tg.deserialize(q_s, Quantiles)

    v = Versions("1.0")
    v_s = tg.serialize(v, Versions)
    assert v_s == "[1.0]"
    assert v == tg.deserialize(v_s, Versions)


def test_fhs_task(client_env, monkeypatch) -> None:
    """Test the serializers for the FHS task generator."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    from tests.worker_node.task_generator_fhs import (
        YearRange,
        Versions,
        FHSFileSpec,
        FHSDirSpec,
        VersionMetadata,
        Quantiles,
        versions_to_list,
        versions_from_list,
        quantiles_to_list,
        quantiles_from_list,
    )

    yr = YearRange(2020, 2021)
    v = Versions("1.0", "2.0")
    fSpec = FHSFileSpec("/path/to/file")
    dSpec = FHSDirSpec("/path/to/dir")
    vm = VersionMetadata("1.0")
    q = Quantiles(0.1, 0.9)

    tool = Tool("test_tool")
    testing_serializer = {
        YearRange: (str, YearRange.parse_year_range),
        Versions: (versions_to_list, versions_from_list),
        FHSFileSpec: (str, FHSFileSpec.parse),
        FHSDirSpec: (str, FHSDirSpec.parse),
        VersionMetadata: (str, VersionMetadata.parse_version),
        Quantiles: (quantiles_to_list, quantiles_from_list),
    }

    @task_generator.task_generator(
        tool_name="test_tool", serializers=testing_serializer, naming_args=["yr", "v"]
    )
    def simple_function(
        yr: YearRange,
        v: Versions,
        fSpec: FHSFileSpec,
        dSpec: FHSDirSpec,
        vm: VersionMetadata,
        q: Optional[Quantiles],
    ) -> None:
        """Simple task_function."""
        pass

    task1 = simple_function.create_task(
        cluster_name="sequential",
        compute_resources={},
        yr=yr,
        v=v,
        fSpec=fSpec,
        dSpec=dSpec,
        vm=vm,
        q=q,
    )
    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " yr='2020-2021'"
        " v='[1.0, 2.0]'"
        " fSpec='/path/to/file'"
        " dSpec='/path/to/dir'"
        " vm='1.0'"
        " q='[0.1, 0.9]'"
    )
    assert task1.name == "simple_function:yr=2020-2021:v=1.0,_2.0"
    assert task1.command == expected_command

    # test optional args
    task2 = simple_function.create_task(
        cluster_name="sequential",
        compute_resources={},
        yr=yr,
        v=v,
        fSpec=fSpec,
        dSpec=dSpec,
        vm=vm,
    )
    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " yr='2020-2021'"
        " v='[1.0, 2.0]'"
        " fSpec='/path/to/file'"
        " dSpec='/path/to/dir'"
        " vm='1.0'"
        " q='None'"
    )
    assert task2.name == "simple_function:yr=2020-2021:v=1.0,_2.0"
    assert task2.command == expected_command

    # test array task
    tasks = simple_function.create_tasks(
        cluster_name="sequential",
        compute_resources={},
        yr=yr,
        v=v,
        fSpec=fSpec,
        dSpec=dSpec,
        vm=vm,
        q=[q, None],
    )
    assert len(tasks) == 2
    # Verify command
    expected_command1 = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " yr='2020-2021'"
        " v='[1.0, 2.0]'"
        " fSpec='/path/to/file'"
        " dSpec='/path/to/dir'"
        " vm='1.0'"
        " q='[0.1, 0.9]'"
    )
    assert tasks[0].command == expected_command1
    # Verify command
    expected_command2 = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " yr='2020-2021'"
        " v='[1.0, 2.0]'"
        " fSpec='/path/to/file'"
        " dSpec='/path/to/dir'"
        " vm='1.0'"
        " q='None'"
    )
    assert tasks[1].command == expected_command2


def test_task_template_not_generated_when_instance_only_generated_once(
    client_env, monkeypatch
) -> None:
    """Test the self._task_template only gererate once."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    def test_func(foo: int) -> None:
        """Simple task_function."""
        pass

    tg = task_generator.TaskGenerator(
        default_cluster_name="sequential",
        task_function=test_func,
        serializers={},
        tool_name="test_tool",
    )
    # test
    assert tg._task_template is None

    # create a task
    tg.create_task(cluster_name="sequential", compute_resources={}, foo=1)
    # verify the task_template is the same
    tt = tg._task_template
    assert tt is not None
    tg_task_template_id = tt.id
    # create another task
    tg.create_task(compute_resources={}, foo=2)
    # verify the task_template is the same
    assert tg._task_template is tt
    assert tg_task_template_id == tg._task_template.id


def test_get_tasks_by_node_args_simple(client_env, monkeypatch):
    """Test the get_tasks_by_node_args method with simple input."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()

    def test_func(foo: int) -> None:
        """Simple task_function."""
        pass

    tg = task_generator.TaskGenerator(
        default_cluster_name="sequential",
        task_function=test_func,
        serializers={},
        tool_name="test_tool",
    )

    # create a task
    t1 = tg.create_task(compute_resources={}, foo=1)
    # create another task
    t2 = tg.create_task(compute_resources={}, foo=2)
    wf.add_tasks([t1, t2])

    # use get_tasks_by_node_args to find t1
    tasks = task_generator.get_tasks_by_node_args(
        workflow=wf, node_args_dict={"foo": 1}, task_generator=tg
    )
    assert len(tasks) == 1
    assert t1 in tasks
    # use get_tasks_by_node_args to find t2
    tasks = task_generator.get_tasks_by_node_args(
        workflow=wf, node_args_dict={"foo": 2}, task_generator=tg
    )
    assert len(tasks) == 1
    assert t2 in tasks


def test_get_tasks_by_node_args_list(client_env, monkeypatch):
    """Test the get_tasks_by_node_args method with list input."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()

    def test_func(foo: List[int]) -> None:
        """Simple task_function."""
        pass

    tg = task_generator.TaskGenerator(
        default_cluster_name="sequential",
        task_function=test_func,
        serializers={},
        tool_name="test_tool",
    )

    # create a task
    t1 = tg.create_task(compute_resources={}, foo=[1, 2])
    # create another task
    t2 = tg.create_task(compute_resources={}, foo=[2])
    wf.add_tasks([t1, t2])

    # use get_tasks_by_node_args to find t1
    tasks = task_generator.get_tasks_by_node_args(
        workflow=wf, node_args_dict={"foo": [1, 2]}, task_generator=tg
    )
    assert len(tasks) == 1
    assert t1 in tasks
    # use get_tasks_by_node_args to find t2
    tasks = task_generator.get_tasks_by_node_args(
        workflow=wf, node_args_dict={"foo": [2]}, task_generator=tg
    )
    assert len(tasks) == 1
    assert t2 in tasks


def test_get_tasks_by_node_args_obj(client_env, monkeypatch):
    """Test the get_tasks_by_node_args method with serializers."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()

    def test_func(foo: FakeYearRange) -> None:
        """Simple task_function."""
        pass

    tg = task_generator.TaskGenerator(
        default_cluster_name="sequential",
        task_function=test_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool_name="test_tool",
    )

    # create a task
    fake_year_range1 = FakeYearRange.parse_year_range("2010:2020:2030")
    fake_year_range2 = FakeYearRange.parse_year_range("2020:2030:2040")
    t1 = tg.create_task(compute_resources={}, foo=fake_year_range1)
    # create another task
    t2 = tg.create_task(compute_resources={}, foo=fake_year_range2)
    wf.add_tasks([t1, t2])

    # use get_tasks_by_node_args to find t1
    tasks = task_generator.get_tasks_by_node_args(
        workflow=wf, node_args_dict={"foo": fake_year_range1}, task_generator=tg
    )
    assert len(tasks) == 1
    assert t1 in tasks
    # use get_tasks_by_node_args to find t2
    tasks = task_generator.get_tasks_by_node_args(
        workflow=wf, node_args_dict={"foo": fake_year_range2}, task_generator=tg
    )
    assert len(tasks) == 1
    assert t2 in tasks


def test_is_multidimensional_type():
    from jobmon.core.task_generator import _is_multidimensional_type

    assert _is_multidimensional_type(List[int]) is False
    assert _is_multidimensional_type(List[List[int]]) is True
    assert _is_multidimensional_type(List[Tuple[int]]) is True
    assert _is_multidimensional_type(List[Tuple[int, int]]) is True
    assert _is_multidimensional_type(List[Tuple[int, ...]]) is True
    assert _is_multidimensional_type(Tuple[Tuple[List[int]]]) is True


def test_naming_func(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that the name only includes the expected naming args.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#711
    """
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    def naming_func(prefix, kwargs_for_name) -> str:
        return f"{prefix}:foo={kwargs_for_name['foo']}"

    @task_generator.task_generator(
        default_cluster_name="sequential",
        serializers={},
        tool_name="test_tool",
        naming_args=["foo"],
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="baz"
    )
    # Verify task name matches the expected
    assert task.name == "simple_function:foo=1"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} {task_generator.TASK_RUNNER_SUB_COMMAND}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " foo='1'"
        " bar='baz'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources


def test_computer_resource_type_protection(client_env, monkeypatch):
    """Verify that the compute_resources parameter is a dict."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )
    tool = Tool("test_tool")

    @task_generator.task_generator(
        default_cluster_name="sequential",
        serializers={},
        tool_name="test_tool",
        naming_args=["foo"],
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    # Exercise & Verify an error is raised
    with pytest.raises(
        TypeError,
        match="Expected a dictionary for compute_resources, but got <class 'str'>.",
    ):
        simple_function.create_task(compute_resources="whatever", foo=1, bar="baz")

    # Exercise & Verify an error is raised
    with pytest.raises(
        TypeError,
        match="Expected a dictionary for compute_resources, but got <class 'str'>.",
    ):
        simple_function.create_tasks(
            compute_resources="whatever", foo=[1, 2], bar="baz"
        )


def test_task_generator_doc(client_env, monkeypatch):
    """Test the doc feature."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    import os

    func_path = script_dir = (
        os.path.dirname(os.path.abspath(__file__)) + "/task_generator_funcs.py"
    )

    tool = Tool("test_tool")

    from docutils.parsers.rst import directives
    from docutils.core import publish_string

    directives.register_directive("task_generator", TaskGeneratorDocumenter)
    rst = f"""
        This is a simple document.

        .. task_generator:: task_generator_funcs:simple_function
           :optional: {func_path}

        This is more content after the directive.
        """
    output = publish_string(rst, writer_name="html")
    assert "Simple task_function." in output.decode("utf-8")


def test_task_generator_docs(client_env, monkeypatch):
    """Test the doc feature."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    import os

    func_path = script_dir = (
        os.path.dirname(os.path.abspath(__file__)) + "/task_generator_funcs.py"
    )

    tool = Tool("test_tool")

    from docutils.parsers.rst import directives
    from docutils.core import publish_string

    directives.register_directive(
        "task_generator_module", TaskGeneratorModuleDocumenter
    )

    rst = f"""
        This is a simple document.

        .. task_generator_module:: task_generator_funcs
           :optional: {func_path}

        This is more content after the directive.
        """
    output = publish_string(rst, writer_name="html")
    assert "Simple task_function." in output.decode("utf-8")
    assert "Simple task_function with special chars." in output.decode("utf-8")
    assert "Simple task_function with a serializer." in output.decode("utf-8")


def test_default_computer_resource(client_env, monkeypatch):
    """Test the default compute resource."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={},
        tool_name="test_tool",
        naming_args=["foo"],
        default_cluster_name="sequential",
        default_compute_resources={"cluster_name": "sequential", "queue": "null.q"},
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    # create a task
    t1 = simple_function.create_task(cluster_name="sequential", foo=1, bar="baz")
    # create a workflow
    wf = tool.create_workflow()
    wf.add_task(t1)
    wf.bind()
    wf._bind_tasks()
    # verify the default compute resources are set at the task_template level
    assert (
        simple_function._task_template._active_task_template_version.default_cluster_name
        == "sequential"
    )
    assert (
        simple_function._task_template._active_task_template_version.default_compute_resources_set
        == {"sequential": {"cluster_name": "sequential", "queue": "null.q"}}
    )

    # verify the task has the default compute resources
    assert t1.compute_resources == {"cluster_name": "sequential", "queue": "null.q"}
    assert t1.resource_scales == {"memory": 0.5, "runtime": 0.5}


def test_default_computer_resource_yaml(client_env, monkeypatch):
    """Test the default compute resource."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    import os

    rsc_path = os.path.dirname(os.path.abspath(__file__)) + "/cluster_resources.yaml"

    tool = Tool("test_tool")

    @task_generator.task_generator(
        serializers={},
        tool_name="test_tool",
        naming_args=["foo"],
        yaml_file=rsc_path,
        default_cluster_name="sequential",
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    # create a task
    t1 = simple_function.create_task(cluster_name="sequential", foo=1, bar="baz")
    # create a workflow
    wf = tool.create_workflow()
    wf.add_task(t1)
    wf.bind()
    wf._bind_tasks()
    # verify the task has the default compute resources
    assert t1.compute_resources == {
        "m_mem_free": "3G",
        "max_runtime_seconds": "(60 * 60 * 4)",
        "num_cores": 1,
        "queue": "null.q",
    }
    assert t1.resource_scales == {"memory": 0.2, "runtime": 0.3}


def test_rsc_overide(client_env, monkeypatch):
    """Test the default compute resource."""
    # Set up function
    monkeypatch.setattr(
        task_generator,
        "_find_executable_path",
        Mock(return_value=task_generator.TASK_RUNNER_NAME),
    )

    tool = Tool("test_tool1")

    @task_generator.task_generator(
        default_cluster_name="dummy",
        default_compute_resources={"queue": "null.q", "core": 1},
        serializers={},
        tool_name="test_tool1",
        naming_args=["foo"],
    )
    def simple_function1(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    # task without cluster name should use the tg cluster name
    t1 = simple_function1.create_task(foo=1, bar="baz")
    wf1 = tool.create_workflow()
    wf1.add_task(t1)
    wf1.bind()
    wf1._bind_tasks()
    # verify the task has the default compute resources
    assert t1.cluster_name == "dummy"
    assert t1.compute_resources == {"queue": "null.q", "core": 1}

    # task with cluster name should use the task cluster name
    t2 = simple_function1.create_task(
        cluster_name="sequential",
        compute_resources={"queue": "null.q", "core": 2},
        foo=1,
        bar="baz",
    )
    wf2 = tool.create_workflow()
    wf2.add_task(t2)
    wf2.bind()
    wf2._bind_tasks()
    # verify the task has the default compute resources
    assert t2.cluster_name == "sequential"
    assert t2.compute_resources == {"queue": "null.q", "core": 2}

    # task with the same cluster name as the tg should use the tg resources
    t3 = simple_function1.create_task(cluster_name="dummy", foo=1, bar="baz")
    wf3 = tool.create_workflow()
    wf3.add_task(t3)
    wf3.bind()
    wf3._bind_tasks()
    # verify the task has the default compute resources
    assert t3.cluster_name == "dummy"
    assert t3.compute_resources == {"queue": "null.q", "core": 1}

    # task use default compute resources
    t4 = simple_function1.create_task(foo=1, bar="baz")
    wf4 = tool.create_workflow()
    wf4.add_task(t4)
    wf4.bind()
    wf4._bind_tasks()
    # verify the task has the default compute resources
    assert t4.cluster_name == "dummy"
    assert t4.compute_resources == {"queue": "null.q", "core": 1}
