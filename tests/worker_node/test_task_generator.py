import pytest
from typing import Any, List, Optional, Tuple
from unittest.mock import Mock

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

def test_simple_task(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we get a good looking command string.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#642
    """
    # Set up function
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    tool = Tool()
    @task_generator.task_generator(serializers={}, tool=tool)
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="baz"
    )

    # Verify task name
    assert task.name == "simple_function:foo=1:bar=baz"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} "
        f" --expected_jobmon_version {core_version}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " --args 'foo=1;bar=baz'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources


def test_list_args(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify we can properly pass args that serialize as lists.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#672
    """
    # Set up functino
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    tool = Tool()
    @task_generator.task_generator(serializers={}, tool=tool)
    def list_function(foo: List[str], bar: List[str]) -> None:
        """Example task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = list_function.create_task(
        compute_resources=compute_resources, foo=["a", "b"], bar=["c", "d"]
    )

    # Verify task name
    assert task.name == "list_function:foo=a,b:bar=c,d"

    # Verify command
    expected_command = (
        f"{task_generator.TASK_RUNNER_NAME} "
        f" --expected_jobmon_version {core_version}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name list_function"
        " --args 'foo=[a, b];bar=[c, d]'"
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
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )
    tool = Tool()
    @task_generator.task_generator(
        serializers={}, tool=tool, naming_args=naming_args
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
        f"{task_generator.TASK_RUNNER_NAME} "
        f" --expected_jobmon_version {core_version}"
        f" --module_name tests.worker_node.test_task_generator"
        " --func_name simple_function"
        " --args 'foo=1;bar=baz'"
    )
    assert task.command == expected_command
    assert task.compute_resources == compute_resources


def test_max_attempts(client_env, monkeypatch: pytest.fixture) -> None:
    """Verify that we pass max_attempts correctly to the task.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#745
    """
    # Set up function
    monkeypatch.setattr(
        task_generator, "_find_executable_path", Mock(return_value=task_generator.TASK_RUNNER_NAME)
    )

    max_attempts = 40
    tool = Tool()
    @task_generator.task_generator(
        serializers={}, tool=tool, max_attempts=max_attempts
    )
    def simple_function(foo: int, bar: str) -> None:
        """Simple task_function."""
        pass

    compute_resources = {}

    # Exercise
    task = simple_function.create_task(
        compute_resources=compute_resources, foo=1, bar="baz"
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

    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Exercise by calling serialize
    result = task_gen.serialize(simple_type, type(simple_type))

    # Verify the result is simply the stringified simple_type
    assert result == str(simple_type)

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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
    )

    # Cast the items as the collection_type
    items_to_serialize = [1.0, False, "something", FakeYearRange.parse_year_range("2010:2020:2030")]

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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Define the items to serialize
    items_to_serialize = ["1.0", "False", "something", "2010:2020:2030"]

    # Cast the items as a multi-dimensional collection
    items_to_serialize = [items_to_serialize]

    # Exercise & Verify an error is raised
    with pytest.raises(TypeError, match="Cannot serialize multi-dimensional collection"):
        task_gen.serialize(obj=items_to_serialize, expected_type=List[List[str]])

def test_serialize_optional(client_env) -> None:
    """Ensure an optional type can be serialized.

    Converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#335
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Define the expected result
    expected_result = []

    # Exercise
    result = task_gen.serialize(list(), List[str])

    # Verify
    assert result == expected_result

def test_optional_collection(client_env) -> None:
    """Ensure an optional collection with data is serialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#363
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Define the items to serialize
    items_to_serialize = [1, 2, 3]

    # Exercise
    result = task_gen.serialize(items_to_serialize, Optional[List[int]])

    # Verify the result matches the expected result
    assert result == ["1", "2", "3"]

    # Exercise
    result = task_gen.serialize(None, Optional[List[int]])

    # Verify the result is simply the stringified None
    assert result == "None"

def test_no_internal_type_raises_error(client_env) -> None:
    """Ensure a collection with no internal type raises an error (``tuple``).

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#398
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
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
def test_deserialize_simple_type(client_env, simple_type: str, expected_result: Any) -> None:
    """Ensure a known simple type is properly deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#410
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Exercise by calling deserialize
    result = task_gen.deserialize(obj=simple_type, obj_type=type(expected_result))

    # Verify the result matches the expected result
    assert result == expected_result

def test_deserializer_specified_type(client_env) -> None:
    """Ensure a serializer-specified type is properly deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#423
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Instantiate an unknown, non-simple type
    my_obj = FakeYearRange.parse_year_range("2010:2020:2030")

    # Exercise by calling deserialize & Verify an error is raised
    with pytest.raises(TypeError, match="Cannot deserialize unknown type FakeYearRange"):
        task_gen.deserialize(obj=str(my_obj), obj_type=FakeYearRange)


@pytest.mark.parametrize(
    "item_type, items_to_deserialize, expected_result",
    [
        [int, ["1", "3", "5"], [1, 3, 5]],
        [float, ["1.0", "3.0", "5.0"], [1.0, 3.0, 5.0]],
        [str, ["one", "three", "five"], ["one", "three", "five"]],
        [bool, ["True", "False"], [True, False]],
        [
            FakeYearRange,
            ["2010:2020:2030", "2040:2050:2060"],
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
    )

    # Exercise by calling deserialize on the items_to_deserialize, having cast them to
    # the collection_type
    result = task_gen.deserialize(
        obj=items_to_deserialize, obj_type=List[item_type]
    )

    # Verify the result matches the expected result (cast as the collection type)
    assert result == expected_result

def test_deserialize_multi_annotated_collection(client_env) -> None:
    """Ensure a multi-annotated collection can be deserialized.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#511
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
    )

    # Define the items to deserialize
    items_to_deserialize = ["0.1", "2010:2020:2030"]

    # Define the expected result
    expected_result = (0.1, FakeYearRange.parse_year_range("2010:2020:2030"))

    # Exercise
    result = task_gen.deserialize(
        obj=items_to_deserialize, obj_type=Tuple[float, FakeYearRange]
    )

    # Verify the result matches the expected result
    assert result == expected_result


@pytest.mark.parametrize(
        ["input", "expected"],
        [["None", None], ["1", [1]], [["1", "2"], [1, 2]], [[], []]],
    )
def test_deserialize_optional_collection(
    client_env, input, expected: Optional[List[int]]
) -> None:
    """Ensure an optional collection can be deserialzed.

    converted from https://stash.ihme.washington.edu/projects/FHSENG/repos/fhs-lib-orchestration-interface/browse/tests/test_task_generator.py#538
    """
    # Instantiate the TaskGenerator
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
    )

    # Define the items to deserialize
    items_to_deserialize = [("0", "1"), ("2", "3")]

    # Exercise & Verify an error is raised
    with pytest.raises(TypeError, match="Cannot deserialize multi-dimensional collection"):
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Exercise by calling deserialize & Verify an error is raised
    with pytest.raises(
        TypeError,
        match=f"annotation ``<class 'list'>`` does not provide enough information",
    ):
        task_gen.deserialize(obj=["1", "2", "3"], obj_type=list)


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
            [Optional[List[int]], ["1", "2"], [1, 2]],
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func,
        serializers={FakeYearRange: (str, FakeYearRange.parse_year_range)},
        tool=tool,
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
    tool = Tool()
    task_gen = task_generator.TaskGenerator(
        task_function=my_func, serializers={}, tool=tool
    )

    # Define the expected result
    expected_result = list()

    # Exercise
    result = task_gen.deserialize(obj=list(), obj_type=List[str])

    # Verify
    assert result == expected_result

