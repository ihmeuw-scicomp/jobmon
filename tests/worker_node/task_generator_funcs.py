import ast
import html
import os
import pickle
from typing import List

from jobmon.core import task_generator
from jobmon.client.api import Tool

# Get the full path of the current script
script_path = os.path.abspath(__file__)

# Resolve any symbolic links (if necessary)
full_script_path = os.path.realpath(script_path)


@task_generator.task_generator(
    serializers={},
    tool_name="test_tool",
    module_source_path=full_script_path,
    max_attempts=1,
    naming_args=["foo"],
)
def simple_function(foo: int, bar: List[str] = []) -> None:
    """Simple task_function."""
    print(f"foo: {foo}")
    print(f"bar: {bar}")


class TestYear:
    """A fake YearRange class for testing"""

    def __init__(self, year: int) -> None:
        self.year = year

    @staticmethod
    def parse_year(year: str):
        """Parse a year range."""
        return TestYear(int(year))

    def __str__(self) -> str:
        return str(self.year)

    def __eq__(self, other):
        return self.year == other.year


test_year_serializer = {TestYear: (str, TestYear.parse_year)}


@task_generator.task_generator(
    serializers=test_year_serializer,
    tool_name="test_tool",
    module_source_path=full_script_path,
    max_attempts=1,
    naming_args=["year"],
)
def simple_function_with_serializer(year: TestYear) -> None:
    """Simple task_function with a serializer."""
    print(f"year: {year}")


@task_generator.task_generator(
    default_cluster_name="slurm",
    default_compute_resources={"cpu": 1, "mem": 1, "queue": "all.q", "project": "proj_scicomp"},
    serializers=test_year_serializer,
    tool_name="test_tool",
    module_source_path=full_script_path,
    max_attempts=1,
    naming_args=["year"],
)
def simple_function_with_serializer_rsc(year: TestYear) -> None:
    """Simple task_function with a serializer."""
    print(f"year: {year}")


def special_char_encodeing(input: str) -> str:
    """Encode special characters."""
    return html.escape(input)


def special_char_decoding(input: str) -> str:
    """Decode special characters."""
    return html.unescape(input)


@task_generator.task_generator(
    serializers={str: (special_char_encodeing, special_char_decoding)},
    tool_name="test_tool",
    module_source_path=full_script_path,
    max_attempts=1,
    naming_args=["foo"],
)
def special_chars_function(foo: str) -> None:
    """Simple task_function with special chars."""
    print(f"foo: {foo}")

