import os

from jobmon.core import task_generator, __version__ as core_version
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
def simple_function(foo: int, bar: list = []) -> None:
    """Simple task_function."""
    print(f"foo: {foo}")
    print(f"bar: {bar}")


class TestYear:
    """A fake YearRange class for testing"""

    def __init__(self, year: int) -> None:
        self.year = year

    @staticmethod
    def parse_year(year: str) -> "FakeYearRange":
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
    """Simple task_function."""
    print(f"year: {year}")
