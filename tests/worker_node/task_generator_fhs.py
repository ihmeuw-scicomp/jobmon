from typing import Optional, Callable, Type, Dict, List, Any, Union

from jobmon.core.task_generator import task_generator
from jobmon.client.api import Tool

class YearRange:
    """A Fake class representing a range of years."""

    def __init__(self, start: int, end: int) -> None:
        self.start = start
        self.end = end

    @staticmethod
    def parse_year_range(year_range: str) -> "YearRange":
        """Parse a year range."""
        start, end = year_range.split("-")
        return YearRange(int(start), int(end))

    def __str__(self) -> str:
        return f"{self.start}-{self.end}"

    def __eq__(self, other: Any) -> bool:
        return self.start == other.start and self.end == other.end


class Versions:
    """A Fake class representing a list of versions."""

    def __init__(self, *version: Any) -> None:
        self._version = str(version)
    def __eq__(self, other: Any) -> bool:
        return self._version == other._version

def versions_to_list(versions: Versions) -> list[str]:
    """Serializer for VersionMetadata that produces a list of strings."""
    if isinstance(versions._version, str):
        return [versions._version]
    if isinstance(versions._version, list):
        return versions._version


def versions_from_list(versions: Union[List[str], str]) -> Versions:
    """Deserializer for VersionMetadata that takes a list of strings."""
    if isinstance(versions, str):
        versions = [versions]

    return Versions(*versions)


class Quantiles:
    """A Fake class representing a range of quantiles."""

    def __init__(self, lower: float, upper: float) -> None:
        self.lower = lower
        self.upper = upper

    def __eq__(self, other: Any) -> bool:
        return self.lower == other.lower and self.upper == other.upper

def quantiles_to_list(quantiles: Quantiles) -> list[str]:
    """Serializer for Quantiles that produces a list of strings."""
    return [str(quantiles.lower), str(quantiles.upper)]


def quantiles_from_list(quantiles: list[str]) -> Quantiles:
    """Deserializer for Quantiles that takes a list of strings."""
    return Quantiles(quantiles[0], quantiles[1])


class FHSFileSpec:
    """A Fake class representing a file specification."""

    def __init__(self, path: str, **kwargs) -> None:
        self.path = path

    @staticmethod
    def parse(file_spec: str) -> "FHSFileSpec":
        """Parse a file specification."""
        return FHSFileSpec(file_spec)

    def __eq__(self, other: Any) -> bool:
        return self.path == other.path


class FHSDirSpec:
    """A Fake class representing a directory specification."""

    def __init__(self, path: str, **kwargs) -> None:
        self.path = path

    @staticmethod
    def parse(dir_spec: str) -> "FHSDirSpec":
        """Parse a directory specification."""
        return FHSDirSpec(dir_spec)

    def __eq__(self, other: Any) -> bool:
        return self.path == other.path


class VersionMetadata:
    """A Fake class representing metadata about a version."""

    def __init__(self, version: str, **kwargs) -> None:
        self.version = version

    @staticmethod
    def parse_version(version: str) -> "VersionMetadata":
        """Parse a version."""
        return VersionMetadata(version)

    def __eq__(self, other: Any) -> bool:
        return self.version == other.version


def fhs_task_generator(
    tool_name: str,
    serializers: Optional[Any] = None,
    naming_args: Optional[List[str]] = None,
    max_attempts: Optional[int] = None,
) -> Callable:
    """This is a wrapper for the task_generator decorator, which includes FHS-specific types.

    This has roughly the same type signature as the init for the
    fhs_lib_orchestration_interface.lib.task_generator.TaskGenerator class, and passes most of
    its arguments down to that class untouched, although it edits the serializer dict to
    automatically include a number of FHS-specific types.

    Additionally, unlike the task_generator decorator, ``serializers`` is optional, and
    defaults to an empty dict.

    This wrapper auto-includes the following types:

    * YearRange
    * Versions
    * VersionMetadata
    * FHSFileSpec
    * FHSDirSpec
    * Quantiles
    """
    serializers = serializers or {}

    additional_serializers = {
        YearRange: (str, YearRange.parse_year_range),
        Versions: (versions_to_list, versions_from_list),
        FHSFileSpec: (str, FHSFileSpec.parse),
        FHSDirSpec: (str, FHSDirSpec.parse),
        VersionMetadata: (str, VersionMetadata.parse_version),
        Quantiles: (quantiles_to_list, quantiles_from_list),
    }

    # We want the passed in serializers to take precedence over the additional serializers
    # Passing them to `dict` in this order results in that behavior.
    serializers = dict(list(additional_serializers.items()) + list(serializers.items()))

    return task_generator(
        serializers=serializers,
        tool_name=tool_name,
        naming_args=naming_args,
        max_attempts=max_attempts,
    )

@fhs_task_generator(tool_name="test_tool", naming_args=["version"])
def simple_function(yr: YearRange, v: Versions, fSpec: FHSFileSpec, dSpec: FHSDirSpec, vm: VersionMetadata, q: Optional[Quantiles]) -> None:
    """Simple task_function."""
    print(f"YearRange: {yr}")
    print(f"Version: {v}")
    print(f"FHSFileSpec: {fSpec}")
    print(f"FHSDirSpec: {dSpec}")
    print(f"VersionMetadata: {vm}")
    print(f"Quantiles: {q}")


def create_wf():
    tool = Tool("test_tool")
    wf = tool.create_workflow()
    tool.set_default_compute_resources_from_dict(
        cluster_name="sequential", compute_resources={"queue": "null.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "null.q"}
    task = simple_function.create_task(
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


def main():
    create_wf()

if __name__ == "__main__":
    main()