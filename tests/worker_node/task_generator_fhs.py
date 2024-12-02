import ast

from typing import Optional, Callable, List, Any, Union
import os

from jobmon.core.task_generator import task_generator

# Get the full path of the current script
script_path = os.path.abspath(__file__)

# Resolve any symbolic links (if necessary)
full_script_path = os.path.realpath(script_path)

fhs_generator_funcs_path = os.path.join(
    os.path.dirname(script_path), "task_generator_fhs.py"
)


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
        if type(version) in [list, tuple, set]:
            self._version = [ast.literal_eval(str(v)) for v in version]
        else:
            self._version = [str(version)]

    def __eq__(self, other: Any) -> bool:
        return self._version == other._version


def versions_to_list(versions: Versions) -> str:
    """Serializer for VersionMetadata that produces a list of strings."""
    if isinstance(versions._version, str):
        return f"[{versions._version}]"
    if isinstance(versions._version, list):
        v_str = []
        for v in versions._version:
            v_str.append(str(v))
        return f"[{', '.join(v_str)}]"


def versions_from_list(versions: str) -> Versions:
    """Deserializer for VersionMetadata that takes a list of strings."""

    if versions[0] == "[" and versions[-1] == "]":
        versions = versions[1:-1]
    vs = versions.split(", ")
    versions = []

    for v in vs:
        versions.append(ast.literal_eval(v))

    return Versions(*versions)


class Quantiles:
    """A Fake class representing a range of quantiles."""

    def __init__(self, lower: Union[float, str], upper: [float, str]) -> None:
        self.lower = float(lower)
        self.upper = float(upper)

    def __eq__(self, other: Any) -> bool:
        return self.lower == other.lower and self.upper == other.upper


def quantiles_to_list(quantiles: Quantiles) -> str:
    """Serializer for Quantiles that produces a list of strings."""
    return f"[{quantiles.lower}, {quantiles.upper}]"


def quantiles_from_list(q: str) -> Quantiles:
    """Deserializer for Quantiles that takes a list of strings."""
    quantiles = ast.literal_eval(q)
    return Quantiles(quantiles[0], quantiles[1])


class FHSFileSpec:
    """A Fake class representing a file specification."""

    def __init__(self, path: str, **kwargs) -> None:
        self.path = path

    @staticmethod
    def parse(file_spec: str):
        """Parse a file specification."""
        return FHSFileSpec(file_spec)

    def __eq__(self, other: Any) -> bool:
        return self.path == other.path

    def __str__(self):
        return self.path


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

    def __str__(self):
        return self.path


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

    def __str__(self):
        return self.version


def fhs_task_generator(
    tool_name: str,
    module_source_path: str = full_script_path,
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
        module_source_path=module_source_path,
    )


@fhs_task_generator(tool_name="test_tool", naming_args=["version"])
def fhs_simple_function(
    yr: YearRange,
    v: Versions,
    fSpec: FHSFileSpec,
    dSpec: FHSDirSpec,
    vm: VersionMetadata,
    q: Optional[Quantiles] = None,
) -> None:
    """Simple task_function."""
    print(f"YearRange: {yr}")
    print(f"Version: {v}")
    print(f"FHSFileSpec: {fSpec}")
    print(f"FHSDirSpec: {dSpec}")
    print(f"VersionMetadata: {vm}")
    if q is not None:
        print(f"Quantiles: {q}")
