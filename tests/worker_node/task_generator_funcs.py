import os

from jobmon.core import task_generator, __version__ as core_version
from jobmon.client.api import Tool

# Get the full path of the current script
script_path = os.path.abspath(__file__)

# Resolve any symbolic links (if necessary)
full_script_path = os.path.realpath(script_path)

@task_generator.task_generator(serializers={}, tool_name="test_tool", module_source_path=full_script_path, max_attempts=1)
def simple_function(foo: int) -> None:
    """Simple task_function."""
    print(f"foo: {foo}")
