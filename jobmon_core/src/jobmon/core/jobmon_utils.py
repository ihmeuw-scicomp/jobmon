import os
import subprocess
from typing import Optional, Union


def true_path(
    file_or_dir: Optional[Union[bytes, str]] = None, executable: Optional[str] = None
) -> str:
    """Get true path to file or executable.

    Args:
        file_or_dir (str): partial file path, to be expanded as per the current
            user
        executable (str): the name of an executable, which will be resolved
            using "which"

    Specify one of the two arguments, not both.
    """
    if file_or_dir is not None:
        f = file_or_dir
    elif executable is not None:
        f = subprocess.check_output(["which", str(executable)])
    else:
        raise ValueError("true_path: file_or_dir and executable " "cannot both be null")

    # Be careful, in python 3 check_output returns bytes
    if not isinstance(f, str):
        f = f.decode("utf-8")
    f = os.path.abspath(os.path.expanduser(f))
    return f.strip(" \t\r\n")
