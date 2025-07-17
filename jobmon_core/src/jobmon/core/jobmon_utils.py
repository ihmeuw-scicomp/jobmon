import os
import subprocess
from typing import Any, Optional, Union

import numpy as np


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


def resource_usage_converter(input: list, ci: Optional[float | str] = None) -> dict:
    """Calculate min, max, mean, median, and CI for resource usage from the input list.

    Args:
        input: List of dictionaries containing resource usage data.
        ci: Confidence interval value. If None, confidence intervals are not calculated.

    Returns:
        Dictionary containing aggregated resource usage statistics.

    Example Input:
        [
            r=None,
            m=None,
            node_id=2,
            task_id=1,
            requested_resources='{"num_cores": 2}',
            attempt_number_of_instance=1, status='D'
        ]
        Note: r and m are the runtime and memory usage of the task.

    Example Output:
        {
            "num_tasks": kwargs["num_tasks"],
            "min_mem": format_bytes(kwargs["min_mem"]),
            "max_mem": format_bytes(kwargs["max_mem"]),
            "mean_mem": format_bytes(kwargs["mean_mem"]),
            "min_runtime": kwargs["min_runtime"],
            "max_runtime": kwargs["max_runtime"],
            "mean_runtime": kwargs["mean_runtime"],
            "median_mem": format_bytes(kwargs["median_mem"]),
            "median_runtime": kwargs["median_runtime"],
            "ci_mem": kwargs["ci_mem"],
            "ci_runtime": kwargs["ci_runtime"],
        }

    Notes:
        If input is None or empty, fill each of the output with 0.
        If any r or m is None, use 0.
    """

    def format_bytes(value: Any) -> Optional[str]:
        if value is not None:
            return str(value) + "B"
        else:
            return value

    if ci is None:
        # Don't calculate confidence intervals
        calculate_ci = False
    else:
        calculate_ci = True
        if isinstance(ci, str):
            ci = float(ci)

    if input is None or len(input) == 0:
        return {
            "num_tasks": None,
            "min_mem": None,
            "max_mem": None,
            "mean_mem": None,
            "min_runtime": None,
            "max_runtime": None,
            "mean_runtime": None,
            "median_mem": None,
            "median_runtime": None,
            "ci_mem": None,
            "ci_runtime": None,
        }

    runtimes = []
    mems = []
    for row in input:
        # Handle None values for runtime
        runtime_val = row["r"] if row["r"] is not None else 0
        runtimes.append(int(runtime_val))  # type: ignore

        # Handle None values for memory
        mem_val = row["m"] if row["m"] is not None else 0
        mems.append(max(0, int(mem_val)))  # type: ignore

    num_tasks = len(runtimes)
    # set 0 to NaN; thus, numpy ignores them
    if 0 in mems:
        mems = [m for m in mems if m != 0]  # More robust removal
    if 0 in runtimes:
        runtimes = [rt for rt in runtimes if rt != 0]  # More robust removal

    min_mem, max_mem, mean_mem, median_mem = 0, 0, 0.0, 0.0
    if len(mems) > 0:
        min_mem = int(np.min(mems))
        max_mem = int(np.max(mems))
        mean_mem = round(float(np.mean(mems)), 2)
        median_mem = round(float(np.percentile(mems, 50)), 2)

    min_runtime, max_runtime, mean_runtime, median_runtime = 0, 0, 0.0, 0.0
    if len(runtimes) > 0:
        min_runtime = int(np.min(runtimes))
        max_runtime = int(np.max(runtimes))
        mean_runtime = round(float(np.mean(runtimes)), 2)
        median_runtime = round(float(np.percentile(runtimes, 50)), 2)

    ci_mem: Optional[list[Optional[float]]] = [None, None]
    ci_runtime: Optional[list[Optional[float]]] = [None, None]
    if calculate_ci and ci is not None:
        ci_float = float(ci) if isinstance(ci, str) else ci
        if len(mems) > 1:
            ci_mem = [
                round(float(np.percentile(mems, 100 * (1 - ci_float) / 2)), 2),
                round(float(np.percentile(mems, 100 * (1 + ci_float) / 2)), 2),
            ]
        if len(runtimes) > 1:
            ci_runtime = [
                round(float(np.percentile(runtimes, 100 * (1 - ci_float) / 2)), 2),
                round(float(np.percentile(runtimes, 100 * (1 + ci_float) / 2)), 2),
            ]

    return {
        "num_tasks": num_tasks,
        "min_mem": format_bytes(min_mem),
        "max_mem": format_bytes(max_mem),
        "mean_mem": format_bytes(mean_mem),
        "median_mem": format_bytes(median_mem),
        "min_runtime": min_runtime,
        "max_runtime": max_runtime,
        "mean_runtime": mean_runtime,
        "median_runtime": median_runtime,
        "ci_mem": ci_mem,
        "ci_runtime": ci_runtime,
    }
