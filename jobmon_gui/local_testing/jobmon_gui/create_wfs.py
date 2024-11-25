import os
import subprocess
from time import sleep
from pathlib import Path

import argparse


def parse_arguments():
    """
    Function to parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    # Create the parser
    parser = argparse.ArgumentParser(description="Example script with optional arguments.")

    # Add the first optional argument
    parser.add_argument('--server-url',
                        type=str,
                        help="Description for the first optional argument.",
                        default="http://localhost:8070/api/v3")

    # Add the second optional argument
    parser.add_argument('--wf',
                        type=int,
                        help="Number of workflows to create. 0 means continuesly creating workflows.",
                        default=0)

    # Parse the arguments
    return parser.parse_args()


def create_wf(total):
    wf_script_path = Path(__file__).parent.parent.parent.parent / "tests/worker_node/task_generator_wf.py"
    created = 0
    # create #total of workflows; if total is 0, create workflows continuously
    while total == 0 or created < total:
        print("Creating workflow")
        created += 1
        # create_workflow()
        # Run the command and wait for it to finish
        result = subprocess.run(["python", wf_script_path, "1"], check=True)
        # This line will only run after the command above finishes
        print("workflow completed!")
        sleep(10)


# Example usage of the function
if __name__ == "__main__":
    args = parse_arguments()

    # Access the arguments
    url = args.server_url
    os.environ["JOBMON__HTTP__SERVICE_URL"] = "http://localhost:8070/api/v3"
    wfs = args.wf
    create_wf(wfs)
