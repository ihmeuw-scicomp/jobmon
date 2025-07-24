#!/usr/bin/env python3
"""
Sample workflow script for testing jobmon_client functionality.

This script demonstrates how to create and run a simple workflow using jobmon.
You can modify this script or create new ones in this directory to test
different aspects of jobmon workflows.

To run this script inside the jobmon_client container:
1. docker-compose up -d jobmon_client
2. docker-compose exec jobmon_client python sample_workflow.py
"""

import os
import time
from jobmon.client.workflow import Workflow
from jobmon.client.task import Task


def create_sample_workflow():
    """Create a simple sample workflow for testing."""
    
    # Create a workflow
    workflow = Workflow(
        workflow_args=f"sample_workflow_{int(time.time())}",
        project="test_project",
        stderr="workflow_stderr.log",
        stdout="workflow_stdout.log"
    )
    
    # Create a simple task that echoes a message
    task1 = Task(
        command="echo 'Hello from jobmon task 1!'",
        name="echo_task_1",
        max_attempts=3,
        compute_resources={
            "memory": "1G",
            "cores": 1,
            "runtime": "00:05:00"
        }
    )
    
    # Create another task that depends on the first
    task2 = Task(
        command="echo 'Hello from jobmon task 2! Task 1 completed.'",
        name="echo_task_2",
        max_attempts=3,
        compute_resources={
            "memory": "1G",
            "cores": 1,
            "runtime": "00:05:00"
        }
    )
    
    # Add dependency: task2 depends on task1
    task2.add_upstream(task1)
    
    # Add tasks to workflow
    workflow.add_task(task1)
    workflow.add_task(task2)
    
    return workflow


def main():
    """Main function to create and optionally run the workflow."""
    print("Creating sample workflow...")
    
    workflow = create_sample_workflow()
    
    print(f"Created workflow with {len(workflow.tasks)} tasks")
    print("Tasks:")
    for task in workflow.tasks:
        print(f"  - {task.name}: {task.command}")
    
    # Uncomment the following lines to actually run the workflow
    # NOTE: This requires a properly configured jobmon server and database
    # print("Running workflow...")
    # workflow.run()
    # print("Workflow completed!")
    
    print("\nTo run this workflow:")
    print("1. Ensure jobmon_backend service is running")
    print("2. Uncomment the workflow.run() lines in this script")
    print("3. Run: python sample_workflow.py")


if __name__ == "__main__":
    main() 