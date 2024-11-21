from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd
import random
import time

from jobmon.server.web.error_log_clustering import cluster_error_logs

import nltk

nltk.download("punkt_tab")


def test_cluster_error_logs():
    input_df = pd.DataFrame(
        [
            {
                "error": "Task instance never reported a heartbeat after scheduling. Will retry",
                "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
                "task_id": 23,
                "task_instance_err_id": 39,
                "task_instance_id": 50040,
                "task_instance_stderr_log": None,
                "workflow_id": 1,
                "workflow_run_id": 1,
            },
            {
                "error": "Task instance never reported a heartbeat after scheduling. Will retry",
                "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
                "task_id": 22,
                "task_instance_err_id": 38,
                "task_instance_id": 50039,
                "task_instance_stderr_log": None,
                "workflow_id": 1,
                "workflow_run_id": 1,
            },
            {
                "error": "Task instance never reported a heartbeat after scheduling. Will retry",
                "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
                "task_id": 21,
                "task_instance_err_id": 37,
                "task_instance_id": 50038,
                "task_instance_stderr_log": None,
                "workflow_id": 1,
                "workflow_run_id": 1,
            },
        ]
    )
    output_df = cluster_error_logs(input_df)
    assert output_df.shape[0] == 1


def test_cluster_error_logs_two_clusters():
    error_types = [
        """Traceback (most recent call last):
  File "/app/task_scheduler.py", line 42, in run_task
    self.connect_to_service()
  File "/app/task_scheduler.py", line 28, in connect_to_service
    connection = self.db_connection.connect()
  File "/app/database.py", line 56, in connect
    result = self.query("SELECT * FROM workers WHERE status='active'")
  File "/app/database.py", line 120, in query
    cursor.execute(query)
  File "/app/database.py", line 132, in execute
    raise TimeoutError("Connection timed out while trying to reach the database server.")
TimeoutError: Connection timed out while trying to reach the database server.""",
        """Traceback (most recent call last):
  File "/app/job_runner.py", line 75, in execute_job
    self.process_task(data)
  File "/app/task_processor.py", line 63, in process_task
    task_result = process_task_data(task_data)
  File "/app/task_processor.py", line 47, in process_task_data
    transformed_data = preprocess_data(task_data)
  File "/app/task_processor.py", line 30, in preprocess_data
    raise MemoryError("Memory limit exceeded during data processing. The task required more memory than allocated.")
MemoryError: Memory limit exceeded during data processing. The task required more memory than allocated.""",
        """Traceback (most recent call last):
  File "/app/data_validator.py", line 18, in validate_data
    check_required_fields(data)
  File "/app/data_validator.py", line 10, in check_required_fields
    if field not in data:
  File "/app/data_validator.py", line 12, in check_required_fields
    raise ValueError(f"Missing required field: {field}")
  File "/app/error_logger.py", line 55, in log_error
    self.log_to_external_service(error_message)
  File "/app/error_logger.py", line 67, in log_to_external_service
    raise ConnectionError("Failed to connect to external error logging service.")
ConnectionError: Failed to connect to external error logging service.""",
        """Traceback (most recent call last):
  File "/app/network_manager.py", line 21, in send_request
    self.connect_to_server()
  File "/app/network_manager.py", line 14, in connect_to_server
    response = requests.get(self.server_url)
  File "/app/vendor/requests/__init__.py", line 232, in get
    raise requests.exceptions.Timeout("The request timed out while trying to connect to the server.")
  File "/app/vendor/requests/sessions.py", line 646, in send
    r = adapter.send(request, **kwargs)
  File "/app/vendor/requests/adapters.py", line 517, in send
    raise requests.exceptions.ConnectionError("Failed to establish a new connection.")
requests.exceptions.ConnectionError: Failed to establish a new connection.""",
        """Traceback (most recent call last):
  File "/app/task_handler.py", line 32, in start_task
    result = execute_task(task)
  File "/app/task_handler.py", line 24, in execute_task
    task_result = complex_task_processing(task)
  File "/app/task_handler.py", line 15, in complex_task_processing
    intermediate_result = process_intermediate_step(task)
  File "/app/task_handler.py", line 10, in process_intermediate_step
    if check_for_errors(intermediate_result):
  File "/app/task_handler.py", line 5, in check_for_errors
    raise RuntimeError("Unexpected error occurred while processing task.")
RuntimeError: Unexpected error occurred while processing task.""",
    ]

    input_data = [
        {
            "error": random.choice(error_types),
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": i + 1,
            "task_instance_err_id": i + 100,
            "task_instance_id": i + 1000,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1,
        }
        for i in range(100)
    ]
    input_df = pd.DataFrame(input_data)
    output_df = cluster_error_logs(input_df)
    assert output_df.shape[0] == 5
