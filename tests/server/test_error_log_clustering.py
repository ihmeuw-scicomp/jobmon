from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd

from jobmon.server.web.error_log_clustering import cluster_error_logs

import nltk

nltk.download('punkt_tab')


def test_cluster_error_logs():
    input_df = pd.DataFrame([
        {
            "error": "Task instance never reported a heartbeat after scheduling. Will retry",
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": 23,
            "task_instance_err_id": 39,
            "task_instance_id": 50040,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1
        },
        {
            "error": "Task instance never reported a heartbeat after scheduling. Will retry",
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": 22,
            "task_instance_err_id": 38,
            "task_instance_id": 50039,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1
        },
        {
            "error": "Task instance never reported a heartbeat after scheduling. Will retry",
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": 21,
            "task_instance_err_id": 37,
            "task_instance_id": 50038,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1
        }
    ])
    output_df = cluster_error_logs(input_df)
    assert output_df.shape[0] == 1
def test_cluster_error_logs_two_clusters():
    input_df = pd.DataFrame([
        {
            "error": "Task instance never reported a heartbeat after scheduling. Will retry",
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": 23,
            "task_instance_err_id": 39,
            "task_instance_id": 50040,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1
        },
        {
            "error": "Task instance never reported a heartbeat after scheduling. Will retry",
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": 22,
            "task_instance_err_id": 38,
            "task_instance_id": 50039,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1
        },
        {
            "error": "Some other kind of error that isn't like the others",
            "error_time": "Mon, 15 Jul 2024 18:05:45 GMT",
            "task_id": 21,
            "task_instance_err_id": 37,
            "task_instance_id": 50038,
            "task_instance_stderr_log": None,
            "workflow_id": 1,
            "workflow_run_id": 1
        }
    ])
    output_df = cluster_error_logs(input_df)
    assert output_df.shape[0] == 2
