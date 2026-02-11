import dayjs from 'dayjs';

export type ErrorSampleModalDetails = {
    sample_index: number;
    sample_ids: number[];
};

export interface ErrorLog {
    task_id: number;
    workflow_id: number;
    workflow_run_id: number;
    task_instance_err_id: number;
    task_instance_stderr_log: string;
    error_time: string;
    error: string;
}

export type ClusteredError = {
    group_instance_count: number;
    task_instance_ids: number[];
    task_ids: number[];
    sample_error: string;
    workflow_run_id: number;
    workflow_id: number;
    first_error_time: string | dayjs.Dayjs;
};

export type ClusteredErrorList = {
    error_logs: ClusteredError[];
    total_count: number;
    page: number;
    page_size: number;
};

export interface ErrorDetails {
    data?: {
        error_logs?: ErrorLog[];
    };
}

export interface ColumnType extends ClusteredError {
    // Extend ClusteredError type to add optional actions column
    actions?: string;
}
