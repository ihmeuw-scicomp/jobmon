export type TaskInstance = {
    ti_distributor_id: string;
    ti_error_log_description: string | null;
    ti_id: number | string;
    ti_maxrss: string | null;
    ti_nodename: string | null;
    ti_resources: string;
    ti_status: string | null;
    ti_stderr: string | null;
    ti_stderr_log: string | null;
    ti_stdout: string | null;
    ti_stdout_log: string | null;
    ti_wallclock: string | number | null;
    ti_submit_date: string | null;
    ti_status_date: string | null;
    ti_queue_name: string | null;
};

export type TypeInstanceResponse = {
    taskinstances: TaskInstance[];
};
