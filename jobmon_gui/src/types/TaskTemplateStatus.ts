export type TTStatus = {
    DONE: number;
    FATAL: number;
    MAXC: number;
    PENDING: number;
    RUNNING: number;
    SCHEDULED: number;
    id: number;
    name: string;
    num_attempts_avg: number;
    num_attempts_max: number;
    num_attempts_min: number;
    task_template_version_id: number;
    tasks: number;
};

export type TTStatusResponse = Record<number | string, TTStatus>;
