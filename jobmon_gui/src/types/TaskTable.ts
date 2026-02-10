import dayjs from 'dayjs';

export type ResourceDataEntry = {
    runtime: number | null;
    memory: number | null;
    attempt: number;
};

export type TaskTableProps = {
    taskTemplateName: string;
    workflowId: number | string;
    resourceDataByTaskId?: Map<number, ResourceDataEntry>;
};

export type Task = {
    task_command: string;
    task_id: number;
    task_max_attempts: number;
    task_name: string;
    task_num_attempts: number;
    task_status: string;
    task_status_date: dayjs.Dayjs;
    runtime_seconds?: number | null;
    memory_gib?: number | null;
};
export type Tasks = {
    tasks: Task[];
};
