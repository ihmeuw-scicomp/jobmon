import dayjs from 'dayjs';

export type TaskInstanceRow = {
    task_id: number;
    task_instance_id: number;
    task_name: string;
    attempt_number: number;
    instance_status: string;
    task_command: string;
    task_num_attempts: number;
    task_max_attempts: number;
    task_status_date: dayjs.Dayjs;
    runtime_seconds: number | null;
    memory_gib: number | null;
};

export type TaskTableProps = {
    data: TaskInstanceRow[];
    isLoading: boolean;
    workflowId: number | string;
    taskTemplateName: string;
    onFilteredInstanceIdsChange?: (ids: Set<number> | null) => void;
};
