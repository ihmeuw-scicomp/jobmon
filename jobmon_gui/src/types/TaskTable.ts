import dayjs from "dayjs";

export type TaskTableProps = {
    taskTemplateName: string
    workflowId: number | string
}

export type Task = {
    task_command: string
    task_id: number
    task_max_attempts: number
    task_name: string
    task_num_attempts: number
    task_status: string
    task_status_date: dayjs.Dayjs
}
export type Tasks = {
    tasks: Task[]
}