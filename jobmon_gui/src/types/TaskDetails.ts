import {TaskStatusCodes} from "@jobmon_gui/types/TaskStatusCodes.ts";

export type TaskDetails = {
    task_command: string
    task_name: string
    task_status: TaskStatusCodes
    task_status_date: string
    workflow_id: number
    task_template_id: number
}
export type TaskDetailsResponse = {
    task_details: TaskDetails[]
}

