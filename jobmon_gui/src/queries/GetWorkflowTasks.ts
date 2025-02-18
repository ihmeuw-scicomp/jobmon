import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {Task, Tasks} from "@jobmon_gui/types/TaskTable.ts";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import {task_table_url} from "@jobmon_gui/configs/ApiUrls.ts";

type getWorkflowTasksQueryFnArgs = {
    queryKey: (string | number | null | string[] | number[] | null[])[]
}

export const getWorkflowTasksQueryFn = async ({queryKey}: getWorkflowTasksQueryFnArgs) => {
    dayjs.extend(utc)
    if (!queryKey || queryKey.length != 4) {
        return;
    }
    const workflowId = queryKey[2]
    const taskTemplateName = queryKey[3]
    return axios.get<Tasks>(
        task_table_url + workflowId,
        {
            ...jobmonAxiosConfig,
            data: null,
            params: {tt_name: taskTemplateName}
        }
    ).then((r) => {
        return r.data.tasks.map((task: Task) => {
            task.task_status_date = dayjs.utc(task.task_status_date, 'YYYY-MM-DD HH:mm:SS')
            return task
        })
    })
}