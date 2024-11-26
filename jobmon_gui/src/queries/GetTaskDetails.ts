import axios from "axios";
import {task_details_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {TaskDetailsResponse} from "@jobmon_gui/types/TaskDetails.ts";

type getTaskDetailsQueryFnFnArgs = {
    queryKey: (string | number | undefined)[]
}

export const getTaskDetailsQueryFn = async ({queryKey}: getTaskDetailsQueryFnFnArgs) => {
    if (!queryKey || queryKey.length != 2) {
        return;
    }
    const taskId = queryKey[1]
    return axios.get<TaskDetailsResponse>(`${task_details_url}${taskId}#`,
        {
            ...jobmonAxiosConfig,
            data: null,
        }
    ).then((r) => {
        console.log("task_details", r.data)
        return r.data.task_details[0]
    })
}