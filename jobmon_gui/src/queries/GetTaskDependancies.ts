import axios from "axios";
import {task_dependencies_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {TaskDependenciesResponse} from "@jobmon_gui/types/TaskDependancies.ts";

type getTaskDetailsQueryFnFnArgs = {
    queryKey: (string | number | undefined)[]
}

export const getTaskDependenciesQuernFn = async ({queryKey}: getTaskDetailsQueryFnFnArgs) => {
    if (!queryKey || queryKey.length != 2) {
        return;
    }
    const taskId = queryKey[1]
    return axios.get<TaskDependenciesResponse>(
        `${task_dependencies_url}${taskId}#`,
        {
            ...jobmonAxiosConfig,
            data: null,
        }
    ).then((r) => {
        return r.data
    })
}