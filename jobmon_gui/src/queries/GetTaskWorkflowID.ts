import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {Task, Tasks} from "@jobmon_gui/types/TaskTable.ts";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import {get_task_wf_id_url} from "@jobmon_gui/configs/ApiUrls.ts";

type getTaskWorkflowIDFnArgs = {
    queryKey: (string | number | null | string[] | number[] | null[])[]
}

export const getTaskWorkflowIDQueryFn = async ({queryKey}: getTaskWorkflowIDFnArgs) => {
    dayjs.extend(utc)
    if (!queryKey || queryKey.length != 2) {
        return;
    }
    const taskID = queryKey[1]

    return axios.get<Tasks>(
        get_task_wf_id_url + taskID,
        {
            ...jobmonAxiosConfig,
            data: null,
        }
    ).then((r) => {
        return r.data
    })
}