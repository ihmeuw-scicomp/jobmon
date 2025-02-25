import axios from "axios";
import {usage_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import any = jasmine.any;

type getWorkflowUsageQueryFnArgs = {
    queryKey: (string | number | undefined)[]
}

type numberOrNull = number | null
type Node = {
    m: string|number
    node_id: number
    r: string|number
    requested_resources: string
    task_id: number
}
type ResponseType = [numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, null | any[], null | any[], Node[]]


export const getWorkflowUsageQueryFn = async ({queryKey}: getWorkflowUsageQueryFnArgs) => {
    if (!queryKey || queryKey.length != 4) {
        return;
    }
    const taskTemplateVersionId = queryKey[2]
    const workflowId = queryKey[3]
    const requestData = {
        task_template_version_id: taskTemplateVersionId,
        workflows: [workflowId],
        viz: true
    }
    return axios.post<ResponseType>(
        usage_url,
        requestData,
        jobmonAxiosConfig,
    ).then((r) => {
        return r.data
    })
}