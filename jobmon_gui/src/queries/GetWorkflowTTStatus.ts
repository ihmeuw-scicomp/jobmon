import axios from "axios";
import {workflow_tt_status_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {TTStatusResponse} from "@jobmon_gui/types/TaskTemplateStatus.ts";

type getWorkflowTTStatusQueryFnArgs = {
    queryKey: (string | number | undefined)[]
}
export const getWorkflowTTStatusQueryFn = async ({queryKey}: getWorkflowTTStatusQueryFnArgs) => {
    if (!queryKey || queryKey.length != 3) {
        return;
    }
    const workflowId = queryKey[2]
    return axios.get<TTStatusResponse>(workflow_tt_status_url + workflowId, {
            ...jobmonAxiosConfig,
            data: null,
        }
    ).then((r) => {
        return r.data
    })
}