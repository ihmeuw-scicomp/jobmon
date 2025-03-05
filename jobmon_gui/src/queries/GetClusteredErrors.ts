import axios from "axios";
import {error_log_viz_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import dayjs from "dayjs";
import {ClusteredErrorList} from "@jobmon_gui/types/ClusteredErrors.ts";

type getClusteredErrorsFnArgs = {
    queryKey: (string | number | undefined)[]
}
export const getClusteredErrorsFn = async ({queryKey}: getClusteredErrorsFnArgs) => {
    if (!queryKey || queryKey.length != 4) {
        return;
    }
    const workflowId = queryKey[2]
    const taskTemplateId = queryKey[3]
    return axios.get<ClusteredErrorList>(
        `${error_log_viz_url}${workflowId}/${taskTemplateId}#`,
        {
            ...jobmonAxiosConfig,
            data: null,
            params: {cluster_errors: "true"}
        }
    ).then((r) => {
        return {
            ...r.data, error_logs: r.data.error_logs.map((el) => {
                el.first_error_time = dayjs(el.first_error_time)
                return el
            })
        }

    })
}