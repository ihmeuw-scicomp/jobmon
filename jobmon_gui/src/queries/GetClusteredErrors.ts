import axios from 'axios';
import { error_log_viz_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import dayjs from 'dayjs';
import { ClusteredErrorList } from '@jobmon_gui/types/ClusteredErrors.ts';
import { extractNumericParam, wfrParams } from './queryKeyUtils';

type getClusteredErrorsFnArgs = {
    queryKey: (string | number | undefined | null)[];
};
export const getClusteredErrorsFn = async ({
    queryKey,
}: getClusteredErrorsFnArgs) => {
    if (!queryKey || queryKey.length < 4) {
        return;
    }
    const workflowId = queryKey[2];
    const taskTemplateId = queryKey[3];
    const workflowRunId = extractNumericParam(queryKey, 4);
    return axios
        .get<ClusteredErrorList>(
            `${error_log_viz_url}${workflowId}/${taskTemplateId}#`,
            {
                ...jobmonAxiosConfig,
                data: null,
                params: wfrParams(workflowRunId, {
                    cluster_errors: 'true',
                }),
            }
        )
        .then(r => {
            return {
                ...r.data,
                error_logs: r.data.error_logs.map(el => {
                    el.first_error_time = dayjs(el.first_error_time);
                    return el;
                }),
            };
        });
};
