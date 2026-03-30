import axios from 'axios';
import { workflow_tt_status_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { TTStatusResponse } from '@jobmon_gui/types/TaskTemplateStatus.ts';
import { extractNumericParam, wfrParams } from './queryKeyUtils';

type getWorkflowTTStatusQueryFnArgs = {
    queryKey: (string | number | undefined | null)[];
};
export const getWorkflowTTStatusQueryFn = async ({
    queryKey,
}: getWorkflowTTStatusQueryFnArgs) => {
    if (!queryKey || queryKey.length < 3) {
        return;
    }
    const workflowId = queryKey[2];
    const workflowRunId = extractNumericParam(queryKey, 3);
    return axios
        .get<TTStatusResponse>(workflow_tt_status_url + workflowId, {
            ...jobmonAxiosConfig,
            data: null,
            params: wfrParams(workflowRunId),
        })
        .then(r => r.data);
};
