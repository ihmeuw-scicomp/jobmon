import axios from 'axios';
import { fatal_error_breakdown_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { extractNumericParam, wfrParams } from './queryKeyUtils';

export interface FatalErrorBreakdown {
    resource: number;
    app: number;
    infra: number;
    resource_error_total: number;
    resource_error_retried: number;
    resource_error_ti_ids: number[];
}

type QueryFnArgs = {
    queryKey: readonly unknown[];
};

export const getFatalErrorBreakdownFn = async ({
    queryKey,
}: QueryFnArgs): Promise<FatalErrorBreakdown> => {
    const workflowId = queryKey[2];
    const ttVersionId = queryKey[3];
    const workflowRunId = extractNumericParam(queryKey, 4);
    return axios
        .get<FatalErrorBreakdown>(
            `${fatal_error_breakdown_url}${workflowId}/${ttVersionId}`,
            {
                ...jobmonAxiosConfig,
                data: null,
                params: wfrParams(workflowRunId),
            }
        )
        .then(r => r.data);
};
