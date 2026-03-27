import axios from 'axios';
import { fatal_error_breakdown_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';

export interface FatalErrorBreakdown {
    resource: number;
    app: number;
    infra: number;
    resource_error_total: number;
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
    return axios
        .get<FatalErrorBreakdown>(
            `${fatal_error_breakdown_url}${workflowId}/${ttVersionId}`,
            { ...jobmonAxiosConfig, data: null }
        )
        .then(r => r.data);
};
