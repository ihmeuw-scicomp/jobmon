import axios from 'axios';
import { fatal_error_breakdown_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';

export interface FatalErrorBreakdown {
    resource: number;
    app: number;
    infra: number;
    resource_error_total: number;
    resource_error_retried: number;
    resource_error_ti_ids: number[];
}

export interface FatalBreakdownParams {
    workflowId: string | number;
    taskTemplateVersionId: string | number | null;
    workflowRunId?: number | null;
}

export function fatalBreakdownKey(
    params: FatalBreakdownParams
): readonly [string, string, FatalBreakdownParams] {
    return ['workflow_details', 'fatal_breakdown', params] as const;
}

type QueryFnArgs = {
    queryKey: readonly [string, string, FatalBreakdownParams];
};

export const getFatalErrorBreakdownFn = async ({
    queryKey,
}: QueryFnArgs): Promise<FatalErrorBreakdown> => {
    const p = queryKey[2];
    const params: Record<string, string> = {};
    if (p.workflowRunId != null) {
        params.workflow_run_id = String(p.workflowRunId);
    }
    return axios
        .get<FatalErrorBreakdown>(
            `${fatal_error_breakdown_url}${p.workflowId}/${p.taskTemplateVersionId}`,
            {
                ...jobmonAxiosConfig,
                data: null,
                params: Object.keys(params).length > 0 ? params : undefined,
            }
        )
        .then(r => r.data);
};
