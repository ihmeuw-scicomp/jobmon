import axios from 'axios';
import { error_log_viz_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import dayjs from 'dayjs';
import { ClusteredErrorList } from '@jobmon_gui/types/ClusteredErrors.ts';

export interface ClusteredErrorsParams {
    workflowId: string | number;
    taskTemplateId: string | number;
    workflowRunId?: number | null;
    fatalTasksOnly?: boolean;
    taskTemplateVersionId?: string | number | null;
}

export function clusteredErrorsKey(
    params: ClusteredErrorsParams
): readonly [string, string, ClusteredErrorsParams] {
    return ['workflow_details', 'clustered_errors', params] as const;
}

type getClusteredErrorsFnArgs = {
    queryKey: readonly [string, string, ClusteredErrorsParams];
};
export const getClusteredErrorsFn = async ({
    queryKey,
}: getClusteredErrorsFnArgs) => {
    const p = queryKey[2];
    const params: Record<string, string> = { cluster_errors: 'true' };
    if (p.workflowRunId != null) {
        params.workflow_run_id = String(p.workflowRunId);
    }
    if (p.fatalTasksOnly) {
        params.fatal_tasks_only = 'true';
    }
    if (p.taskTemplateVersionId != null) {
        params.task_template_version_id = String(p.taskTemplateVersionId);
    }
    return axios
        .get<ClusteredErrorList>(
            `${error_log_viz_url}${p.workflowId}/${p.taskTemplateId}#`,
            {
                ...jobmonAxiosConfig,
                data: null,
                params,
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
