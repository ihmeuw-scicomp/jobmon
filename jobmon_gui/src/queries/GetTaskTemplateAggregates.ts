import axios from 'axios';
import { QueryFunctionContext } from '@tanstack/react-query';

import { usage_aggregates_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { components } from '@jobmon_gui/types/apiSchema';

type TaskTemplateResourceAggregatesResponse =
    components['schemas']['TaskTemplateResourceAggregatesResponse'];

export type TaskTemplateAggregatesQueryKey = readonly [
    string,
    string,
    string | number | undefined,
    string | number | undefined,
    boolean,
];

export const getTaskTemplateAggregatesQueryFn = async (
    context: QueryFunctionContext<TaskTemplateAggregatesQueryKey>
): Promise<TaskTemplateResourceAggregatesResponse | undefined> => {
    const { queryKey } = context;
    if (
        !queryKey ||
        queryKey.length !== 5 ||
        queryKey[2] === undefined ||
        queryKey[3] === undefined
    ) {
        return undefined;
    }
    const taskTemplateVersionId = queryKey[2];
    const workflowId = queryKey[3];
    const latestOnly = queryKey[4];

    const response = await axios.post<TaskTemplateResourceAggregatesResponse>(
        usage_aggregates_url,
        {
            task_template_version_id: taskTemplateVersionId,
            workflows: [workflowId],
            latest_only: latestOnly,
        },
        jobmonAxiosConfig
    );
    return response.data;
};
