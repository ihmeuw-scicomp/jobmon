import axios from 'axios';
import { QueryFunctionContext } from '@tanstack/react-query';

import {
    task_resources_batch_url,
    workflow_requested_resources_url,
} from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { components } from '@jobmon_gui/types/apiSchema';

export type WorkflowRequestedResourcesResponse =
    components['schemas']['WorkflowRequestedResourcesResponse'];

export type WorkflowRequestedResourcesQueryKey = readonly [
    'workflow_requested_resources',
    string | number,
];

export const getWorkflowRequestedResourcesQueryFn = async (
    context: QueryFunctionContext<WorkflowRequestedResourcesQueryKey>
): Promise<WorkflowRequestedResourcesResponse | undefined> => {
    const { queryKey } = context;
    if (!queryKey || queryKey.length !== 2 || queryKey[1] === undefined) {
        return undefined;
    }
    const workflowId = queryKey[1];
    const response = await axios.get<WorkflowRequestedResourcesResponse>(
        workflow_requested_resources_url(workflowId),
        jobmonAxiosConfig
    );
    return response.data;
};

export type TaskResourcesBatchResponse =
    components['schemas']['TaskResourcesBatchResponse'];

export const getTaskResourcesBatchQueryFn = async (
    ids: number[]
): Promise<TaskResourcesBatchResponse> => {
    if (ids.length === 0) return { resources: [] };
    const response = await axios.post<TaskResourcesBatchResponse>(
        task_resources_batch_url,
        { task_resources_ids: ids },
        jobmonAxiosConfig
    );
    return response.data;
};
