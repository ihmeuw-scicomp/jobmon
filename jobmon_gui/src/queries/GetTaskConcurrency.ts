import axios from 'axios';
import { api_base_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { components } from '@jobmon_gui/types/apiSchema';

// Use generated types from OpenAPI schema
export type TaskConcurrencyResponse =
    components['schemas']['TaskConcurrencyResponse'];

export type WorkflowTaskTemplatesResponse =
    components['schemas']['WorkflowTaskTemplatesResponse'];

type GetTaskConcurrencyQueryFnArgs = {
    queryKey: (string | number | undefined | null)[];
};

export async function getTaskConcurrencyQueryFn({
    queryKey,
}: GetTaskConcurrencyQueryFnArgs): Promise<
    TaskConcurrencyResponse | undefined
> {
    if (!queryKey || queryKey.length < 3) {
        return;
    }
    const [, , workflowId, bucketSeconds = 10] = queryKey;

    const params = new URLSearchParams();
    params.append('bucket_seconds', bucketSeconds.toString());

    const response = await axios.get<TaskConcurrencyResponse>(
        `${api_base_url}/workflow/${workflowId}/task_concurrency?${params}`,
        {
            ...jobmonAxiosConfig,
            data: null,
        }
    );
    return response.data;
}

type GetWorkflowTaskTemplatesQueryFnArgs = {
    queryKey: (string | number | undefined)[];
};

export async function getWorkflowTaskTemplatesQueryFn({
    queryKey,
}: GetWorkflowTaskTemplatesQueryFnArgs): Promise<
    WorkflowTaskTemplatesResponse | undefined
> {
    if (!queryKey || queryKey.length < 3) {
        return;
    }
    const [, , workflowId] = queryKey;

    const response = await axios.get<WorkflowTaskTemplatesResponse>(
        `${api_base_url}/workflow/${workflowId}/task_templates`,
        {
            ...jobmonAxiosConfig,
            data: null,
        }
    );
    return response.data;
}
