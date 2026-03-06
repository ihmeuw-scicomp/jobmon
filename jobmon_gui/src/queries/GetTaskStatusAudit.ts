import axios from 'axios';
import { api_base_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { components } from '@jobmon_gui/types/apiSchema';

export type TaskStatusAuditResponse =
    components['schemas']['TaskStatusAuditResponse'];

type GetTaskStatusAuditQueryFnArgs = {
    queryKey: (string | number | undefined | null)[];
};

export async function getTaskStatusAuditQueryFn({
    queryKey,
}: GetTaskStatusAuditQueryFnArgs): Promise<
    TaskStatusAuditResponse | undefined
> {
    if (!queryKey || queryKey.length < 3) {
        return;
    }
    const [, workflowId, taskId] = queryKey;

    const params = new URLSearchParams();
    if (taskId != null) {
        params.append('task_id', taskId.toString());
    }
    params.append('limit', '100');

    const response = await axios.get<TaskStatusAuditResponse>(
        `${api_base_url}/workflow/${workflowId}/task_status_audit?${params}`,
        {
            ...jobmonAxiosConfig,
            data: null,
        }
    );
    return response.data;
}
