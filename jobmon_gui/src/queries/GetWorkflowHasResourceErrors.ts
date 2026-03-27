import axios from 'axios';
import { workflow_has_resource_errors_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';

type QueryFnArgs = {
    queryKey: (string | number | undefined)[];
};

export interface WorkflowResourceErrorCheck {
    has_resource_errors: boolean;
}

export const getWorkflowHasResourceErrorsFn = async ({
    queryKey,
}: QueryFnArgs) => {
    if (!queryKey || queryKey.length !== 3) {
        return { has_resource_errors: false };
    }
    const workflowId = queryKey[2];
    return axios
        .get<WorkflowResourceErrorCheck>(
            workflow_has_resource_errors_url + workflowId,
            { ...jobmonAxiosConfig, data: null }
        )
        .then(r => r.data);
};
