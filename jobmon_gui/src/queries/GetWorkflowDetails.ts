import axios from 'axios';
import { WorkflowDetailsResponse } from '@jobmon_gui/types/WorkflowDetails.ts';
import { workflow_details_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';

type getWorkflowDetailsQueryFnArgs = {
    queryKey: (string | number | undefined)[];
};
export const getWorkflowDetailsQueryFn = async ({
    queryKey,
}: getWorkflowDetailsQueryFnArgs) => {
    if (!queryKey || queryKey.length != 3) {
        return;
    }
    const wf_id = queryKey[2];
    return axios
        .get<WorkflowDetailsResponse>(workflow_details_url + wf_id, {
            ...jobmonAxiosConfig,
            data: null,
        })
        .then(r => {
            return r.data[0];
        });
};
