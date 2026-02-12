import axios from 'axios';
import { useQuery } from '@tanstack/react-query';

import { get_task_template_details_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { TaskTemplateDetailsResponse } from '@jobmon_gui/types/TaskTemplateDetails.ts';

type getTaskTemplateDetailsQueryFnArgs = {
    queryKey: (string | number | undefined)[];
};

export const getTaskTemplateDetailsQueryFn = async ({
    queryKey,
}: getTaskTemplateDetailsQueryFnArgs) => {
    if (!queryKey || queryKey.length != 3) {
        return;
    }

    const wf_id = queryKey[1] as number;
    const tt_id = queryKey[2] as number;

    if (!wf_id || !tt_id) {
        return;
    }

    return axios
        .get<TaskTemplateDetailsResponse>(
            get_task_template_details_url(wf_id, tt_id),
            {
                ...jobmonAxiosConfig,
                data: null,
            }
        )
        .then(response => {
            return response.data;
        });
};

export const useTaskTemplateDetails = (
    workflowId: number | string,
    taskTemplateId: number | string
) => {
    return useQuery({
        queryKey: ['task_template_details', workflowId, taskTemplateId],
        queryFn: getTaskTemplateDetailsQueryFn,
        enabled: !!workflowId && !!taskTemplateId,
    });
};
