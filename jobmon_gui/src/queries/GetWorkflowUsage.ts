import axios from 'axios';
import { usage_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { QueryFunctionContext } from '@tanstack/react-query';
import { components } from '@jobmon_gui/types/apiSchema';

// Define the expected schema type for the response
type TaskTemplateResourceUsageResponse =
  components['schemas']['TaskTemplateResourceUsageResponse'];

// Define the specific query key structure this function expects
export type WorkflowUsageQueryKey = readonly [
  string,
  string,
  string | number | undefined,
  string | number | undefined,
];

export const getWorkflowUsageQueryFn = async (
  context: QueryFunctionContext<WorkflowUsageQueryKey>
): Promise<TaskTemplateResourceUsageResponse | undefined> => {
  const { queryKey } = context;
  // queryKey should be [string, string, taskTemplateVersionId, workflowId]
  if (
    !queryKey ||
    queryKey.length !== 4 ||
    queryKey[2] === undefined ||
    queryKey[3] === undefined
  ) {
    // console.error("Invalid queryKey for getWorkflowUsageQueryFn", queryKey);
    return undefined; // Or throw an error, depending on desired handling
  }
  const taskTemplateVersionId = queryKey[2];
  const workflowId = queryKey[3];

  const requestData = {
    task_template_version_id: taskTemplateVersionId,
    workflows: [workflowId],
    viz: true,
  };

  const response = await axios.post<TaskTemplateResourceUsageResponse>(
    usage_url,
    requestData,
    jobmonAxiosConfig
  );
  return response.data; // This should now be TaskTemplateResourceUsageResponse
};
