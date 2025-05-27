import axios from "axios";
import {usage_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import { QueryFunctionContext } from "@tanstack/react-query";
import { components } from '@jobmon_gui/types/apiSchema';

type getWorkflowUsageQueryFnArgs = {
    queryKey: (string | number | undefined)[]
}

type numberOrNull = number | null
type Node = {
    m: string|number
    node_id: number
    r: string|number
    requested_resources: string
    task_id: number
}
type ResponseType = [numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, numberOrNull, null | any[], null | any[], Node[]]

// Define the expected schema type for the response
type TaskTemplateResourceUsageResponse = components["schemas"]["TaskTemplateResourceUsageResponse"];

// Define the specific query key structure this function expects
export type WorkflowUsageQueryKey = readonly [string, string, string | number | undefined, string | number | undefined];

export const getWorkflowUsageQueryFn = async (
    context: QueryFunctionContext<WorkflowUsageQueryKey>
): Promise<TaskTemplateResourceUsageResponse | undefined> => {
    const { queryKey } = context;
    // queryKey should be [string, string, taskTemplateVersionId, workflowId]
    if (!queryKey || queryKey.length !== 4 || queryKey[2] === undefined || queryKey[3] === undefined) {
        // console.error("Invalid queryKey for getWorkflowUsageQueryFn", queryKey);
        return undefined; // Or throw an error, depending on desired handling
    }
    const taskTemplateVersionId = queryKey[2];
    const workflowId = queryKey[3];

    const requestData = {
        task_template_version_id: taskTemplateVersionId,
        workflows: [workflowId],
        viz: true
    };

    try {
        const response = await axios.post<TaskTemplateResourceUsageResponse>(
            usage_url,
            requestData,
            jobmonAxiosConfig,
        );
        return response.data; // This should now be TaskTemplateResourceUsageResponse
    } catch (error) {
        // console.error("Error fetching workflow usage:", error);
        // Handle error appropriately, e.g., throw it or return a specific error structure
        throw error; // Re-throwing is a common pattern for react-query to handle it
    }
};