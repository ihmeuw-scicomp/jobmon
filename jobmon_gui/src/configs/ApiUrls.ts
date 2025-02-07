export const api_base_url = import.meta.env.VITE_APP_BASE_URL;
const oidcBaseUrl = import.meta.env.VITE_APP_OIDC_BASE_URL;

export const loginURL = oidcBaseUrl + "/login";
export const logoutURL = oidcBaseUrl + "/logout";
export const userInfoURL = oidcBaseUrl + "/userinfo";

export const task_table_url = api_base_url + "/task_table_viz/";

export const usage_url = api_base_url + "/task_template_resource_usage";

export const workflow_details_url = api_base_url + "/workflow_details_viz/"
export const workflow_tt_status_url = api_base_url + "/workflow_tt_status_viz/";
export const workflow_overview_url = api_base_url + "/workflow_overview_viz";
export const workflow_status_url = api_base_url + "/workflow_status_viz";

export const error_log_viz_url = api_base_url + "/tt_error_log_viz/";
export const ti_details_url = api_base_url + "/task/get_ti_details_viz/";
export const task_details_url = api_base_url + "/task/get_task_details_viz/";
export const task_dependencies_url = api_base_url + "/task_dependencies/";

export const workflow_set_resume_url = (wf_id: number | string) => api_base_url + `/workflow/${wf_id}/set_resume`;
export const set_task_template_concurrency_url = (wf_id: number | string) => api_base_url + `/workflow/${wf_id}/update_array_max_concurrently_running`;
export const set_wf_concurrency_url = (wf_id: number | string) => api_base_url + `/workflow/${wf_id}/update_max_concurrently_running`;
export const get_workflow_concurrency_url = (wf_id: number | string) => api_base_url + `/workflow/${wf_id}/get_max_concurrently_running`;
export const get_task_template_concurrency_url = (wf_id: number | string, tt_version_id: number | string) => api_base_url + `/workflow/${wf_id}/get_array_max_concurrently_running/${tt_version_id}`;

export const get_task_template_details_url = (workflowId: number, taskTemplateId: number) => api_base_url + `/get_task_template_details?workflow_id=${workflowId}&task_template_id=${taskTemplateId}`;
