export const api_base_url = import.meta.env.VITE_APP_BASE_URL
const oidcBaseUrl = import.meta.env.VITE_APP_OIDC_BASE_URL

export const loginURL = oidcBaseUrl + "/login"
export const logoutURL = oidcBaseUrl + "/logout"
export const userInfoURL = oidcBaseUrl + "/userinfo"

export const task_table_url = api_base_url + "/task_table_viz/";

export const usage_url = api_base_url + "/task_template_resource_usage";

export const workflow_details_url = api_base_url + "/workflow_details_viz/"
export const workflow_tt_status_url = api_base_url + "/workflow_tt_status_viz/";
export const workflow_overview_url = api_base_url + "/workflow_overview_viz";
export const workflow_status_url = api_base_url + "/workflow_status_viz";
export const error_log_viz_url = api_base_url + "/tt_error_log_viz/"
