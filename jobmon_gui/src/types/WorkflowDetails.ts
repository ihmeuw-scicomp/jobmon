export type WorkflowDetails = {
  tool_name: string;
  wf_args: string;
  wf_created_date: string;
  wf_name: string;
  wf_status: string;
  wf_status_date: string;
  wf_status_desc: string;
  wfr_jobmon_version: string;
  wfr_heartbeat_date: string;
  wfr_user: string;
};
export type WorkflowDetailsResponse = WorkflowDetails[];
