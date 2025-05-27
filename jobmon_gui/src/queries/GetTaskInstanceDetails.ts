import axios from 'axios';
import { ti_details_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { TypeInstanceResponse } from '@jobmon_gui/types/TaskInstance.ts';

type getTaskInstanceDetailsQueryFnArgs = {
  queryKey: (string | number | undefined)[];
};

export const getTaskInstanceDetailsQueryFn = async ({
  queryKey,
}: getTaskInstanceDetailsQueryFnArgs) => {
  if (!queryKey || queryKey.length != 2) {
    return;
  }
  const taskId = queryKey[1];
  return axios
    .get<TypeInstanceResponse>(`${ti_details_url}${taskId}#`, {
      ...jobmonAxiosConfig,
      data: null,
    })
    .then(r => {
      return r.data.taskinstances.map(data => ({
        ti_id: data.ti_id,
        ti_status: data.ti_status,
        ti_stdout: data.ti_stdout,
        ti_stderr: data.ti_stderr,
        ti_distributor_id: data.ti_distributor_id,
        ti_nodename: data.ti_nodename,
        ti_stdout_log: data.ti_stdout_log,
        ti_stderr_log: data.ti_stderr_log,
        ti_error_log_description: data.ti_error_log_description,
        ti_wallclock: data.ti_wallclock,
        ti_maxrss: data.ti_maxrss,
        ti_resources: data.ti_resources,
      }));
    });
};
