import axios from 'axios';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { task_table_url } from '@jobmon_gui/configs/ApiUrls.ts';
import timezone from 'dayjs/plugin/timezone';

type Task = {
    task_command: string;
    task_id: number;
    task_max_attempts: number;
    task_name: string;
    task_num_attempts: number;
    task_status: string;
    task_status_date: dayjs.Dayjs;
};

type Tasks = {
    tasks: Task[];
};

type getWorkflowTasksQueryFnArgs = {
    queryKey: (string | number | null | string[] | number[] | null[])[];
};

dayjs.extend(utc);
dayjs.extend(timezone);

export const getWorkflowTasksQueryFn = async ({
    queryKey,
}: getWorkflowTasksQueryFnArgs) => {
    if (!queryKey || queryKey.length != 4) {
        return;
    }
    const workflowId = queryKey[2];
    const taskTemplateName = queryKey[3];
    return axios
        .get<Tasks>(task_table_url + workflowId, {
            ...jobmonAxiosConfig,
            data: null,
            params: { tt_name: taskTemplateName },
        })
        .then(r => {
            return r.data.tasks.map((task: Task) => {
                task.task_status_date = dayjs.tz(
                    task.task_status_date,
                    'YYYY-MM-DD HH:mm:ss',
                    'America/Los_Angeles'
                );
                return task;
            });
        });
};
