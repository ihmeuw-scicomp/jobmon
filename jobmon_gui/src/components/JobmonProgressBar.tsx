import React from 'react';
import ProgressBar from 'react-bootstrap/ProgressBar';
import { OverlayTrigger } from 'react-bootstrap';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { CircularProgress } from '@mui/material';
import Typography from '@mui/material/Typography';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios';
import { workflow_tt_status_url } from '@jobmon_gui/configs/ApiUrls';
import { TTStatusResponse } from '@jobmon_gui/types/TaskTemplateStatus';
import TaskTemplatePopover from '@jobmon_gui/components/TaskTemplatePopover.tsx';

type WfDetails = {
    DONE: number;
    FATAL: number;
    MAXC: number;
    PENDING: number;
    RUNNING: number;
    SCHEDULED: number;
    id: number;
    num_attempts_avg: number;
    num_attempts_max: number;
    num_attempts_min: number;
    tasks: number;
};

type WfDetailsResponse = Record<string | number, WfDetails>;

type JobmonProgressBarProps = {
    workflowId: number | string;
    ttId?: string | number;
    placement?: 'top' | 'bottom' | 'left' | 'right';
};

export default function JobmonProgressBar({
    workflowId,
    ttId,
    placement = 'bottom',
}: JobmonProgressBarProps) {
    const workflow_status = useQuery({
        queryKey: ['workflow_details', 'progress_bar', workflowId],
        queryFn: async () => {
            const baseUrl =
                import.meta.env.VITE_APP_BASE_URL + '/workflow_status_viz';
            const url = new URL(baseUrl);
            const wf_ids = [workflowId];
            wf_ids.forEach(id =>
                url.searchParams.append('workflow_ids', id.toString())
            );
            return axios
                .get<WfDetailsResponse>(url.toString(), {
                    ...jobmonAxiosConfig,
                })
                .then(r => {
                    return r.data[workflowId];
                });
        },
        refetchOnWindowFocus: 'always',
        refetchOnMount: 'always',
    });

    const wfTTStatus = useQuery({
        queryKey: ['workflow_details', 'tt_status', workflowId],
        queryFn: async () => {
            return axios
                .get<TTStatusResponse>(workflow_tt_status_url + workflowId, {
                    ...jobmonAxiosConfig,
                    data: null,
                })
                .then(r => {
                    return r.data;
                });
        },
        refetchOnWindowFocus: 'always',
        refetchOnMount: 'always',
    });

    if (workflow_status.isLoading) {
        return <CircularProgress />;
    }
    if (workflow_status.isError) {
        return (
            <Typography>
                Unable to retrieve workflow status. Please reload and try again.
            </Typography>
        );
    }

    if (!!ttId && wfTTStatus.isLoading) {
        return <CircularProgress />;
    }
    if (!!ttId && wfTTStatus.isError) {
        return (
            <Typography>
                Error loading workflow task template details. Please refresh and
                try again.
            </Typography>
        );
    }

    const data = ttId ? wfTTStatus.data[ttId] : workflow_status.data;

    if (!data) {
        return <CircularProgress />;
    }

    function getLabel(value) {
        const total = data.tasks;

        // Handle edge case where total is 0. This shouldn't happen
        if (total === 0) return '0%';

        // Only show 100% if all tasks are done i.e. do not round up to 100%
        if (value / total === 1) return '100%';

        // Calculate percentage and round down to 1 decimal place
        const percentage = (value / total) * 100;
        return `${Math.floor(percentage * 10) / 10}%`;
    }

    return (
        <OverlayTrigger
            placement={placement}
            trigger={['hover', 'focus']}
            overlay={popoverProps => (
                <TaskTemplatePopover
                    data={data}
                    placement={popoverProps.placement}
                    ref={popoverProps.ref}
                    {...popoverProps}
                />
            )}
        >
            <ProgressBar>
                <ProgressBar
                    className="pending-progress-bar"
                    max={data.tasks}
                    now={data.PENDING}
                    key={1}
                    isChild={true}
                    label={getLabel(data.PENDING)}
                />
                <ProgressBar
                    className="scheduled-progress-bar"
                    max={data.tasks}
                    now={data.SCHEDULED}
                    key={2}
                    isChild={true}
                    label={getLabel(data.SCHEDULED)}
                />
                <ProgressBar
                    className="running-progress-bar"
                    max={data.tasks}
                    now={data.RUNNING}
                    key={3}
                    isChild={true}
                    label={getLabel(data.RUNNING)}
                />
                <ProgressBar
                    className="done-progress-bar"
                    max={data.tasks}
                    now={data.DONE}
                    key={4}
                    isChild={true}
                    label={getLabel(data.DONE)}
                />
                <ProgressBar
                    className="fatal-progress-bar"
                    max={data.tasks}
                    now={data.FATAL}
                    key={5}
                    isChild={true}
                    label={getLabel(data.FATAL)}
                />
            </ProgressBar>
        </OverlayTrigger>
    );
}
