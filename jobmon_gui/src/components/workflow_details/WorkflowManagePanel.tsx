import React, { useState, useEffect } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import InfoIcon from '@mui/icons-material/Info';
import { useMutation, useQuery } from '@tanstack/react-query';
import { compare } from 'compare-versions';
import axios from 'axios';
import {
    get_workflow_concurrency_url,
    set_wf_concurrency_url,
    update_task_status_url,
} from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import StopWorkflowButton from '@jobmon_gui/components/StopWorkflow.tsx';
import { WorkflowDetails } from '@jobmon_gui/types/WorkflowDetails.ts';

interface WorkflowData {
    max_concurrently_running: number;
}

interface WorkflowManagePanelProps {
    wfId: string | number;
    workflowDetails?: WorkflowDetails;
    onBack: () => void;
    onClose: () => void;
}

function normalizeVersion(version: string): string {
    return version
        .replace(/\.dev/, '-dev')
        .replace(/(\d+)rc(\d+)/, '$1-rc.$2');
}

export default function WorkflowManagePanel({
    wfId,
    workflowDetails,
    onBack,
    onClose: _onClose,
}: WorkflowManagePanelProps) {
    const [wfFieldValues, setWfFieldValues] = useState<number | null>(null);
    const [wfTaskStatusUpdateMsg, setWfTaskStatusUpdateMsg] = useState('');

    const disabled = workflowDetails
        ? !compare(normalizeVersion(workflowDetails.wfr_jobmon_version), '3.3', '>')
        : true;

    const wfConcurrencyQuery = useQuery({
        queryKey: ['workflow_details', 'wf_concurrency', wfId],
        queryFn: () =>
            axios
                .get<WorkflowData>(get_workflow_concurrency_url(wfId), {
                    ...jobmonAxiosConfig,
                    data: null,
                })
                .then(r => r.data.max_concurrently_running),
    });

    useEffect(() => {
        if (wfConcurrencyQuery.data != null) {
            setWfFieldValues(wfConcurrencyQuery.data);
        }
    }, [wfConcurrencyQuery.data]);

    const updateWfConcurrency = useMutation({
        mutationFn: async ({ max_tasks }: { max_tasks: string }) => {
            return axios.put(
                set_wf_concurrency_url(wfId),
                { max_tasks: max_tasks },
                jobmonAxiosConfig
            );
        },
    });

    const extractErrorMessage = (error: any): string => {
        if (error.response?.data?.detail) {
            return error.response.data.detail;
        }
        if (error.response?.data?.error?.exception_message) {
            return error.response.data.error.exception_message;
        }
        return error.message || error.toString();
    };

    const handleWfInputChange = (
        event: React.ChangeEvent<HTMLInputElement>
    ) => {
        const value =
            event.target.value === '' ? '' : Number(event.target.value);
        if (value === '' || (value >= 0 && value <= 2147483647)) {
            setWfFieldValues(value === '' ? 0 : value);
        }
        updateWfConcurrency.mutate({
            max_tasks: value.toString(),
        });
    };

    const handleUpdateStatusAll = (status: string) => {
        setWfTaskStatusUpdateMsg('Updating...');
        axios
            .put(
                update_task_status_url,
                {
                    workflow_id: wfId,
                    new_status: status,
                    recursive: false,
                    task_ids: 'all',
                },
                jobmonAxiosConfig
            )
            .then(() => {
                setWfTaskStatusUpdateMsg('Success');
            })
            .catch(error => {
                setWfTaskStatusUpdateMsg(
                    `Error: ${extractErrorMessage(error)}`
                );
            });
    };

    const msgColor = (msg: string) =>
        msg === 'Success' ? 'success.main' : 'error';

    return (
        <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            {/* Header */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                <IconButton size="small" onClick={onBack}>
                    <ArrowBackIcon />
                </IconButton>
                <Typography variant="subtitle2" sx={{ flex: 1 }}>
                    Manage Workflow
                </Typography>
            </Box>

            {/* Stop Workflow */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                <StopWorkflowButton wf_id={wfId} disabled={disabled} />
                <Tooltip
                    title="Halts workflow, terminates run, fails running tasks/instances"
                    placement="right"
                >
                    <InfoIcon fontSize="small" color="action" sx={{ cursor: 'help' }} />
                </Tooltip>
            </Box>

            <Divider sx={{ mb: 1.5 }} />

            {/* Workflow-level controls */}
            <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>
                Workflow
            </Typography>
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start', mb: 1 }}>
                <TextField
                    label="Concurrency"
                    value={wfFieldValues ?? ''}
                    onChange={handleWfInputChange}
                    inputProps={{ step: 1, min: 0, max: 2147483647, type: 'number' }}
                    variant="outlined"
                    size="small"
                    disabled={disabled}
                    sx={{ flex: 1 }}
                />
                <FormControl variant="outlined" size="small" sx={{ flex: 1 }}>
                    <InputLabel id="wf-status-label">All Tasks</InputLabel>
                    <Select
                        labelId="wf-status-label"
                        label="All Tasks"
                        onChange={e => handleUpdateStatusAll(e.target.value as string)}
                    >
                        <MenuItem value="D">Done</MenuItem>
                        <MenuItem value="G">Registered</MenuItem>
                    </Select>
                </FormControl>
                <Tooltip
                    title="Done: mark task only. Registered: also resets downstream tasks."
                    placement="right"
                >
                    <InfoIcon fontSize="small" color="action" sx={{ mt: 1, cursor: 'help' }} />
                </Tooltip>
            </Box>
            {wfTaskStatusUpdateMsg !== '' && (
                <Typography variant="caption" color={msgColor(wfTaskStatusUpdateMsg)} sx={{ display: 'block', mb: 0.5 }}>
                    {wfTaskStatusUpdateMsg}
                </Typography>
            )}
        </Box>
    );
}
