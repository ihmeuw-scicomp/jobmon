import React, { useState, useEffect } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import Button from '@mui/material/Button';
import TextField from '@mui/material/TextField';
import Chip from '@mui/material/Chip';
import Tooltip from '@mui/material/Tooltip';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Select from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import BuildIcon from '@mui/icons-material/Build';
import InfoIcon from '@mui/icons-material/Info';
import { useQuery, useMutation } from '@tanstack/react-query';
import axios from 'axios';
import { TTStatus } from '@jobmon_gui/types/TaskTemplateStatus';
import { getClusteredErrorsFn } from '@jobmon_gui/queries/GetClusteredErrors';
import { getWorkflowUsageQueryFn } from '@jobmon_gui/queries/GetWorkflowUsage';
import ErrorClustersCard from '@jobmon_gui/components/task_template_details/usage/ErrorClustersCard';
import ResourceCard from '@jobmon_gui/components/workflow_details/ResourceCard';
import { getFatalErrorBreakdownFn } from '@jobmon_gui/queries/GetFatalErrorBreakdown';
import {
    set_task_template_concurrency_url,
    task_table_url,
    update_task_status_url,
} from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import {
    TEMPLATE_STATUS_COLORS,
    TEMPLATE_STATUS_KEYS,
} from '@jobmon_gui/constants/taskStatus';
import TemplateStatusBar from '@jobmon_gui/components/common/TemplateStatusBar';

const MAX_CONCURRENCY_SENTINEL = 2147483647;

interface TemplateDetailPanelProps {
    workflowId: string | number;
    templateData: TTStatus;
    onBack: () => void;
    onNavigate: () => void;
    disabled?: boolean;
}

export default function TemplateDetailPanel({
    workflowId,
    templateData,
    onBack,
    onNavigate,
    disabled,
}: TemplateDetailPanelProps) {
    const [concurrencyValue, setConcurrencyValue] = useState<
        number | string
    >(
        templateData.MAXC >= MAX_CONCURRENCY_SENTINEL
            ? ''
            : templateData.MAXC
    );
    const [statusMsg, setStatusMsg] = useState('');
    const [showManage, setShowManage] = useState(false);

    // Reset local state when switching templates
    useEffect(() => {
        setConcurrencyValue(
            templateData.MAXC >= MAX_CONCURRENCY_SENTINEL
                ? ''
                : templateData.MAXC
        );
        setStatusMsg('');
        setShowManage(false);
    }, [templateData.task_template_version_id]);

    const updateConcurrency = useMutation({
        mutationFn: async ({
            task_template_version_id,
            max_tasks,
        }: {
            task_template_version_id: string;
            max_tasks: string;
        }) => {
            return axios.put(
                set_task_template_concurrency_url(workflowId),
                { task_template_version_id, max_tasks },
                jobmonAxiosConfig
            );
        },
    });

    const handleConcurrencyChange = (
        e: React.ChangeEvent<HTMLInputElement>
    ) => {
        const val =
            e.target.value === '' ? '' : Number(e.target.value);
        if (val === '' || (val >= 0 && val <= 2147483647)) {
            setConcurrencyValue(val);
        }
    };

    const handleConcurrencyBlur = () => {
        updateConcurrency.mutate({
            task_template_version_id:
                templateData.task_template_version_id.toString(),
            max_tasks: concurrencyValue.toString(),
        });
    };

    const handleStatusUpdate = (action: 'rerun' | 'skip') => {
        setStatusMsg('Updating...');
        const newStatus = action === 'rerun' ? 'G' : 'D';
        const recursive = action === 'rerun';

        axios
            .get<{ tasks: { task_id: string | number }[] }>(
                task_table_url + workflowId,
                { params: { tt_name: templateData.name }, ...jobmonAxiosConfig }
            )
            .then(r => {
                const taskIds = r.data.tasks.map(
                    (t: { task_id: string | number }) => t.task_id
                );
                if (taskIds.length > 10000 && recursive) {
                    setStatusMsg('Too many tasks — use manage panel');
                    return;
                }
                return axios.put(
                    update_task_status_url,
                    {
                        workflow_id: workflowId,
                        task_ids: taskIds,
                        new_status: newStatus,
                        recursive,
                    },
                    jobmonAxiosConfig
                );
            })
            .then(r => {
                if (r) setStatusMsg('Success');
            })
            .catch(() => {
                setStatusMsg('Error');
            });
    };

    const errorsQuery = useQuery({
        queryKey: [
            'workflow_details',
            'clustered_errors',
            workflowId,
            templateData.id,
        ],
        queryFn: getClusteredErrorsFn,
        enabled: templateData.FATAL > 0,
    });

    const usageQuery = useQuery({
        queryKey: [
            'workflow_details',
            'usage',
            templateData.task_template_version_id,
            workflowId,
        ],
        queryFn: getWorkflowUsageQueryFn,
    });

    const breakdownQuery = useQuery({
        queryKey: [
            'workflow_details',
            'fatal_breakdown',
            workflowId,
            templateData.task_template_version_id,
        ],
        queryFn: getFatalErrorBreakdownFn,
        enabled: templateData.FATAL > 0,
        staleTime: 120000,
    });

    const errorClusters = errorsQuery.data?.error_logs ?? [];

    return (
        <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            {/* Header */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                    mb: 1,
                }}
            >
                <IconButton size="small" onClick={onBack}>
                    <ArrowBackIcon />
                </IconButton>
                <Typography variant="h6" sx={{ flex: 1, fontSize: '1rem' }}>
                    {templateData.name}
                </Typography>
                <Tooltip title="Manage Template">
                    <IconButton
                        size="small"
                        onClick={() => setShowManage(v => !v)}
                    >
                        <BuildIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
            </Box>
            <Button
                variant="outlined"
                fullWidth
                onClick={onNavigate}
                sx={{
                    mb: 2,
                    textTransform: 'none',
                    fontSize: '0.85rem',
                }}
            >
                View Task Details
            </Button>

            {/* Manage controls (toggled by wrench icon) */}
            {showManage && (
                <Box sx={{ mb: 1.5 }}>
                    <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>
                        Manage Template
                    </Typography>
                    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
                        <FormControl variant="outlined" size="small" sx={{ flex: 1 }} disabled={disabled}>
                            <InputLabel id="tt-status-label">Set Status</InputLabel>
                            <Select
                                labelId="tt-status-label"
                                label="Set Status"
                                onChange={e => {
                                    const val = e.target.value as string;
                                    if (val === 'G') handleStatusUpdate('rerun');
                                    else if (val === 'D') handleStatusUpdate('skip');
                                }}
                            >
                                <MenuItem value="G">Re-run</MenuItem>
                                <MenuItem value="D">Skip to Done</MenuItem>
                            </Select>
                        </FormControl>
                        <TextField
                            label="Concurrency"
                            value={concurrencyValue}
                            onChange={handleConcurrencyChange}
                            onBlur={handleConcurrencyBlur}
                            inputProps={{
                                step: 1,
                                min: 0,
                                max: 2147483647,
                                type: 'number',
                            }}
                            variant="outlined"
                            size="small"
                            disabled={disabled}
                            sx={{ flex: 1 }}
                            placeholder="\u221e"
                        />
                        <Tooltip
                            title="Skip to Done: mark tasks as done. Re-run: reset tasks and downstream."
                            placement="right"
                        >
                            <InfoIcon fontSize="small" color="action" sx={{ mt: 1, cursor: 'help' }} />
                        </Tooltip>
                    </Box>
                    {statusMsg && (
                        <Typography
                            variant="caption"
                            color={statusMsg === 'Success' ? 'success.main' : 'error'}
                            sx={{ display: 'block', mt: 0.5 }}
                        >
                            {statusMsg}
                        </Typography>
                    )}
                </Box>
            )}

            {/* Status bar + breakdown */}
            <Box sx={{ mb: 1.5 }}>
                <TemplateStatusBar counts={templateData} height={18} showLabels />
                <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mt: 0.5, mb: 0.5 }}
                >
                    {templateData.tasks.toLocaleString()} tasks
                    {' \u00b7 '}
                    {templateData.num_attempts_avg.toFixed(1)} avg attempts
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {TEMPLATE_STATUS_KEYS.map(key => {
                        const count = templateData[key];
                        if (count === 0) return null;
                        return (
                            <Chip
                                key={key}
                                label={`${key} ${count.toLocaleString()}`}
                                size="small"
                                sx={{
                                    height: 20,
                                    fontSize: '0.7rem',
                                    fontWeight: 600,
                                    backgroundColor: TEMPLATE_STATUS_COLORS[key],
                                    color: key === 'SCHEDULED' ? '#333' : '#fff',
                                }}
                            />
                        );
                    })}
                </Box>
            </Box>

            {/* Errors */}
            {templateData.FATAL > 0 && (
                <Box sx={{ mb: 1 }}>
                    <ErrorClustersCard
                        errorLogs={errorClusters}
                        isLoading={errorsQuery.isLoading}
                        workflowId={workflowId}
                        taskTemplateId={templateData.id}
                        maxListHeight={120}
                    />
                </Box>
            )}

            {/* Resources card */}
            <ResourceCard
                workflowId={workflowId}
                taskTemplateId={templateData.id}
                usageData={usageQuery.data ?? null}
                usageLoading={usageQuery.isLoading}
                breakdown={breakdownQuery.data}
                breakdownLoading={breakdownQuery.isLoading}
            />

        </Box>
    );
}
