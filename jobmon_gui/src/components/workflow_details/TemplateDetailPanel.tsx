import React, { useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import Button from '@mui/material/Button';
import TextField from '@mui/material/TextField';
import CircularProgress from '@mui/material/CircularProgress';
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
import humanizeDuration from 'humanize-duration';
import axios from 'axios';
import { TTStatus } from '@jobmon_gui/types/TaskTemplateStatus';
import { getClusteredErrorsFn } from '@jobmon_gui/queries/GetClusteredErrors';
import { getWorkflowUsageQueryFn } from '@jobmon_gui/queries/GetWorkflowUsage';
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

const INITIAL_ERROR_DISPLAY = 3;

function formatRuntime(seconds: number | null | undefined): string {
    if (seconds == null) return 'N/A';
    return humanizeDuration(seconds * 1000, { largest: 1, round: true });
}

function formatMemory(bytes: number | null | undefined): string {
    if (bytes == null) return 'N/A';
    const gib = bytes / 1073741824;
    return `${gib.toFixed(1)} GiB`;
}

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
    const [showAllErrors, setShowAllErrors] = useState(false);
    const [concurrencyValue, setConcurrencyValue] = useState<number | string>(
        templateData.MAXC >= MAX_CONCURRENCY_SENTINEL ? '' : templateData.MAXC
    );
    const [statusMsg, setStatusMsg] = useState('');
    const [showManage, setShowManage] = useState(false);

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
        const val = e.target.value === '' ? '' : Number(e.target.value);
        if (val === '' || (val >= 0 && val <= 2147483647)) {
            setConcurrencyValue(val);
            updateConcurrency.mutate({
                task_template_version_id:
                    templateData.task_template_version_id.toString(),
                max_tasks: val.toString(),
            });
        }
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
            templateData.task_template_version_id,
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

    const usageData = usageQuery.data;
    const hasUsageData =
        usageData &&
        (usageData.median_runtime != null ||
            usageData.median_mem != null);

    const errorClusters = errorsQuery.data?.error_logs ?? [];
    const totalErrorClusters = errorClusters.length;
    const visibleErrors = showAllErrors
        ? errorClusters
        : errorClusters.slice(0, INITIAL_ERROR_DISPLAY);

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
                <TemplateStatusBar counts={templateData} />
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

            {/* Errors section */}
            {templateData.FATAL > 0 && (
                <Box sx={{ mb: 2 }}>
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            mb: 1,
                        }}
                    >
                        <Typography variant="subtitle2">
                            Errors
                        </Typography>
                        {errorsQuery.isSuccess && (
                            <Chip
                                label={`${totalErrorClusters} cluster${totalErrorClusters !== 1 ? 's' : ''}`}
                                size="small"
                                sx={{
                                    height: 20,
                                    fontSize: '0.75rem',
                                    backgroundColor: TEMPLATE_STATUS_COLORS.FATAL,
                                    color: '#fff',
                                }}
                            />
                        )}
                    </Box>

                    {errorsQuery.isLoading && (
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <CircularProgress size={16} />
                            <Typography variant="caption" color="text.secondary">
                                Loading errors...
                            </Typography>
                        </Box>
                    )}

                    {errorsQuery.isError && (
                        <Typography variant="caption" color="text.secondary">
                            Could not load errors
                        </Typography>
                    )}

                    {errorsQuery.isSuccess && (
                        <>
                            <Box
                                sx={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    gap: 1,
                                }}
                            >
                                {visibleErrors.map((cluster, idx) => (
                                    <Box
                                        key={idx}
                                        sx={{
                                            display: 'flex',
                                            gap: 1,
                                            p: 1,
                                            borderRadius: 1,
                                            border: '1px solid',
                                            borderColor: 'divider',
                                        }}
                                    >
                                        <Chip
                                            label={`\u00d7${cluster.group_instance_count}`}
                                            size="small"
                                            sx={{
                                                height: 22,
                                                fontSize: '0.75rem',
                                                fontWeight: 'bold',
                                                backgroundColor:
                                                    TEMPLATE_STATUS_COLORS.FATAL,
                                                color: '#fff',
                                                flexShrink: 0,
                                            }}
                                        />
                                        <Typography
                                            variant="body2"
                                            sx={{
                                                fontSize: '0.8rem',
                                                overflow: 'hidden',
                                                display: '-webkit-box',
                                                WebkitLineClamp: 3,
                                                WebkitBoxOrient:
                                                    'vertical',
                                                wordBreak: 'break-all',
                                                lineHeight: 1.3,
                                            }}
                                        >
                                            {cluster.sample_error}
                                        </Typography>
                                    </Box>
                                ))}
                            </Box>

                            {totalErrorClusters > INITIAL_ERROR_DISPLAY &&
                                !showAllErrors && (
                                    <Typography
                                        variant="body2"
                                        color="primary"
                                        sx={{
                                            mt: 1,
                                            cursor: 'pointer',
                                            '&:hover': {
                                                textDecoration: 'underline',
                                            },
                                        }}
                                        onClick={() => setShowAllErrors(true)}
                                    >
                                        Show all {totalErrorClusters} clusters ▸
                                    </Typography>
                                )}
                        </>
                    )}
                </Box>
            )}

            {/* Resources section */}
            {usageQuery.isLoading && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
                    <CircularProgress size={16} />
                    <Typography variant="caption" color="text.secondary">
                        Loading resources...
                    </Typography>
                </Box>
            )}

            {hasUsageData && (
                <Box
                    sx={{
                        display: 'grid',
                        gridTemplateColumns: 'auto 1fr',
                        gap: '2px 8px',
                        mb: 1.5,
                    }}
                >
                    <Typography variant="caption" color="text.secondary" fontWeight={600}>
                        Resources
                    </Typography>
                    <Box />
                    {usageData.median_runtime != null && (
                        <>
                            <Typography variant="caption" color="text.secondary">Runtime</Typography>
                            <Typography variant="caption">
                                {formatRuntime(usageData.median_runtime)}
                                {' ('}
                                {formatRuntime(usageData.min_runtime)}
                                {' – '}
                                {formatRuntime(usageData.max_runtime)}
                                {')'}
                            </Typography>
                        </>
                    )}
                    {usageData.median_mem != null && (
                        <>
                            <Typography variant="caption" color="text.secondary">Memory</Typography>
                            <Typography variant="caption">
                                {formatMemory(usageData.median_mem)}
                                {' ('}
                                {formatMemory(usageData.min_mem)}
                                {' – '}
                                {formatMemory(usageData.max_mem)}
                                {')'}
                            </Typography>
                        </>
                    )}
                </Box>
            )}

        </Box>
    );
}
