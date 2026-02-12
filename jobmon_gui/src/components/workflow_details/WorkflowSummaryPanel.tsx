import React, { useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import BuildIcon from '@mui/icons-material/Build';
import ReplayIcon from '@mui/icons-material/Replay';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import humanizeDuration from 'humanize-duration';
import axios from 'axios';
import { formatJobmonDate } from '@jobmon_gui/utils/DayTime.ts';
import { WorkflowDetails } from '@jobmon_gui/types/WorkflowDetails.ts';
import { TTStatusResponse, TTStatus } from '@jobmon_gui/types/TaskTemplateStatus';
import { task_table_url, update_task_status_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import {
    TEMPLATE_STATUS_COLORS,
    TEMPLATE_STATUS_KEYS,
} from '@jobmon_gui/constants/taskStatus';
import TemplateStatusBar from '@jobmon_gui/components/common/TemplateStatusBar';

const STATUS_DESC: Record<string, string> = {
    A: 'Aborted',
    D: 'Done',
    F: 'Failed',
    G: 'Registered',
    H: 'Halted',
    I: 'Instantiated',
    O: 'Launched',
    Q: 'Queued',
    R: 'Running',
};

interface WorkflowSummaryPanelProps {
    ttData: TTStatusResponse;
    hoveredTemplateName: string | null;
    onTemplateSelect: (name: string) => void;
    onTemplateHover: (name: string | null) => void;
    onPrefetch: (tt: {
        task_template_version_id: string | number;
        name: string;
    }) => void;
    workflowDetails?: WorkflowDetails;
    workflowId?: string | number;
    onManageClick?: () => void;
}

export default function WorkflowSummaryPanel({
    ttData,
    hoveredTemplateName,
    onTemplateSelect,
    onTemplateHover,
    onPrefetch,
    workflowDetails,
    workflowId,
    onManageClick,
}: WorkflowSummaryPanelProps) {
    const [showMore, setShowMore] = useState(false);
    const [actionMsg, setActionMsg] = useState<Record<string | number, string>>({});
    const templates = Object.values(ttData);
    const templateCount = templates.length;

    // Aggregate status counts across all templates
    const totals = { PENDING: 0, SCHEDULED: 0, RUNNING: 0, DONE: 0, FATAL: 0, tasks: 0 };
    for (const tt of templates) {
        totals.PENDING += tt.PENDING;
        totals.SCHEDULED += tt.SCHEDULED;
        totals.RUNNING += tt.RUNNING;
        totals.DONE += tt.DONE;
        totals.FATAL += tt.FATAL;
        totals.tasks += tt.tasks;
    }

    // Templates with failures, sorted by FATAL count descending
    const failingTemplates = templates
        .filter(tt => tt.FATAL > 0)
        .sort((a, b) => b.FATAL - a.FATAL);

    const handleQuickAction = (tt: TTStatus, action: 'rerun' | 'skip') => {
        if (!workflowId) return;
        setActionMsg(prev => ({ ...prev, [tt.id]: 'Updating...' }));
        const newStatus = action === 'rerun' ? 'G' : 'D';
        const recursive = action === 'rerun';

        axios
            .get<{ tasks: { task_id: string | number }[] }>(
                task_table_url + workflowId,
                { params: { tt_name: tt.name }, ...jobmonAxiosConfig }
            )
            .then(r => {
                const taskIds = r.data.tasks.map(
                    (t: { task_id: string | number }) => t.task_id
                );
                if (taskIds.length > 10000 && recursive) {
                    setActionMsg(prev => ({
                        ...prev,
                        [tt.id]: 'Too many tasks — use manage panel',
                    }));
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
                if (r) setActionMsg(prev => ({ ...prev, [tt.id]: 'Done' }));
            })
            .catch(() => {
                setActionMsg(prev => ({ ...prev, [tt.id]: 'Error' }));
            });
    };

    const wfElapsed = workflowDetails
        ? humanizeDuration(
              new Date(workflowDetails.wfr_heartbeat_date).getTime() -
                  new Date(workflowDetails.wf_created_date).getTime(),
              { largest: 2, round: true }
          )
        : null;

    const statusDesc = workflowDetails
        ? STATUS_DESC[workflowDetails.wf_status] || workflowDetails.wf_status
        : null;

    return (
        <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            {/* Workflow metadata */}
            {workflowDetails && (
                <Box sx={{ mb: 1.5 }}>
                    {/* Title row: status + name + manage button */}
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                        <Chip
                            label={statusDesc}
                            size="small"
                            sx={{ height: 20, fontSize: '0.7rem', fontWeight: 600 }}
                        />
                        {workflowDetails.wf_name && (
                            <Typography
                                variant="body2"
                                sx={{
                                    fontWeight: 500,
                                    flex: 1,
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap',
                                }}
                            >
                                {workflowDetails.wf_name}
                            </Typography>
                        )}
                        {!workflowDetails.wf_name && <Box sx={{ flex: 1 }} />}
                        {onManageClick && (
                            <IconButton size="small" onClick={onManageClick} title="Manage Workflow">
                                <BuildIcon fontSize="small" />
                            </IconButton>
                        )}
                    </Box>

                    {/* Compact metadata line */}
                    <Typography variant="caption" color="text.secondary">
                        {workflowDetails.wfr_user}
                        {' \u00b7 '}
                        {wfElapsed}
                        {' \u00b7 '}
                        {formatJobmonDate(workflowDetails.wf_created_date)}
                    </Typography>

                    {/* Expandable details */}
                    {showMore && (
                        <Box
                            sx={{
                                display: 'grid',
                                gridTemplateColumns: 'auto 1fr',
                                gap: '1px 8px',
                                mt: 0.5,
                            }}
                        >
                            <Typography variant="caption" color="text.secondary">Tool</Typography>
                            <Typography variant="caption">{workflowDetails.tool_name}</Typography>
                            <Typography variant="caption" color="text.secondary">Args</Typography>
                            <Typography variant="caption" sx={{ wordBreak: 'break-all' }}>
                                {workflowDetails.wf_args}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">Heartbeat</Typography>
                            <Typography variant="caption">
                                {formatJobmonDate(workflowDetails.wfr_heartbeat_date)}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">Version</Typography>
                            <Typography variant="caption">{workflowDetails.wfr_jobmon_version}</Typography>
                        </Box>
                    )}

                    <Typography
                        variant="caption"
                        color="primary"
                        sx={{
                            cursor: 'pointer',
                            '&:hover': { textDecoration: 'underline' },
                            display: 'inline-block',
                        }}
                        onClick={() => setShowMore(v => !v)}
                    >
                        {showMore ? 'Less' : 'More \u25B8'}
                    </Typography>
                </Box>
            )}

            {/* Aggregated status bar */}
            <Box sx={{ mb: 0.5 }}>
                <TemplateStatusBar counts={totals} />
            </Box>

            {/* Summary line */}
            <Typography variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
                {totals.tasks.toLocaleString()} tasks across{' '}
                {templateCount} template{templateCount !== 1 ? 's' : ''}
            </Typography>

            {/* Status count chips */}
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 2 }}>
                {TEMPLATE_STATUS_KEYS.map(key => {
                    if (totals[key] === 0) return null;
                    return (
                        <Chip
                            key={key}
                            label={`${key} ${totals[key].toLocaleString()}`}
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

            {/* Needs Attention section */}
            {failingTemplates.length > 0 && (
                <Box sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                        <Typography variant="subtitle2">
                            Needs Attention
                        </Typography>
                        <Chip
                            label={`${failingTemplates.length}`}
                            size="small"
                            sx={{
                                height: 20,
                                fontSize: '0.75rem',
                                backgroundColor: TEMPLATE_STATUS_COLORS.FATAL,
                                color: '#fff',
                            }}
                        />
                    </Box>
                    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                        {failingTemplates.map(tt => (
                            <Box
                                key={tt.id}
                                onClick={() => onTemplateSelect(tt.name)}
                                onMouseEnter={() => {
                                    onTemplateHover(tt.name);
                                    onPrefetch(tt);
                                }}
                                onMouseLeave={() => onTemplateHover(null)}
                                sx={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                    p: 1,
                                    borderRadius: 1,
                                    border: '1px solid',
                                    borderColor: 'divider',
                                    cursor: 'pointer',
                                    backgroundColor:
                                        hoveredTemplateName === tt.name
                                            ? '#e3f2fd'
                                            : undefined,
                                    transition: 'background-color 0.15s ease',
                                    '&:hover': {
                                        backgroundColor: '#e3f2fd',
                                    },
                                }}
                            >
                                <Typography
                                    variant="body2"
                                    sx={{
                                        fontWeight: 500,
                                        overflow: 'hidden',
                                        textOverflow: 'ellipsis',
                                        whiteSpace: 'nowrap',
                                        flex: 1,
                                        mr: 1,
                                    }}
                                >
                                    {tt.name}
                                </Typography>
                                <Chip
                                    label={`${tt.FATAL} fatal`}
                                    size="small"
                                    sx={{
                                        height: 20,
                                        fontSize: '0.7rem',
                                        fontWeight: 'bold',
                                        backgroundColor: TEMPLATE_STATUS_COLORS.FATAL,
                                        color: '#fff',
                                        flexShrink: 0,
                                    }}
                                />
                                {workflowId && (
                                    <>
                                        <Tooltip title="Re-run: set to Registered (resets downstream)">
                                            <IconButton
                                                size="small"
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    handleQuickAction(tt, 'rerun');
                                                }}
                                            >
                                                <ReplayIcon sx={{ fontSize: 16 }} />
                                            </IconButton>
                                        </Tooltip>
                                        <Tooltip title="Skip: mark as Done">
                                            <IconButton
                                                size="small"
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    handleQuickAction(tt, 'skip');
                                                }}
                                            >
                                                <SkipNextIcon sx={{ fontSize: 16 }} />
                                            </IconButton>
                                        </Tooltip>
                                    </>
                                )}
                                {actionMsg[tt.id] && (
                                    <Typography
                                        variant="caption"
                                        color={
                                            actionMsg[tt.id] === 'Done'
                                                ? 'success.main'
                                                : 'error'
                                        }
                                        sx={{ flexShrink: 0 }}
                                    >
                                        {actionMsg[tt.id]}
                                    </Typography>
                                )}
                            </Box>
                        ))}
                    </Box>
                </Box>
            )}

            {/* All Templates list with mini status bars */}
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
                All Templates
            </Typography>
            <List sx={{ padding: 0 }}>
                {templates.map(tt => {
                    const isHovered = hoveredTemplateName === tt.name;
                    return (
                        <ListItem key={tt.id} disablePadding>
                            <ListItemButton
                                onClick={() => onTemplateSelect(tt.name)}
                                onMouseEnter={() => {
                                    onTemplateHover(tt.name);
                                    onPrefetch(tt);
                                }}
                                onMouseLeave={() => onTemplateHover(null)}
                                sx={{
                                    py: 0.75,
                                    px: 1.5,
                                    backgroundColor: isHovered
                                        ? '#e3f2fd !important'
                                        : undefined,
                                    transition: 'background-color 0.15s ease',
                                }}
                            >
                                <Box sx={{ width: '100%' }}>
                                    <Box
                                        sx={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'space-between',
                                            mb: 0.25,
                                        }}
                                    >
                                        <Typography
                                            variant="body2"
                                            sx={{
                                                fontWeight: 500,
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                whiteSpace: 'nowrap',
                                                flex: 1,
                                                mr: 1,
                                            }}
                                        >
                                            {tt.name}
                                        </Typography>
                                        <Typography
                                            variant="caption"
                                            color="text.secondary"
                                            sx={{ flexShrink: 0 }}
                                        >
                                            {tt.tasks.toLocaleString()}
                                        </Typography>
                                    </Box>
                                    {/* Mini status bar */}
                                    <TemplateStatusBar counts={tt} height={4} borderRadius={0.5} />
                                </Box>
                            </ListItemButton>
                        </ListItem>
                    );
                })}
            </List>
        </Box>
    );
}
