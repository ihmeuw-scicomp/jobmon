import React, { useState, useMemo } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import BuildIcon from '@mui/icons-material/Build';
import humanizeDuration from 'humanize-duration';
import { formatJobmonDate } from '@jobmon_gui/utils/DayTime.ts';
import { WorkflowDetails } from '@jobmon_gui/types/WorkflowDetails.ts';
import { TTStatusResponse } from '@jobmon_gui/types/TaskTemplateStatus';
import {
    TEMPLATE_STATUS_COLORS,
    TEMPLATE_STATUS_KEYS,
    getStatusLabel,
} from '@jobmon_gui/constants/taskStatus';
import TemplateStatusBar from '@jobmon_gui/components/common/TemplateStatusBar';

interface WorkflowSummaryBarProps {
    workflowDetails?: WorkflowDetails;
    ttData?: TTStatusResponse;
    onManageClick?: () => void;
}

export default function WorkflowSummaryBar({
    workflowDetails,
    ttData,
    onManageClick,
}: WorkflowSummaryBarProps) {
    const [showMore, setShowMore] = useState(false);

    const { templates, totals } = useMemo(() => {
        const tpls = ttData ? Object.values(ttData) : [];
        const result = {
            PENDING: 0,
            SCHEDULED: 0,
            RUNNING: 0,
            DONE: 0,
            FATAL: 0,
            tasks: 0,
        };
        for (const tt of tpls) {
            result.PENDING += tt.PENDING;
            result.SCHEDULED += tt.SCHEDULED;
            result.RUNNING += tt.RUNNING;
            result.DONE += tt.DONE;
            result.FATAL += tt.FATAL;
            result.tasks += tt.tasks;
        }
        return { templates: tpls, totals: result };
    }, [ttData]);
    const templateCount = templates.length;

    const wfElapsed = workflowDetails
        ? humanizeDuration(
              new Date(workflowDetails.wfr_heartbeat_date).getTime() -
                  new Date(workflowDetails.wf_created_date).getTime(),
              { largest: 2, round: true }
          )
        : null;

    const statusDesc = workflowDetails
        ? getStatusLabel(workflowDetails.wf_status)
        : null;

    return (
        <Box
            sx={{
                px: 2,
                py: 1,
                borderBottom: '1px solid',
                borderColor: 'divider',
                flexShrink: 0,
            }}
        >
            {/* Top row: metadata + manage button */}
            {workflowDetails && (
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 1,
                        mb: 0.75,
                    }}
                >
                    <Chip
                        label={statusDesc}
                        size="small"
                        sx={{
                            height: 20,
                            fontSize: '0.7rem',
                            fontWeight: 600,
                        }}
                    />
                    {workflowDetails.wf_name && (
                        <Typography
                            variant="body2"
                            sx={{
                                fontWeight: 500,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {workflowDetails.wf_name}
                        </Typography>
                    )}
                    <Typography
                        variant="caption"
                        color="text.secondary"
                    >
                        {workflowDetails.wfr_user}
                        {' \u00b7 '}
                        {wfElapsed}
                        {' \u00b7 '}
                        {formatJobmonDate(
                            workflowDetails.wf_created_date
                        )}
                    </Typography>
                    <Typography
                        variant="caption"
                        color="primary"
                        sx={{
                            cursor: 'pointer',
                            '&:hover': {
                                textDecoration: 'underline',
                            },
                            flexShrink: 0,
                        }}
                        onClick={() => setShowMore(v => !v)}
                    >
                        {showMore ? 'Less' : 'More \u25B8'}
                    </Typography>
                    <Box sx={{ flex: 1 }} />
                    {onManageClick && (
                        <Tooltip title="Manage Workflow">
                            <IconButton
                                size="small"
                                onClick={onManageClick}
                            >
                                <BuildIcon fontSize="small" />
                            </IconButton>
                        </Tooltip>
                    )}
                </Box>
            )}

            {/* Expandable details */}
            {showMore && workflowDetails && (
                <Box
                    sx={{
                        display: 'grid',
                        gridTemplateColumns: 'auto 1fr',
                        gap: '1px 8px',
                        mb: 0.75,
                    }}
                >
                    <Typography
                        variant="caption"
                        color="text.secondary"
                    >
                        Tool
                    </Typography>
                    <Typography variant="caption">
                        {workflowDetails.tool_name}
                    </Typography>
                    <Typography
                        variant="caption"
                        color="text.secondary"
                    >
                        Args
                    </Typography>
                    <Typography
                        variant="caption"
                        sx={{ wordBreak: 'break-all' }}
                    >
                        {workflowDetails.wf_args}
                    </Typography>
                    <Typography
                        variant="caption"
                        color="text.secondary"
                    >
                        Heartbeat
                    </Typography>
                    <Typography variant="caption">
                        {formatJobmonDate(
                            workflowDetails.wfr_heartbeat_date
                        )}
                    </Typography>
                    <Typography
                        variant="caption"
                        color="text.secondary"
                    >
                        Version
                    </Typography>
                    <Typography variant="caption">
                        {workflowDetails.wfr_jobmon_version}
                    </Typography>
                </Box>
            )}

            {/* Progress bar + summary */}
            <Box sx={{ mb: 0.5 }}>
                <TemplateStatusBar counts={totals} height={18} showLabels />
            </Box>

            {/* Status chips row */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    gap: 0.5,
                }}
            >
                <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mr: 1 }}
                >
                    {totals.tasks.toLocaleString()} tasks across{' '}
                    {templateCount} template
                    {templateCount !== 1 ? 's' : ''}
                </Typography>
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
                                backgroundColor:
                                    TEMPLATE_STATUS_COLORS[key],
                                color:
                                    key === 'SCHEDULED'
                                        ? '#333'
                                        : '#fff',
                            }}
                        />
                    );
                })}
            </Box>
        </Box>
    );
}
