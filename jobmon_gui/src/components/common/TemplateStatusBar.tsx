import React from 'react';
import Box from '@mui/material/Box';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import {
    TEMPLATE_STATUS_COLORS,
    TEMPLATE_STATUS_KEYS,
} from '@jobmon_gui/constants/taskStatus';

interface StatusCounts {
    PENDING: number;
    SCHEDULED: number;
    RUNNING: number;
    DONE: number;
    FATAL: number;
    tasks: number;
    [key: string]: unknown;
}

interface TemplateStatusBarProps {
    counts: StatusCounts;
    height?: number;
    borderRadius?: number;
    showLabels?: boolean;
}

export default function TemplateStatusBar({
    counts,
    height = 8,
    borderRadius = 1,
    showLabels = false,
}: TemplateStatusBarProps) {
    if (counts.tasks === 0) return null;

    const barHeight = showLabels ? Math.max(height, 18) : height;

    return (
        <Box
            sx={{
                display: 'flex',
                height: barHeight,
                borderRadius,
                overflow: 'hidden',
            }}
        >
            {TEMPLATE_STATUS_KEYS.map(key => {
                const count = counts[key] as number;
                if (count === 0) return null;
                const pct = (count / counts.tasks) * 100;
                const pctLabel =
                    pct === 100
                        ? '100%'
                        : pct < 1
                          ? '<1%'
                          : `${Math.floor(pct)}%`;

                const segment = (
                    <Box
                        key={key}
                        sx={{
                            width: `${pct}%`,
                            backgroundColor:
                                TEMPLATE_STATUS_COLORS[key],
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            overflow: 'hidden',
                            minWidth: 0,
                        }}
                    >
                        {showLabels && pct >= 8 && (
                            <Typography
                                sx={{
                                    fontSize: '0.65rem',
                                    fontWeight: 600,
                                    color:
                                        key === 'SCHEDULED'
                                            ? '#333'
                                            : '#fff',
                                    lineHeight: 1,
                                    whiteSpace: 'nowrap',
                                }}
                            >
                                {pctLabel}
                            </Typography>
                        )}
                    </Box>
                );

                if (!showLabels) return segment;

                return (
                    <Tooltip
                        key={key}
                        title={`${key}: ${count.toLocaleString()} (${pctLabel})`}
                        arrow
                        placement="top"
                    >
                        {segment}
                    </Tooltip>
                );
            })}
        </Box>
    );
}
