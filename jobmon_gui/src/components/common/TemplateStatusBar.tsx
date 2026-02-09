import React from 'react';
import Box from '@mui/material/Box';
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
}

export default function TemplateStatusBar({
    counts,
    height = 8,
    borderRadius = 1,
}: TemplateStatusBarProps) {
    if (counts.tasks === 0) return null;
    return (
        <Box
            sx={{
                display: 'flex',
                height,
                borderRadius,
                overflow: 'hidden',
            }}
        >
            {TEMPLATE_STATUS_KEYS.map(key => {
                const count = counts[key] as number;
                if (count === 0) return null;
                return (
                    <Box
                        key={key}
                        sx={{
                            width: `${(count / counts.tasks) * 100}%`,
                            backgroundColor: TEMPLATE_STATUS_COLORS[key],
                        }}
                    />
                );
            })}
        </Box>
    );
}
