import React from 'react';
import Box from '@mui/material/Box';
import LinearProgress from '@mui/material/LinearProgress';
import Typography from '@mui/material/Typography';

type ResourceComparisonBarProps = {
    label: string;
    requested: number | null;
    utilized: number | null;
    requestedDisplay: string;
    utilizedDisplay: string;
};

function getBarColor(percent: number): string {
    if (percent > 95) return '#d32f2f';
    if (percent >= 80) return '#ed6c02';
    return '#2e7d32';
}

export default function ResourceComparisonBar({
    label,
    requested,
    utilized,
    requestedDisplay,
    utilizedDisplay,
}: ResourceComparisonBarProps) {
    const hasRequested = requested != null && requested > 0;
    const hasUtilized = utilized != null && utilized > 0;

    // Both missing: show "No data"
    if (!hasRequested && !hasUtilized) {
        return (
            <Box sx={{ mb: 1 }}>
                <Typography variant="body2" fontWeight={600}>
                    {label}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                    No data
                </Typography>
            </Box>
        );
    }

    // Utilized exists but no requested limit
    if (!hasRequested && hasUtilized) {
        return (
            <Box sx={{ mb: 1 }}>
                <Typography variant="body2" fontWeight={600}>
                    {label}
                </Typography>
                <Typography variant="body2">
                    {utilizedDisplay}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                    Not requested
                </Typography>
            </Box>
        );
    }

    // Normal: both requested and utilized available
    const usedVal = utilized ?? 0;
    const percent = Math.min((usedVal / requested!) * 100, 100);
    const barColor = getBarColor(percent);

    return (
        <Box sx={{ mb: 1 }}>
            <Box
                sx={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'baseline',
                    mb: 0.5,
                }}
            >
                <Typography variant="body2" fontWeight={600}>
                    {label}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                    {utilizedDisplay} / {requestedDisplay}
                </Typography>
            </Box>
            <LinearProgress
                variant="determinate"
                value={percent}
                sx={{
                    height: 6,
                    borderRadius: 3,
                    backgroundColor: 'action.hover',
                    '& .MuiLinearProgress-bar': {
                        backgroundColor: barColor,
                        borderRadius: 3,
                    },
                }}
            />
            <Typography
                variant="caption"
                color="text.secondary"
                sx={{ mt: 0.25, display: 'block' }}
            >
                {percent.toFixed(0)}% utilized
            </Typography>
        </Box>
    );
}
