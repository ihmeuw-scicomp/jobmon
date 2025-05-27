// Plot section component for Usage analysis

import React from 'react';
import { Box, Paper, Skeleton, Typography } from '@mui/material';
import { Info as InfoIcon } from '@mui/icons-material';
import RuntimeMemoryScatterPlot from './RuntimeMemoryScatterPlot';
import { ScatterDataPoint } from '@jobmon_gui/types/Usage';

interface UsagePlotSectionProps {
    isLoading: boolean;
    filteredScatterData: ScatterDataPoint[];
    taskTemplateName: string;
    medianRequestedRuntime?: number;
    medianRequestedMemoryGiB?: number;
    showResourceZones: boolean;
    onTaskClick: (taskId: number | string) => void;
    onSelected: (selectedPoints: ScatterDataPoint[]) => void;
}

const UsagePlotSection: React.FC<UsagePlotSectionProps> = ({
    isLoading,
    filteredScatterData,
    taskTemplateName,
    medianRequestedRuntime,
    medianRequestedMemoryGiB,
    showResourceZones,
    onTaskClick,
    onSelected,
}) => {
    return (
        <Paper
            elevation={0}
            sx={{
                mx: { xs: 1, sm: 2 },
                mb: 2,
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 2,
            }}
        >
            <Box sx={{ p: 3, pb: 1 }}>
                <Typography
                    variant="h6"
                    fontWeight="bold"
                    color="primary.main"
                    sx={{ mb: 0.5 }}
                >
                    Interactive Scatter Plot
                </Typography>
                <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mb: 2 }}
                >
                    Runtime vs memory usage patterns • Use toolbar: 🔍 to
                    zoom/pan, 📦 for box select, 🎯 for lasso select •{' '}
                    <strong>
                        Statistics above update when you select data
                    </strong>{' '}
                    • Click points to view task details
                </Typography>
            </Box>

            <Box sx={{ height: '600px', position: 'relative' }}>
                {isLoading ? (
                    <Box sx={{ p: 3, height: '100%' }}>
                        <Skeleton
                            variant="rectangular"
                            height="100%"
                            sx={{ borderRadius: 1 }}
                        />
                    </Box>
                ) : filteredScatterData.length > 0 ? (
                    <Box sx={{ height: '100%', p: { xs: 1, sm: 2 }, pt: 0 }}>
                        <RuntimeMemoryScatterPlot
                            data={filteredScatterData}
                            onTaskClick={onTaskClick}
                            medianRequestedRuntime={medianRequestedRuntime}
                            medianRequestedMemory={medianRequestedMemoryGiB}
                            taskTemplateName={taskTemplateName}
                            showResourceZones={showResourceZones}
                            onSelected={onSelected}
                        />
                    </Box>
                ) : (
                    <Box
                        sx={{
                            height: '100%',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            flexDirection: 'column',
                            color: 'text.secondary',
                            p: 4,
                        }}
                    >
                        <InfoIcon sx={{ fontSize: 48, mb: 2, opacity: 0.3 }} />
                        <Typography
                            variant="h6"
                            sx={{ mb: 1, fontWeight: 'medium' }}
                        >
                            No Data Available
                        </Typography>
                        <Typography
                            variant="body2"
                            textAlign="center"
                            sx={{ maxWidth: 400 }}
                        >
                            No data matches the current filter criteria. Try
                            adjusting your filters or check if data exists for
                            this task template.
                        </Typography>
                    </Box>
                )}
            </Box>
        </Paper>
    );
};

export default UsagePlotSection;
