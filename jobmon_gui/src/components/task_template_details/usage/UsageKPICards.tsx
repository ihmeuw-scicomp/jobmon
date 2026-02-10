// KPI Cards component for Usage analysis

import React from 'react';
import {
    Box,
    Card,
    CardContent,
    Grid,
    Typography,
    LinearProgress,
    Chip,
} from '@mui/material';
import {
    Memory as MemoryIcon,
    Timer as TimerIcon,
    Info as InfoIcon,
} from '@mui/icons-material';
import humanizeDuration from 'humanize-duration';
import {
    UsageKPIStats,
    ResourceEfficiencyMetrics,
} from '@jobmon_gui/types/Usage';

interface UsageKPICardsProps {
    kpiStats: UsageKPIStats;
    resourceEfficiency: ResourceEfficiencyMetrics;
    selectedDataCount?: number; // Add prop to show if we're viewing selected data
    totalDataCount?: number;
}

const UsageKPICards: React.FC<UsageKPICardsProps> = ({
    kpiStats,
    resourceEfficiency,
    selectedDataCount,
    totalDataCount,
}) => {
    const isShowingSelection =
        selectedDataCount !== undefined && selectedDataCount > 0;

    // Helper function to get efficiency status
    const getEfficiencyStatus = (utilization: number) => {
        if (utilization > 90) {
            return {
                label: 'UNDER-ALLOCATED',
                description: 'High Risk',
                color: 'error' as const,
                bgColor: 'error.50',
                textColor: 'error.main',
            };
        } else if (utilization < 50) {
            return {
                label: 'OVER-ALLOCATED',
                description: 'Wasteful',
                color: 'warning' as const,
                bgColor: 'warning.50',
                textColor: 'warning.main',
            };
        } else {
            return {
                label: 'WELL-ALLOCATED',
                description: 'Optimal',
                color: 'success' as const,
                bgColor: 'success.50',
                textColor: 'success.main',
            };
        }
    };

    const runtimeStatus = getEfficiencyStatus(
        resourceEfficiency.runtimeUtilization
    );
    const memoryStatus = getEfficiencyStatus(
        resourceEfficiency.memoryUtilization
    );

    return (
        <Box sx={{ mb: 1 }}>
            {/* Data Source Indicator */}
            {isShowingSelection && (
                <Box
                    sx={{
                        bgcolor: 'primary.50',
                        color: 'primary.main',
                        p: 1,
                        borderRadius: 1,
                        display: 'flex',
                        alignItems: 'center',
                        gap: 1,
                        border: '1px solid',
                        borderColor: 'primary.200',
                        mb: 1,
                    }}
                >
                    <InfoIcon sx={{ fontSize: 16 }} />
                    <Typography variant="caption" fontWeight="medium">
                        Showing statistics for{' '}
                        <strong>{selectedDataCount}</strong> selected points
                        (out of {totalDataCount} total)
                    </Typography>
                </Box>
            )}

            {/* KPI Cards Grid */}
            <Grid container spacing={1}>
                {/* Runtime Stats Card */}
                <Grid item xs={12} md={6}>
                    <Card
                        elevation={0}
                        sx={{
                            height: '100%',
                            border: '1px solid',
                            borderColor: 'divider',
                            borderRadius: 2,
                            transition: 'all 0.2s ease-in-out',
                            '&:hover': {
                                boxShadow: 3,
                                borderColor: 'primary.main',
                            },
                        }}
                    >
                        <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                            <Box display="flex" alignItems="center" mb={1.5}>
                                <Box
                                    sx={{
                                        bgcolor: 'primary.50',
                                        color: 'primary.main',
                                        p: 0.75,
                                        borderRadius: 1,
                                        mr: 1.5,
                                    }}
                                >
                                    <TimerIcon sx={{ fontSize: 18 }} />
                                </Box>
                                <Typography
                                    variant="subtitle1"
                                    component="div"
                                    fontWeight="bold"
                                >
                                    Runtime Analysis
                                </Typography>
                            </Box>

                            <Grid container spacing={1.5}>
                                <Grid item xs={4}>
                                    <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        fontWeight="medium"
                                        sx={{
                                            textTransform: 'uppercase',
                                            letterSpacing: 0.5,
                                        }}
                                    >
                                        Minimum
                                    </Typography>
                                    <Typography
                                        variant="body2"
                                        fontWeight="bold"
                                        sx={{ mt: 0.5 }}
                                    >
                                        {kpiStats.minRuntime !== undefined
                                            ? humanizeDuration(
                                                kpiStats.minRuntime * 1000,
                                                {
                                                    largest: 1,
                                                    round: true,
                                                }
                                            )
                                            : 'N/A'}
                                    </Typography>
                                </Grid>
                                <Grid item xs={4}>
                                    <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        fontWeight="medium"
                                        sx={{
                                            textTransform: 'uppercase',
                                            letterSpacing: 0.5,
                                        }}
                                    >
                                        Maximum
                                    </Typography>
                                    <Typography
                                        variant="body2"
                                        fontWeight="bold"
                                        sx={{ mt: 0.5 }}
                                    >
                                        {kpiStats.maxRuntime !== undefined
                                            ? humanizeDuration(
                                                kpiStats.maxRuntime * 1000,
                                                {
                                                    largest: 1,
                                                    round: true,
                                                }
                                            )
                                            : 'N/A'}
                                    </Typography>
                                </Grid>
                                <Grid item xs={4}>
                                    <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        fontWeight="medium"
                                        sx={{
                                            textTransform: 'uppercase',
                                            letterSpacing: 0.5,
                                        }}
                                    >
                                        Median
                                    </Typography>
                                    <Typography
                                        variant="body2"
                                        fontWeight="bold"
                                        sx={{ mt: 0.5 }}
                                    >
                                        {kpiStats.medianRuntime !== undefined
                                            ? humanizeDuration(
                                                kpiStats.medianRuntime * 1000,
                                                {
                                                    largest: 1,
                                                    round: true,
                                                }
                                            )
                                            : 'N/A'}
                                    </Typography>
                                </Grid>

                                {/* Efficiency Row */}
                                <Grid item xs={12} sx={{ mt: 0.5 }}>
                                    <Box
                                        sx={{
                                            bgcolor: 'grey.50',
                                            p: 1,
                                            borderRadius: 1,
                                            border: '1px solid',
                                            borderColor: 'grey.200',
                                        }}
                                    >
                                        <Box
                                            display="flex"
                                            alignItems="center"
                                            justifyContent="space-between"
                                            mb={0.5}
                                        >
                                            <Typography
                                                variant="caption"
                                                color="text.secondary"
                                                fontWeight="medium"
                                                sx={{
                                                    textTransform: 'uppercase',
                                                    letterSpacing: 0.5,
                                                }}
                                            >
                                                Efficiency
                                            </Typography>
                                            <Chip
                                                label={runtimeStatus.label}
                                                size="small"
                                                sx={{
                                                    fontSize: '0.6rem',
                                                    height: 18,
                                                    backgroundColor:
                                                        runtimeStatus.bgColor,
                                                    color: runtimeStatus.textColor,
                                                    fontWeight: 'bold',
                                                }}
                                            />
                                        </Box>

                                        <Box mb={1}>
                                            <Box
                                                display="flex"
                                                alignItems="center"
                                                justifyContent="space-between"
                                                mb={0.25}
                                            >
                                                <Typography
                                                    variant="body2"
                                                    fontWeight="bold"
                                                    sx={{
                                                        color: runtimeStatus.textColor,
                                                    }}
                                                >
                                                    {resourceEfficiency.runtimeUtilization.toFixed(
                                                        1
                                                    )}
                                                    % utilized
                                                </Typography>
                                                <Typography
                                                    variant="caption"
                                                    color="text.secondary"
                                                >
                                                    {runtimeStatus.description}
                                                </Typography>
                                            </Box>
                                            <LinearProgress
                                                variant="determinate"
                                                value={Math.min(
                                                    resourceEfficiency.runtimeUtilization,
                                                    100
                                                )}
                                                sx={{
                                                    height: 6,
                                                    borderRadius: 3,
                                                    backgroundColor: 'grey.200',
                                                    '& .MuiLinearProgress-bar':
                                                        {
                                                            borderRadius: 3,
                                                            backgroundColor:
                                                                runtimeStatus.textColor,
                                                        },
                                                }}
                                            />
                                        </Box>

                                        <Box>
                                            <Typography
                                                variant="caption"
                                                color="text.secondary"
                                                fontWeight="medium"
                                                sx={{
                                                    textTransform: 'uppercase',
                                                    letterSpacing: 0.5,
                                                }}
                                            >
                                                Median Requested
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                fontWeight="bold"
                                            >
                                                {kpiStats.medianRequestedRuntime !==
                                                undefined
                                                    ? humanizeDuration(
                                                        kpiStats.medianRequestedRuntime *
                                                              1000,
                                                        {
                                                            largest: 1,
                                                            round: true,
                                                        }
                                                    )
                                                    : 'N/A'}
                                            </Typography>
                                        </Box>
                                    </Box>
                                </Grid>
                            </Grid>
                        </CardContent>
                    </Card>
                </Grid>

                {/* Memory Stats Card */}
                <Grid item xs={12} md={6}>
                    <Card
                        elevation={0}
                        sx={{
                            height: '100%',
                            border: '1px solid',
                            borderColor: 'divider',
                            borderRadius: 2,
                            transition: 'all 0.2s ease-in-out',
                            '&:hover': {
                                boxShadow: 3,
                                borderColor: 'secondary.main',
                            },
                        }}
                    >
                        <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                            <Box display="flex" alignItems="center" mb={1.5}>
                                <Box
                                    sx={{
                                        bgcolor: 'secondary.50',
                                        color: 'secondary.main',
                                        p: 0.75,
                                        borderRadius: 1,
                                        mr: 1.5,
                                    }}
                                >
                                    <MemoryIcon sx={{ fontSize: 18 }} />
                                </Box>
                                <Typography
                                    variant="subtitle1"
                                    component="div"
                                    fontWeight="bold"
                                >
                                    Memory Analysis
                                </Typography>
                            </Box>

                            <Grid container spacing={1.5}>
                                <Grid item xs={4}>
                                    <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        fontWeight="medium"
                                        sx={{
                                            textTransform: 'uppercase',
                                            letterSpacing: 0.5,
                                        }}
                                    >
                                        Minimum
                                    </Typography>
                                    <Typography
                                        variant="body2"
                                        fontWeight="bold"
                                        sx={{ mt: 0.5 }}
                                    >
                                        {kpiStats.minMemoryGiB !== undefined
                                            ? `${kpiStats.minMemoryGiB.toFixed(2)} GiB`
                                            : 'N/A'}
                                    </Typography>
                                </Grid>
                                <Grid item xs={4}>
                                    <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        fontWeight="medium"
                                        sx={{
                                            textTransform: 'uppercase',
                                            letterSpacing: 0.5,
                                        }}
                                    >
                                        Maximum
                                    </Typography>
                                    <Typography
                                        variant="body2"
                                        fontWeight="bold"
                                        sx={{ mt: 0.5 }}
                                    >
                                        {kpiStats.maxMemoryGiB !== undefined
                                            ? `${kpiStats.maxMemoryGiB.toFixed(2)} GiB`
                                            : 'N/A'}
                                    </Typography>
                                </Grid>
                                <Grid item xs={4}>
                                    <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        fontWeight="medium"
                                        sx={{
                                            textTransform: 'uppercase',
                                            letterSpacing: 0.5,
                                        }}
                                    >
                                        Median
                                    </Typography>
                                    <Typography
                                        variant="body2"
                                        fontWeight="bold"
                                        sx={{ mt: 0.5 }}
                                    >
                                        {kpiStats.medianMemoryGiB !== undefined
                                            ? `${kpiStats.medianMemoryGiB.toFixed(2)} GiB`
                                            : 'N/A'}
                                    </Typography>
                                </Grid>

                                {/* Efficiency Row */}
                                <Grid item xs={12} sx={{ mt: 0.5 }}>
                                    <Box
                                        sx={{
                                            bgcolor: 'grey.50',
                                            p: 1,
                                            borderRadius: 1,
                                            border: '1px solid',
                                            borderColor: 'grey.200',
                                        }}
                                    >
                                        <Box
                                            display="flex"
                                            alignItems="center"
                                            justifyContent="space-between"
                                            mb={0.5}
                                        >
                                            <Typography
                                                variant="caption"
                                                color="text.secondary"
                                                fontWeight="medium"
                                                sx={{
                                                    textTransform: 'uppercase',
                                                    letterSpacing: 0.5,
                                                }}
                                            >
                                                Efficiency
                                            </Typography>
                                            <Chip
                                                label={memoryStatus.label}
                                                size="small"
                                                sx={{
                                                    fontSize: '0.6rem',
                                                    height: 18,
                                                    backgroundColor:
                                                        memoryStatus.bgColor,
                                                    color: memoryStatus.textColor,
                                                    fontWeight: 'bold',
                                                }}
                                            />
                                        </Box>

                                        <Box mb={1}>
                                            <Box
                                                display="flex"
                                                alignItems="center"
                                                justifyContent="space-between"
                                                mb={0.25}
                                            >
                                                <Typography
                                                    variant="body2"
                                                    fontWeight="bold"
                                                    sx={{
                                                        color: memoryStatus.textColor,
                                                    }}
                                                >
                                                    {resourceEfficiency.memoryUtilization.toFixed(
                                                        1
                                                    )}
                                                    % utilized
                                                </Typography>
                                                <Typography
                                                    variant="caption"
                                                    color="text.secondary"
                                                >
                                                    {memoryStatus.description}
                                                </Typography>
                                            </Box>
                                            <LinearProgress
                                                variant="determinate"
                                                value={Math.min(
                                                    resourceEfficiency.memoryUtilization,
                                                    100
                                                )}
                                                sx={{
                                                    height: 6,
                                                    borderRadius: 3,
                                                    backgroundColor: 'grey.200',
                                                    '& .MuiLinearProgress-bar':
                                                        {
                                                            borderRadius: 3,
                                                            backgroundColor:
                                                                memoryStatus.textColor,
                                                        },
                                                }}
                                            />
                                        </Box>

                                        <Box>
                                            <Typography
                                                variant="caption"
                                                color="text.secondary"
                                                fontWeight="medium"
                                                sx={{
                                                    textTransform: 'uppercase',
                                                    letterSpacing: 0.5,
                                                }}
                                            >
                                                Median Requested
                                            </Typography>
                                            <Typography
                                                variant="body2"
                                                fontWeight="bold"
                                            >
                                                {kpiStats.medianRequestedMemoryGiB !==
                                                undefined
                                                    ? `${kpiStats.medianRequestedMemoryGiB.toFixed(2)} GiB`
                                                    : 'N/A'}
                                            </Typography>
                                        </Box>
                                    </Box>
                                </Grid>
                            </Grid>
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </Box>
    );
};

export default UsageKPICards;
