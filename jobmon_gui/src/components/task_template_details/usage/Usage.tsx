import React, { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import Typography from '@mui/material/Typography';
import { Grid, Skeleton, Box } from '@mui/material';
import { useNavigate } from 'react-router-dom';
import {
    calculateResourceEfficiency,
    calculateMedian,
    getResourceClusterKey,
} from './usageCalculations';
import { bytes_to_gib } from '@jobmon_gui/utils/formatters';
import { useUsageFilters } from '../../../hooks/useUsageFilters';

// Components
import UsageKPICards from './UsageKPICards';
import UsageFilters from './UsageFilters';
import UsagePlotSection from './UsagePlotSection';

// Types and queries
import {
    getWorkflowUsageQueryFn,
    WorkflowUsageQueryKey,
} from '@jobmon_gui/queries/GetWorkflowUsage.ts';
import { components } from '@jobmon_gui/types/apiSchema';
import {
    ScatterDataPoint,
    UsageProps,
    UsageKPIStats,
    ResourceEfficiencyMetrics,
} from '@jobmon_gui/types/Usage';

// Type Aliases from apiSchema
type TaskTemplateResourceUsageResponse =
    components['schemas']['TaskTemplateResourceUsageResponse'];

export default function Usage({
    taskTemplateName,
    taskTemplateVersionId,
    workflowId,
}: UsageProps) {
    const navigate = useNavigate();

    const queryKeyForWorkflowUsage: WorkflowUsageQueryKey = [
        'workflow_details',
        'usage',
        taskTemplateVersionId,
        workflowId,
    ];

    const usageInfo = useQuery<
        TaskTemplateResourceUsageResponse | undefined,
        Error,
        TaskTemplateResourceUsageResponse | undefined,
        WorkflowUsageQueryKey
    >({
        queryKey: queryKeyForWorkflowUsage,
        queryFn: getWorkflowUsageQueryFn,
        staleTime: 5000,
    });

    const rawTaskNodesFromApi = usageInfo.data?.result_viz || [];

    // Use custom hook for filter management
    const {
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
        availableAttempts,
        availableStatuses,
        availableResourceClusters,
        setSelectedAttempts,
        setSelectedStatuses,
        setSelectedResourceClusters,
        resetFilters,
        clearFilters,
    } = useUsageFilters({ rawTaskNodesFromApi });

    // State for plot interactions
    const [selectedData, setSelectedData] = useState<ScatterDataPoint[]>([]);

    // Add new state for resource zones toggle
    const [showResourceZones, setShowResourceZones] = useState(false);

    // Calculate median requested resources from FILTERED nodes (dynamic based on current filters)
    const filteredRequestedRuntimes = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(d => {
                const clusterKey = getResourceClusterKey(d.requested_resources);
                return (
                    selectedAttempts.has(
                        String(d.attempt_number_of_instance || 1)
                    ) &&
                    selectedStatuses.has(
                        String(d.status || 'UNKNOWN').toUpperCase()
                    ) &&
                    (clusterKey === null ||
                        selectedResourceClusters.has(clusterKey))
                );
            })
            .map(item => {
                try {
                    const reqRes = item.requested_resources
                        ? JSON.parse(item.requested_resources)
                        : {};
                    const val = Number(reqRes.runtime);
                    return !isNaN(val) ? val : undefined;
                } catch {
                    return undefined;
                }
            });
    }, [
        rawTaskNodesFromApi,
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
    ]);

    const filteredRequestedMemoriesGiB = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(d => {
                const clusterKey = getResourceClusterKey(d.requested_resources);
                return (
                    selectedAttempts.has(
                        String(d.attempt_number_of_instance || 1)
                    ) &&
                    selectedStatuses.has(
                        String(d.status || 'UNKNOWN').toUpperCase()
                    ) &&
                    (clusterKey === null ||
                        selectedResourceClusters.has(clusterKey))
                );
            })
            .map(item => {
                try {
                    const reqRes = item.requested_resources
                        ? JSON.parse(item.requested_resources)
                        : {};
                    const val = Number(reqRes.memory); // Assuming this is already in GiB
                    return !isNaN(val) ? val : undefined;
                } catch {
                    return undefined;
                }
            });
    }, [
        rawTaskNodesFromApi,
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
    ]);

    const medianRequestedRuntime = useMemo(
        () => calculateMedian(filteredRequestedRuntimes),
        [filteredRequestedRuntimes]
    );
    const medianRequestedMemoryGiB = useMemo(
        () => calculateMedian(filteredRequestedMemoriesGiB),
        [filteredRequestedMemoriesGiB]
    );

    const filteredScatterData = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(d => {
                const clusterKey = getResourceClusterKey(d.requested_resources);
                return (
                    selectedAttempts.has(
                        String(d.attempt_number_of_instance || 1)
                    ) &&
                    selectedStatuses.has(
                        String(d.status || 'UNKNOWN').toUpperCase()
                    ) &&
                    (clusterKey === null ||
                        selectedResourceClusters.has(clusterKey))
                );
            })
            .map((item): ScatterDataPoint | null => {
                // Ensure item.r and item.m are numbers for scatter plot
                const runtime = typeof item.r === 'number' ? item.r : null;
                const memoryBytes = typeof item.m === 'number' ? item.m : null;
                const memoryGiB =
                    memoryBytes !== null ? bytes_to_gib(memoryBytes) : null;

                // Extract requested resources for this specific task
                let requestedRuntime: number | undefined;
                let requestedMemory: number | undefined;
                try {
                    const reqRes = item.requested_resources
                        ? JSON.parse(item.requested_resources)
                        : {};
                    const reqRuntimeVal = Number(reqRes.runtime);
                    const reqMemoryVal = Number(reqRes.memory);
                    requestedRuntime =
                        !isNaN(reqRuntimeVal) && reqRuntimeVal > 0
                            ? reqRuntimeVal
                            : undefined;
                    requestedMemory =
                        !isNaN(reqMemoryVal) && reqMemoryVal > 0
                            ? reqMemoryVal
                            : undefined;
                } catch {
                    // Skip invalid JSON, leave as undefined
                }

                if (
                    runtime !== null &&
                    runtime > 0 &&
                    memoryGiB !== null &&
                    memoryGiB > 0
                ) {
                    return {
                        task_id: item.task_id,
                        runtime: runtime,
                        memory: memoryGiB,
                        status: String(item.status || 'UNKNOWN').toUpperCase(),
                        attempt_num: item.attempt_number_of_instance || 1,
                        requestedRuntime: requestedRuntime,
                        requestedMemory: requestedMemory,
                    };
                }
                return null;
            })
            .filter((item): item is ScatterDataPoint => item !== null); // Filter out nulls
    }, [
        rawTaskNodesFromApi,
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
    ]);

    // Determine which data to use for KPI calculations: selected data if available, otherwise filtered data
    const dataForKPICalculations = useMemo(() => {
        return selectedData.length > 0 ? selectedData : filteredScatterData;
    }, [selectedData, filteredScatterData]);

    // Handle brush selection events
    const handleDataSelection = (selectedPoints: ScatterDataPoint[]) => {
        setSelectedData(selectedPoints);
    };

    // Calculate median requested resources from SELECTION when available, otherwise from FILTERED data
    const medianRequestedRuntimeForKPI = useMemo(() => {
        if (selectedData.length > 0) {
            // Calculate from selected task IDs
            const selectedTaskIds = new Set(selectedData.map(d => d.task_id));
            const selectedRequestedRuntimes = rawTaskNodesFromApi
                .filter(item => selectedTaskIds.has(item.task_id))
                .map(item => {
                    try {
                        const reqRes = item.requested_resources
                            ? JSON.parse(item.requested_resources)
                            : {};
                        const val = Number(reqRes.runtime);
                        return !isNaN(val) ? val : undefined;
                    } catch {
                        return undefined;
                    }
                });
            return calculateMedian(selectedRequestedRuntimes);
        }
        return medianRequestedRuntime;
    }, [selectedData, rawTaskNodesFromApi, medianRequestedRuntime]);

    const medianRequestedMemoryGiBForKPI = useMemo(() => {
        if (selectedData.length > 0) {
            // Calculate from selected task IDs
            const selectedTaskIds = new Set(selectedData.map(d => d.task_id));
            const selectedRequestedMemories = rawTaskNodesFromApi
                .filter(item => selectedTaskIds.has(item.task_id))
                .map(item => {
                    try {
                        const reqRes = item.requested_resources
                            ? JSON.parse(item.requested_resources)
                            : {};
                        const val = Number(reqRes.memory); // Assuming this is already in GiB
                        return !isNaN(val) ? val : undefined;
                    } catch {
                        return undefined;
                    }
                });
            return calculateMedian(selectedRequestedMemories);
        }
        return medianRequestedMemoryGiB;
    }, [selectedData, rawTaskNodesFromApi, medianRequestedMemoryGiB]);

    // Calculate KPI stats based on dataForKPICalculations (selected data if available, otherwise filtered data)
    const kpiRuntimes = useMemo(
        () => dataForKPICalculations.map(d => d.runtime),
        [dataForKPICalculations]
    );
    const kpiMemoriesGiB = useMemo(
        () => dataForKPICalculations.map(d => d.memory),
        [dataForKPICalculations]
    );

    const kpiStats: UsageKPIStats = useMemo(
        () => ({
            minRuntime:
                kpiRuntimes.length > 0 ? Math.min(...kpiRuntimes) : undefined,
            maxRuntime:
                kpiRuntimes.length > 0 ? Math.max(...kpiRuntimes) : undefined,
            meanRuntime:
                kpiRuntimes.length > 0
                    ? kpiRuntimes.reduce((a, b) => a + b, 0) /
                      kpiRuntimes.length
                    : undefined,
            medianRuntime: calculateMedian(kpiRuntimes),
            minMemoryGiB:
                kpiMemoriesGiB.length > 0
                    ? Math.min(...kpiMemoriesGiB)
                    : undefined,
            maxMemoryGiB:
                kpiMemoriesGiB.length > 0
                    ? Math.max(...kpiMemoriesGiB)
                    : undefined,
            meanMemoryGiB:
                kpiMemoriesGiB.length > 0
                    ? kpiMemoriesGiB.reduce((a, b) => a + b, 0) /
                      kpiMemoriesGiB.length
                    : undefined,
            medianMemoryGiB: calculateMedian(kpiMemoriesGiB),
            medianRequestedRuntime: medianRequestedRuntimeForKPI,
            medianRequestedMemoryGiB: medianRequestedMemoryGiBForKPI,
        }),
        [
            kpiRuntimes,
            kpiMemoriesGiB,
            medianRequestedRuntimeForKPI,
            medianRequestedMemoryGiBForKPI,
        ]
    );

    // Calculate resource efficiency metrics based on dataForKPICalculations
    const resourceEfficiency: ResourceEfficiencyMetrics = useMemo(() => {
        if (dataForKPICalculations.length === 0) {
            return {
                memoryUtilization: 0,
                runtimeUtilization: 0,
                overAllocatedMemory: 0,
                underAllocatedMemory: 0,
                overAllocatedRuntime: 0,
                underAllocatedRuntime: 0,
                p95Memory: undefined,
                p95Runtime: undefined,
                outlierCount: 0,
            };
        }

        return calculateResourceEfficiency(dataForKPICalculations);
    }, [dataForKPICalculations]);

    const handleScatterTaskClick = (clickedTaskId: number | string) => {
        navigate(`/task_details/${clickedTaskId}`);
    };

    if (!taskTemplateName) {
        return (
            <Typography sx={{ pt: 5 }}>
                Could not retrieve resource usage for this task template.
            </Typography>
        );
    }

    if (usageInfo.isLoading) {
        return (
            <div>
                <Grid
                    container
                    spacing={3}
                    style={{
                        marginTop: '20px',
                        paddingLeft: '15px',
                        paddingRight: '15px',
                        marginBottom: '30px',
                    }}
                    columns={{ xs: 12, sm: 12, md: 12 }}
                >
                    {[1, 2, 3].map(key => (
                        <Grid item xs={12} sm={4} key={key}>
                            <Skeleton variant="rectangular" height={150} />
                        </Grid>
                    ))}
                </Grid>
                {/* Simplified skeleton for plot area */}
                <Grid
                    container
                    spacing={3}
                    style={{
                        marginTop: '30px',
                        paddingLeft: '15px',
                        paddingRight: '15px',
                    }}
                    columns={{ xs: 12, sm: 12, md: 12 }}
                >
                    <Grid item xs={12}>
                        <Skeleton variant="rectangular" height={400} />
                    </Grid>
                </Grid>
            </div>
        );
    }

    if (usageInfo.isError) {
        return (
            <Typography>
                Unable to retrieve resource usage. Please refresh and try again
            </Typography>
        );
    }

    return (
        <Box sx={{ bgcolor: 'grey.50', minHeight: '100vh', pb: 4 }}>
            {/* Page Header */}
            <Box
                sx={{
                    mb: 3,
                    px: { xs: 1, sm: 2 },
                    py: 4,
                    bgcolor: 'white',
                    borderBottom: '1px solid',
                    borderColor: 'divider',
                }}
            >
                <Typography
                    variant="h4"
                    component="h1"
                    fontWeight="bold"
                    color="primary.main"
                    sx={{ mb: 1 }}
                >
                    Resource Usage Analysis
                </Typography>
                <Typography
                    variant="subtitle1"
                    color="text.secondary"
                    sx={{ mb: 2 }}
                >
                    {taskTemplateName}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                    Analyze runtime and memory usage patterns across task
                    instances. Use filters and brush selection to drill down
                    into specific subsets of data.
                </Typography>
            </Box>

            {/* KPI Cards Section */}
            <UsageKPICards
                kpiStats={kpiStats}
                resourceEfficiency={resourceEfficiency}
                selectedDataCount={
                    selectedData.length > 0 ? selectedData.length : undefined
                }
                totalDataCount={filteredScatterData.length}
            />

            {/* Filters Section */}
            <UsageFilters
                availableAttempts={availableAttempts}
                availableStatuses={availableStatuses}
                availableResourceClusters={availableResourceClusters}
                selectedAttempts={selectedAttempts}
                selectedStatuses={selectedStatuses}
                selectedResourceClusters={selectedResourceClusters}
                showResourceZones={showResourceZones}
                onSelectedAttemptsChange={setSelectedAttempts}
                onSelectedStatusesChange={setSelectedStatuses}
                onSelectedResourceClustersChange={setSelectedResourceClusters}
                onShowResourceZonesChange={setShowResourceZones}
                onClearFilters={clearFilters}
                onResetFilters={resetFilters}
            />

            {/* Plot Section */}
            <UsagePlotSection
                isLoading={usageInfo.isLoading}
                filteredScatterData={filteredScatterData}
                taskTemplateName={taskTemplateName}
                medianRequestedRuntime={medianRequestedRuntimeForKPI}
                medianRequestedMemoryGiB={medianRequestedMemoryGiBForKPI}
                showResourceZones={showResourceZones}
                onTaskClick={handleScatterTaskClick}
                onSelected={handleDataSelection}
            />
        </Box>
    );
}
