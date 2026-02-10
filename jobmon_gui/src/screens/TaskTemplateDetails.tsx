import { useState, useMemo } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Grid from '@mui/material/Grid';
import Skeleton from '@mui/material/Skeleton';
import Tab from '@mui/material/Tab';
import Tabs from '@mui/material/Tabs';
import Typography from '@mui/material/Typography';

import {
    AppBreadcrumbs,
    BreadcrumbItem,
} from '@jobmon_gui/components/common/AppBreadcrumbs';
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import TabPanel from '@jobmon_gui/components/common/TabPanel';
import ClusteredErrors from '@jobmon_gui/components/task_template_details/ClusteredErrors';
import TaskTable from '@jobmon_gui/components/task_template_details/TaskTable';
import TaskTemplateHeader from '@jobmon_gui/components/task_template_details/TaskTemplateHeader';
import UsageKPICards from '@jobmon_gui/components/task_template_details/usage/UsageKPICards';
import UsagePlotSection from '@jobmon_gui/components/task_template_details/usage/UsagePlotSection';
import ErrorSummaryCard from '@jobmon_gui/components/task_template_details/usage/ErrorSummaryCard';
import { useTaskTemplateDetails } from '@jobmon_gui/queries/GetTaskTemplateDetails.ts';
import { getWorkflowDetailsQueryFn } from '@jobmon_gui/queries/GetWorkflowDetails.ts';
import { getWorkflowTTStatusQueryFn } from '@jobmon_gui/queries/GetWorkflowTTStatus.ts';
import {
    getWorkflowUsageQueryFn,
    WorkflowUsageQueryKey,
} from '@jobmon_gui/queries/GetWorkflowUsage.ts';
import { getClusteredErrorsFn } from '@jobmon_gui/queries/GetClusteredErrors.ts';
import { getWorkflowFiltersForNavigation } from '@jobmon_gui/utils/workflowFilterPersistence';
import { bytes_to_gib } from '@jobmon_gui/utils/formatters';
import { useUsageFilters } from '@jobmon_gui/hooks/useUsageFilters';
import {
    calculateResourceEfficiency,
    calculateMedian,
    getResourceClusterKey,
} from '@jobmon_gui/components/task_template_details/usage/usageCalculations';
import { components } from '@jobmon_gui/types/apiSchema';
import {
    ScatterDataPoint,
    UsageKPIStats,
    ResourceEfficiencyMetrics,
} from '@jobmon_gui/types/Usage';

type TaskTemplateResourceUsageResponse =
    components['schemas']['TaskTemplateResourceUsageResponse'];

export default function TaskTemplateDetails() {
    const { workflowId, taskTemplateId } = useParams();
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    const location = useLocation();

    const [activeTab, setActiveTab] = useState(0);

    const TaskTemplateDetailsData = useTaskTemplateDetails(
        workflowId,
        taskTemplateId
    );

    // --- Usage API query (lifted from Usage.tsx) ---
    const taskTemplateVersionId =
        TaskTemplateDetailsData.data?.task_template_version_id;
    const taskTemplateName = TaskTemplateDetailsData.data?.task_template_name;

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
        enabled:
            !!taskTemplateVersionId &&
            !!workflowId &&
            !TaskTemplateDetailsData.isLoading,
    });

    const rawTaskNodesFromApi = usageInfo.data?.result_viz || [];

    // --- Clustered Errors query (for badge + Error Summary card) ---
    const clusteredErrorsQuery = useQuery({
        queryKey: [
            'workflow_details',
            'clustered_errors',
            workflowId,
            TaskTemplateDetailsData.data?.task_template_id,
        ],
        queryFn: getClusteredErrorsFn,
        enabled:
            !!TaskTemplateDetailsData.data?.task_template_id &&
            !TaskTemplateDetailsData.isLoading,
    });

    const errorLogs = clusteredErrorsQuery.data?.error_logs || [];
    const errorClusterCount = errorLogs.length;
    const totalFailures = useMemo(
        () =>
            errorLogs.reduce(
                (sum, el) => sum + (el.group_instance_count || 0),
                0
            ),
        [errorLogs]
    );
    const topErrorPreview = useMemo(() => {
        if (errorLogs.length === 0) return undefined;
        const topError = errorLogs[0]?.sample_error || '';
        return topError.length > 120 ? `...${topError.slice(-120)}` : topError;
    }, [errorLogs]);

    // --- Usage filters ---
    const {
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
        selectedTaskNames,
        availableAttempts,
        availableStatuses,
        availableResourceClusters,
        availableTaskNames,
        setSelectedAttempts,
        setSelectedStatuses,
        setSelectedResourceClusters,
        setSelectedTaskNames,
        resetFilters,
    } = useUsageFilters({ rawTaskNodesFromApi });

    // --- Plot interaction state ---
    const [selectedData, setSelectedData] = useState<ScatterDataPoint[]>([]);
    const [showResourceZones, setShowResourceZones] = useState(false);

    // --- Helper: filtered requested resource values ---
    const getFilteredRequestedResourceValues = useMemo(() => {
        return (fieldName: 'runtime' | 'memory'): (number | undefined)[] => {
            if (!rawTaskNodesFromApi) return [];
            return rawTaskNodesFromApi
                .filter(d => {
                    const clusterKey = getResourceClusterKey(
                        d.requested_resources
                    );
                    return (
                        selectedAttempts.has(
                            String(d.attempt_number_of_instance || 1)
                        ) &&
                        selectedStatuses.has(
                            String(d.status || 'UNKNOWN').toUpperCase()
                        ) &&
                        (clusterKey === null ||
                            selectedResourceClusters.has(clusterKey)) &&
                        (!d.task_name || selectedTaskNames.has(d.task_name))
                    );
                })
                .map(item => {
                    try {
                        const reqRes = item.requested_resources
                            ? JSON.parse(item.requested_resources)
                            : {};
                        const val = Number(reqRes[fieldName]);
                        return !isNaN(val) ? val : undefined;
                    } catch {
                        return undefined;
                    }
                });
        };
    }, [
        rawTaskNodesFromApi,
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
        selectedTaskNames,
    ]);

    // --- Filtered scatter data ---
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
                        selectedResourceClusters.has(clusterKey)) &&
                    (!d.task_name || selectedTaskNames.has(d.task_name))
                );
            })
            .map((item): ScatterDataPoint | null => {
                const runtime = typeof item.r === 'number' ? item.r : null;
                const memoryBytes = typeof item.m === 'number' ? item.m : null;
                const memoryGiB =
                    memoryBytes !== null ? bytes_to_gib(memoryBytes) : null;

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
                    // Skip invalid JSON
                }

                if (
                    runtime !== null &&
                    runtime > 0 &&
                    memoryGiB !== null &&
                    memoryGiB > 0
                ) {
                    return {
                        task_id: item.task_id,
                        task_name: item.task_name,
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
            .filter((item): item is ScatterDataPoint => item !== null);
    }, [
        rawTaskNodesFromApi,
        selectedAttempts,
        selectedStatuses,
        selectedResourceClusters,
        selectedTaskNames,
    ]);

    // --- KPI computation ---
    const dataForKPICalculations = useMemo(() => {
        return selectedData.length > 0 ? selectedData : filteredScatterData;
    }, [selectedData, filteredScatterData]);

    const handleDataSelection = (selectedPoints: ScatterDataPoint[]) => {
        setSelectedData(selectedPoints);
    };

    const filteredRequestedRuntimes = useMemo(() => {
        return getFilteredRequestedResourceValues('runtime');
    }, [getFilteredRequestedResourceValues]);

    const filteredRequestedMemoriesGiB = useMemo(() => {
        return getFilteredRequestedResourceValues('memory');
    }, [getFilteredRequestedResourceValues]);

    const medianRequestedRuntime = useMemo(
        () => calculateMedian(filteredRequestedRuntimes),
        [filteredRequestedRuntimes]
    );
    const medianRequestedMemoryGiB = useMemo(
        () => calculateMedian(filteredRequestedMemoriesGiB),
        [filteredRequestedMemoriesGiB]
    );

    const medianRequestedRuntimeForKPI = useMemo(() => {
        if (selectedData.length > 0) {
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
            const selectedTaskIds = new Set(selectedData.map(d => d.task_id));
            const selectedRequestedMemories = rawTaskNodesFromApi
                .filter(item => selectedTaskIds.has(item.task_id))
                .map(item => {
                    try {
                        const reqRes = item.requested_resources
                            ? JSON.parse(item.requested_resources)
                            : {};
                        const val = Number(reqRes.memory);
                        return !isNaN(val) ? val : undefined;
                    } catch {
                        return undefined;
                    }
                });
            return calculateMedian(selectedRequestedMemories);
        }
        return medianRequestedMemoryGiB;
    }, [selectedData, rawTaskNodesFromApi, medianRequestedMemoryGiB]);

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

    // --- CSV download ---
    const parseRequestedResources = (
        requestedResourcesJson: string | null | undefined
    ): { runtime?: number; memory?: number } => {
        if (!requestedResourcesJson) return {};
        try {
            const parsed = JSON.parse(requestedResourcesJson);
            const runtime = Number(parsed.runtime);
            const memory = Number(parsed.memory);
            return {
                runtime: !isNaN(runtime) && runtime > 0 ? runtime : undefined,
                memory: !isNaN(memory) && memory > 0 ? memory : undefined,
            };
        } catch {
            return {};
        }
    };

    const formatDateForCSV = (date: string | null | undefined): string => {
        if (!date) return '';
        try {
            return new Date(date).toISOString();
        } catch {
            return String(date);
        }
    };

    const downloadCSV = () => {
        if (!rawTaskNodesFromApi || rawTaskNodesFromApi.length === 0) {
            return;
        }

        const csvColumns = [
            'task_id',
            'task_name',
            'status',
            'task_status_date',
            'task_command',
            'task_num_attempts',
            'task_max_attempts',
            'runtime_seconds',
            'memory_gib',
            'memory_bytes',
            'attempt_number',
            'requested_runtime_seconds',
            'requested_memory_gib',
            'node_id',
            'requested_resources_json',
        ] as const;

        const csvData = rawTaskNodesFromApi.map(item => {
            const runtime = typeof item.r === 'number' ? item.r : null;
            const memoryBytes = typeof item.m === 'number' ? item.m : null;
            const memoryGiB =
                memoryBytes !== null ? bytes_to_gib(memoryBytes) : null;
            const requestedResources = parseRequestedResources(
                item.requested_resources
            );

            return {
                task_id: item.task_id,
                task_name: item.task_name || '',
                status: item.status || 'UNKNOWN',
                task_status_date: formatDateForCSV(item.task_status_date),
                task_command: item.task_command || '',
                task_num_attempts: item.task_num_attempts ?? null,
                task_max_attempts: item.task_max_attempts ?? null,
                runtime_seconds: runtime,
                memory_gib: memoryGiB,
                memory_bytes: memoryBytes,
                attempt_number: item.attempt_number_of_instance || 1,
                requested_runtime_seconds: requestedResources.runtime,
                requested_memory_gib: requestedResources.memory,
                node_id: item.node_id,
                requested_resources_json: item.requested_resources || '',
            };
        });

        const headers = csvColumns;

        const csvContent = [
            headers.join(','),
            ...csvData.map(row =>
                headers
                    .map(header => {
                        const value = row[header as keyof typeof row];
                        if (value === null || value === undefined) {
                            return '';
                        }
                        const stringValue = String(value);
                        if (
                            stringValue.includes(',') ||
                            stringValue.includes('"') ||
                            stringValue.includes('\n')
                        ) {
                            return `"${stringValue.replace(/"/g, '""')}"`;
                        }
                        return stringValue;
                    })
                    .join(',')
            ),
        ].join('\n');

        const blob = new Blob([csvContent], {
            type: 'text/csv;charset=utf-8;',
        });
        const link = document.createElement('a');
        if (link.download !== undefined) {
            const url = URL.createObjectURL(blob);
            link.setAttribute('href', url);
            link.setAttribute('download', `${taskTemplateName}_usage_data.csv`);
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        }
    };

    // --- Resource data map for TaskTable enrichment ---
    const resourceDataByTaskId = useMemo(() => {
        const map = new Map<
            number,
            {
                runtime: number | null;
                memory: number | null;
                attempt: number;
            }
        >();
        for (const item of rawTaskNodesFromApi) {
            const existing = map.get(item.task_id);
            const attemptNum = item.attempt_number_of_instance || 1;
            if (!existing || attemptNum > existing.attempt) {
                map.set(item.task_id, {
                    runtime: typeof item.r === 'number' ? item.r : null,
                    memory:
                        typeof item.m === 'number'
                            ? bytes_to_gib(item.m)
                            : null,
                    attempt: attemptNum,
                });
            }
        }
        return map;
    }, [rawTaskNodesFromApi]);

    // --- Scatter task click handler ---
    const handleScatterTaskClick = (clickedTaskId: number | string) => {
        navigate(`/task_details/${clickedTaskId}`);
    };

    // --- Navigation ---
    const handleHomeClick = () => {
        const search = getWorkflowFiltersForNavigation(location.search);
        navigate({
            pathname: '/',
            search: search || '',
        });
    };

    const handleWorkflowMouseEnter = async () => {
        queryClient.prefetchQuery({
            queryKey: ['workflow_details', 'details', workflowId],
            queryFn: getWorkflowDetailsQueryFn,
        });
        queryClient.prefetchQuery({
            queryKey: ['workflow_details', 'tt_status', workflowId],
            queryFn: getWorkflowTTStatusQueryFn,
        });
    };

    const breadcrumbItems: BreadcrumbItem[] = [
        {
            label: 'Home',
            to: '/',
            onClick: handleHomeClick,
        },
        {
            label: `Workflow ID ${workflowId}`,
            to: `/workflow/${workflowId}`,
            onMouseEnter: handleWorkflowMouseEnter,
        },
        {
            label: `Task Template ID ${taskTemplateId}`,
            active: true,
        },
    ];

    if (TaskTemplateDetailsData.isLoading) {
        return <CircularProgress />;
    }
    if (TaskTemplateDetailsData.isError || !TaskTemplateDetailsData.data) {
        return <Typography>Error loading template.</Typography>;
    }

    const usageIsLoading = usageInfo.isLoading;

    return (
        <Box>
            <AppBreadcrumbs items={breadcrumbItems} />

            <Box sx={{ justifyContent: 'start', pt: 1 }}>
                <TaskTemplateHeader
                    taskTemplateId={
                        TaskTemplateDetailsData.data.task_template_id
                    }
                    taskTemplateName={
                        TaskTemplateDetailsData.data.task_template_name
                    }
                />
            </Box>

            {/* Progress Bar */}
            <Box id="tt_progress" className="div-level-2">
                <JobmonProgressBar
                    workflowId={workflowId}
                    ttId={TaskTemplateDetailsData.data.task_template_id}
                    placement="bottom"
                />
            </Box>

            {/* --- SECTION A: Analytics (always visible) --- */}

            {/* KPI Cards Row */}
            {usageIsLoading ? (
                <Box sx={{ px: 1, mb: 1 }}>
                    <Grid container spacing={1}>
                        {[1, 2, 3].map(key => (
                            <Grid item xs={12} sm={4} key={key}>
                                <Skeleton variant="rectangular" height={150} />
                            </Grid>
                        ))}
                    </Grid>
                </Box>
            ) : (
                <Grid container spacing={1} sx={{ px: 1, mb: 0 }}>
                    <Grid item xs={12} md={8}>
                        <UsageKPICards
                            kpiStats={kpiStats}
                            resourceEfficiency={resourceEfficiency}
                            selectedDataCount={
                                selectedData.length > 0
                                    ? selectedData.length
                                    : undefined
                            }
                            totalDataCount={filteredScatterData.length}
                        />
                    </Grid>
                    <Grid item xs={12} md={4}>
                        <ErrorSummaryCard
                            errorClusterCount={errorClusterCount}
                            totalFailures={totalFailures}
                            topErrorPreview={topErrorPreview}
                            isLoading={clusteredErrorsQuery.isLoading}
                            onViewDetails={() => setActiveTab(1)}
                        />
                    </Grid>
                </Grid>
            )}

            {/* Scatter Plot Section (filters inside as popover) */}
            {!usageIsLoading && (
                <UsagePlotSection
                    isLoading={usageInfo.isLoading}
                    filteredScatterData={filteredScatterData}
                    taskTemplateName={taskTemplateName || ''}
                    medianRequestedRuntime={medianRequestedRuntimeForKPI}
                    medianRequestedMemoryGiB={medianRequestedMemoryGiBForKPI}
                    showResourceZones={showResourceZones}
                    onTaskClick={handleScatterTaskClick}
                    onSelected={handleDataSelection}
                    onDownloadCSV={downloadCSV}
                    hasData={
                        rawTaskNodesFromApi && rawTaskNodesFromApi.length > 0
                    }
                    availableAttempts={availableAttempts}
                    availableStatuses={availableStatuses}
                    availableResourceClusters={availableResourceClusters}
                    availableTaskNames={availableTaskNames}
                    selectedAttempts={selectedAttempts}
                    selectedStatuses={selectedStatuses}
                    selectedResourceClusters={selectedResourceClusters}
                    selectedTaskNames={selectedTaskNames}
                    onSelectedAttemptsChange={setSelectedAttempts}
                    onSelectedStatusesChange={setSelectedStatuses}
                    onSelectedResourceClustersChange={
                        setSelectedResourceClusters
                    }
                    onSelectedTaskNamesChange={setSelectedTaskNames}
                    onShowResourceZonesChange={setShowResourceZones}
                    onResetFilters={resetFilters}
                />
            )}

            {/* --- SECTION B: Detail Tables (2 tabs) --- */}
            <Box
                sx={{
                    borderBottom: 1,
                    borderColor: 'divider',
                    mt: 1,
                }}
            >
                <Tabs
                    value={activeTab}
                    onChange={(_event, newValue) => setActiveTab(newValue)}
                    aria-label="Tab selection"
                >
                    <Tab label="Tasks" value={0} />
                    <Tab
                        label={
                            <Badge
                                badgeContent={errorClusterCount}
                                color="error"
                                max={999}
                            >
                                Errors
                            </Badge>
                        }
                        value={1}
                    />
                </Tabs>
            </Box>
            <TabPanel value={activeTab} index={0}>
                <TaskTable
                    taskTemplateName={
                        TaskTemplateDetailsData.data.task_template_name
                    }
                    workflowId={workflowId}
                    resourceDataByTaskId={resourceDataByTaskId}
                />
            </TabPanel>
            <TabPanel value={activeTab} index={1}>
                <ClusteredErrors
                    taskTemplateId={
                        TaskTemplateDetailsData.data.task_template_id
                    }
                    workflowId={workflowId}
                />
            </TabPanel>
        </Box>
    );
}
