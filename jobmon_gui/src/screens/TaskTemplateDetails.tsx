import { useState, useMemo, useEffect, useCallback } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Skeleton from '@mui/material/Skeleton';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import useMediaQuery from '@mui/material/useMediaQuery';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';

import {
    AppBreadcrumbs,
    BreadcrumbItem,
} from '@jobmon_gui/components/common/AppBreadcrumbs';
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import TaskTable from '@jobmon_gui/components/task_template_details/TaskTable';
import UsageKPICards from '@jobmon_gui/components/task_template_details/usage/UsageKPICards';
import UsagePlotSection from '@jobmon_gui/components/task_template_details/usage/UsagePlotSection';
import ErrorClustersCard from '@jobmon_gui/components/task_template_details/usage/ErrorClustersCard';
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
import { TaskInstanceRow } from '@jobmon_gui/types/TaskTable';

dayjs.extend(utc);

type TaskTemplateResourceUsageResponse =
    components['schemas']['TaskTemplateResourceUsageResponse'];

export default function TaskTemplateDetails() {
    const { workflowId, taskTemplateId } = useParams();
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    const location = useLocation();
    const theme = useTheme();
    const isMdUp = useMediaQuery(theme.breakpoints.up('md'));

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

    // --- Usage filters ---
    const {
        selectedResourceClusters,
        availableResourceClusters,
        setSelectedResourceClusters,
        resetFilters,
    } = useUsageFilters({ rawTaskNodesFromApi });

    // --- Plot interaction state ---
    const [selectedData, setSelectedData] = useState<ScatterDataPoint[]>([]);
    const [showResourceZones, setShowResourceZones] = useState(false);
    const [tableFilteredInstanceIds, setTableFilteredInstanceIds] =
        useState<Set<number> | null>(null);

    // --- Helper: resource cluster filter predicate ---
    const passesResourceClusterFilter = useCallback(
        (d: { requested_resources?: string | null }): boolean => {
            const clusterKey = getResourceClusterKey(d.requested_resources);
            return (
                clusterKey === null || selectedResourceClusters.has(clusterKey)
            );
        },
        [selectedResourceClusters]
    );

    // --- Filtered instance data (shared by table + scatter) ---
    const filteredInstanceData: TaskInstanceRow[] = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(passesResourceClusterFilter)
            .map(item => ({
                task_id: item.task_id,
                task_instance_id: item.task_instance_id ?? 0,
                task_name: item.task_name || '',
                attempt_number: item.attempt_number_of_instance || 1,
                instance_status: String(item.status || 'UNKNOWN').toUpperCase(),
                task_command: item.task_command || '',
                task_num_attempts: item.task_num_attempts ?? 0,
                task_max_attempts: item.task_max_attempts ?? 0,
                task_status_date: item.task_status_date
                    ? dayjs.utc(item.task_status_date)
                    : dayjs(),
                runtime_seconds: typeof item.r === 'number' ? item.r : null,
                memory_gib:
                    typeof item.m === 'number' ? bytes_to_gib(item.m) : null,
            }));
    }, [rawTaskNodesFromApi, passesResourceClusterFilter]);

    // --- Helper: filtered requested resource values ---
    const getFilteredRequestedResourceValues = useMemo(() => {
        return (fieldName: 'runtime' | 'memory'): (number | undefined)[] => {
            if (!rawTaskNodesFromApi) return [];
            return rawTaskNodesFromApi
                .filter(passesResourceClusterFilter)
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
    }, [rawTaskNodesFromApi, passesResourceClusterFilter]);

    // --- Filtered scatter data (derived from filteredInstanceData) ---
    // We need requested resource values from raw API data, so we
    // build a lookup keyed by task_id + attempt for merging.
    const requestedResourcesById = useMemo(() => {
        const map = new Map<
            number,
            {
                requestedRuntime?: number;
                requestedMemory?: number;
            }
        >();
        for (const item of rawTaskNodesFromApi) {
            const id = item.task_instance_id;
            if (id == null) continue;
            try {
                const reqRes = item.requested_resources
                    ? JSON.parse(item.requested_resources)
                    : {};
                const reqRuntimeVal = Number(reqRes.runtime);
                const reqMemoryVal = Number(reqRes.memory);
                map.set(id, {
                    requestedRuntime:
                        !isNaN(reqRuntimeVal) && reqRuntimeVal > 0
                            ? reqRuntimeVal
                            : undefined,
                    requestedMemory:
                        !isNaN(reqMemoryVal) && reqMemoryVal > 0
                            ? reqMemoryVal
                            : undefined,
                });
            } catch {
                map.set(id, {});
            }
        }
        return map;
    }, [rawTaskNodesFromApi]);

    const filteredScatterData = useMemo(() => {
        return filteredInstanceData
            .filter(
                d =>
                    d.runtime_seconds !== null &&
                    d.runtime_seconds > 0 &&
                    d.memory_gib !== null &&
                    d.memory_gib > 0
            )
            .map((d): ScatterDataPoint => {
                const req = requestedResourcesById.get(d.task_instance_id);
                return {
                    task_id: d.task_id,
                    task_instance_id: d.task_instance_id,
                    task_name: d.task_name,
                    runtime: d.runtime_seconds!,
                    memory: d.memory_gib!,
                    status: d.instance_status,
                    attempt_num: d.attempt_number,
                    requestedRuntime: req?.requestedRuntime,
                    requestedMemory: req?.requestedMemory,
                };
            });
    }, [filteredInstanceData, requestedResourcesById]);

    // --- Effective scatter data (narrowed by table column filters) ---
    const effectiveScatterData = useMemo(() => {
        if (!tableFilteredInstanceIds) return filteredScatterData;
        return filteredScatterData.filter(d =>
            tableFilteredInstanceIds.has(d.task_instance_id)
        );
    }, [filteredScatterData, tableFilteredInstanceIds]);

    // --- KPI computation ---
    const dataForKPICalculations = useMemo(() => {
        if (selectedData.length > 0) {
            if (tableFilteredInstanceIds) {
                return selectedData.filter(d =>
                    tableFilteredInstanceIds.has(d.task_instance_id)
                );
            }
            return selectedData;
        }
        return effectiveScatterData;
    }, [selectedData, effectiveScatterData, tableFilteredInstanceIds]);

    const handleDataSelection = (selectedPoints: ScatterDataPoint[]) => {
        setSelectedData(selectedPoints);
    };

    const handleErrorFilterByInstanceIds = useCallback(
        (instanceIds: number[]) => {
            const instanceIdSet = new Set(instanceIds);
            const currentIds = new Set(
                selectedData.map(d => d.task_instance_id)
            );
            const isSame =
                selectedData.length > 0 &&
                instanceIds.every(id => currentIds.has(id)) &&
                currentIds.size === instanceIdSet.size;
            if (isSame) {
                setSelectedData([]);
                return;
            }
            setSelectedData(
                effectiveScatterData.filter(d =>
                    instanceIdSet.has(d.task_instance_id)
                )
            );
        },
        [selectedData, effectiveScatterData]
    );

    const handleClearSelection = useCallback(() => {
        setSelectedData([]);
    }, []);

    const handleTableFilteredInstanceIdsChange = useCallback(
        (ids: Set<number> | null) => {
            setTableFilteredInstanceIds(ids);
        },
        []
    );

    // Stable Set of selected instance IDs for scatter highlighting
    const selectedInstanceIds = useMemo(() => {
        if (selectedData.length === 0) return undefined;
        return new Set(selectedData.map(d => d.task_instance_id));
    }, [selectedData]);

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
            const selIds = new Set(selectedData.map(d => d.task_instance_id));
            const selectedRequestedRuntimes = rawTaskNodesFromApi
                .filter(
                    item =>
                        item.task_instance_id != null &&
                        selIds.has(item.task_instance_id)
                )
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
            const selIds = new Set(selectedData.map(d => d.task_instance_id));
            const selectedRequestedMemories = rawTaskNodesFromApi
                .filter(
                    item =>
                        item.task_instance_id != null &&
                        selIds.has(item.task_instance_id)
                )
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

    // --- Cross-component filtering ---
    // Error logs filtered by data filters only (not scatter/error selection)
    const dataFilterInstanceIds = useMemo(() => {
        return new Set(filteredScatterData.map(d => d.task_instance_id));
    }, [filteredScatterData]);

    const isDataFiltered = useMemo(() => {
        return selectedResourceClusters.size < availableResourceClusters.length;
    }, [selectedResourceClusters, availableResourceClusters]);

    const errorLogsForCard = useMemo(() => {
        if (!isDataFiltered && !tableFilteredInstanceIds) return errorLogs;
        let effectiveIds = dataFilterInstanceIds;
        if (tableFilteredInstanceIds) {
            effectiveIds = new Set(
                [...effectiveIds].filter(id => tableFilteredInstanceIds.has(id))
            );
        }
        return errorLogs
            .map(el => {
                const matchingInstanceIds = el.task_instance_ids.filter(id =>
                    effectiveIds.has(id)
                );
                if (matchingInstanceIds.length === 0) return null;
                return {
                    ...el,
                    task_instance_ids: matchingInstanceIds,
                    group_instance_count: matchingInstanceIds.length,
                };
            })
            .filter((el): el is NonNullable<typeof el> => el !== null);
    }, [
        errorLogs,
        dataFilterInstanceIds,
        isDataFiltered,
        tableFilteredInstanceIds,
    ]);

    // Table data: pre-filtered by scatter selection
    const tableData = useMemo(() => {
        if (selectedData.length === 0) return filteredInstanceData;
        const selectedIds = new Set(selectedData.map(d => d.task_instance_id));
        return filteredInstanceData.filter(d =>
            selectedIds.has(d.task_instance_id)
        );
    }, [filteredInstanceData, selectedData]);

    // Clear brush selection and table filter feedback when filters change
    useEffect(() => {
        setSelectedData([]);
        setTableFilteredInstanceIds(null);
    }, [selectedResourceClusters]);

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

            {/* Header + Progress Bar */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 2,
                    px: 1,
                    pt: 1,
                    pb: 0.5,
                }}
            >
                <Typography
                    variant="h6"
                    component="h1"
                    fontWeight="bold"
                    sx={{ whiteSpace: 'nowrap' }}
                >
                    {TaskTemplateDetailsData.data.task_template_id}
                    {TaskTemplateDetailsData.data.task_template_name
                        ? ` - ${TaskTemplateDetailsData.data.task_template_name}`
                        : ''}
                </Typography>
                <Box id="tt_progress" sx={{ flex: 1, minWidth: 0 }}>
                    <JobmonProgressBar
                        workflowId={workflowId}
                        ttId={TaskTemplateDetailsData.data.task_template_id}
                        placement="bottom"
                    />
                </Box>
            </Box>

            {/* --- SECTION A: Analytics (sidebar + main) --- */}

            {usageIsLoading ? (
                <Box
                    sx={{
                        display: 'flex',
                        flexDirection: { xs: 'column', md: 'row' },
                        px: 1,
                        gap: 1,
                    }}
                >
                    <Box
                        sx={{
                            flex: {
                                xs: '1 1 auto',
                                md: '0 0 280px',
                            },
                            maxWidth: { md: 280 },
                            minWidth: 0,
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 1,
                        }}
                    >
                        <Skeleton variant="rectangular" height={150} />
                        <Skeleton variant="rectangular" height={150} />
                        <Skeleton variant="rectangular" height={200} />
                    </Box>
                    <Box sx={{ flex: '1 1 0', minWidth: 0 }}>
                        <Skeleton variant="rectangular" height={500} />
                    </Box>
                </Box>
            ) : (
                <Box
                    sx={{
                        display: 'flex',
                        flexDirection: {
                            xs: 'column',
                            md: 'row',
                        },
                        px: 1,
                        gap: 1,
                    }}
                >
                    {/* Sidebar */}
                    <Box
                        sx={{
                            flex: {
                                xs: '1 1 auto',
                                md: '0 0 280px',
                            },
                            maxWidth: { md: 280 },
                            minWidth: 0,
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 1,
                        }}
                    >
                        <UsageKPICards
                            layout={isMdUp ? 'vertical' : 'horizontal'}
                            kpiStats={kpiStats}
                            resourceEfficiency={resourceEfficiency}
                            selectedDataCount={
                                selectedData.length > 0
                                    ? selectedData.length
                                    : undefined
                            }
                            totalDataCount={effectiveScatterData.length}
                        />
                        <ErrorClustersCard
                            errorLogs={errorLogsForCard}
                            isLoading={clusteredErrorsQuery.isLoading}
                            workflowId={workflowId}
                            taskTemplateId={
                                TaskTemplateDetailsData.data.task_template_id
                            }
                            selectedInstanceIds={selectedInstanceIds}
                            onFilterByInstanceIds={
                                handleErrorFilterByInstanceIds
                            }
                            maxListHeight={isMdUp ? 400 : 180}
                        />
                    </Box>

                    {/* Main content */}
                    <Box
                        sx={{
                            flex: '1 1 0',
                            minWidth: 0,
                            display: 'flex',
                            flexDirection: 'column',
                        }}
                    >
                        <UsagePlotSection
                            isLoading={usageInfo.isLoading}
                            filteredScatterData={effectiveScatterData}
                            taskTemplateName={taskTemplateName || ''}
                            medianRequestedRuntime={
                                medianRequestedRuntimeForKPI
                            }
                            medianRequestedMemoryGiB={
                                medianRequestedMemoryGiBForKPI
                            }
                            showResourceZones={showResourceZones}
                            selectedInstanceIds={selectedInstanceIds}
                            onTaskClick={handleScatterTaskClick}
                            onSelected={handleDataSelection}
                            onShowResourceZonesChange={setShowResourceZones}
                            onDownloadCSV={downloadCSV}
                            hasData={
                                rawTaskNodesFromApi &&
                                rawTaskNodesFromApi.length > 0
                            }
                            availableResourceClusters={
                                availableResourceClusters
                            }
                            selectedResourceClusters={selectedResourceClusters}
                            onSelectedResourceClustersChange={
                                setSelectedResourceClusters
                            }
                            onResetFilters={resetFilters}
                            hasActiveSelection={selectedData.length > 0}
                            onClearSelection={handleClearSelection}
                        />
                    </Box>
                </Box>
            )}

            {/* Task Table (full width, below both columns) */}
            <Box sx={{ mt: 1 }}>
                <TaskTable
                    data={tableData}
                    isLoading={usageIsLoading}
                    taskTemplateName={
                        TaskTemplateDetailsData.data.task_template_name
                    }
                    workflowId={workflowId}
                    onFilteredInstanceIdsChange={
                        handleTableFilteredInstanceIdsChange
                    }
                />
            </Box>
        </Box>
    );
}
