import { useState, useMemo, useEffect, useCallback } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import axios from 'axios';
import {
    InfiniteData,
    QueryFunctionContext,
    useInfiniteQuery,
    useQuery,
    useQueryClient,
} from '@tanstack/react-query';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Collapse from '@mui/material/Collapse';
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
import ResourceSummaryBar from '@jobmon_gui/components/task_template_details/usage/ResourceSummaryBar';
import ErrorClustersCard from '@jobmon_gui/components/task_template_details/usage/ErrorClustersCard';
import { useTaskTemplateDetails } from '@jobmon_gui/queries/GetTaskTemplateDetails.ts';
import { getWorkflowDetailsQueryFn } from '@jobmon_gui/queries/GetWorkflowDetails.ts';
import { getWorkflowTTStatusQueryFn } from '@jobmon_gui/queries/GetWorkflowTTStatus.ts';
import { USAGE_PAGE_SIZE } from '@jobmon_gui/queries/GetWorkflowUsage.ts';
import { usage_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import {
    getTaskTemplateAggregatesQueryFn,
    TaskTemplateAggregatesQueryKey,
} from '@jobmon_gui/queries/GetTaskTemplateAggregates.ts';
import {
    getClusteredErrorsFn,
    clusteredErrorsKey,
} from '@jobmon_gui/queries/GetClusteredErrors.ts';
import {
    getFatalErrorBreakdownFn,
    fatalBreakdownKey,
} from '@jobmon_gui/queries/GetFatalErrorBreakdown';
import { getWorkflowFiltersForNavigation } from '@jobmon_gui/utils/workflowFilterPersistence';
import { bytes_to_gib } from '@jobmon_gui/utils/formatters';
import {
    downloadUsageCSV,
    parseResourceJson,
} from '@jobmon_gui/utils/csvExport';
import { useUsageFilters } from '@jobmon_gui/hooks/useUsageFilters';
import {
    calculateResourceEfficiency,
    calculateMedian,
    createResourceClusterKey,
    createResourceClusterLabel,
    getResourceClusterKey,
    ResourceCluster,
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
type TaskResourceVizItem = components['schemas']['TaskResourceVizItem'];
type TaskTemplateResourceAggregatesResponse =
    components['schemas']['TaskTemplateResourceAggregatesResponse'];

export default function TaskTemplateDetails() {
    const { workflowId, taskTemplateId } = useParams();
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    const location = useLocation();
    const theme = useTheme();
    const isMdUp = useMediaQuery(theme.breakpoints.up('md'));

    const searchParams = new URLSearchParams(location.search);
    const ttvFromUrl = searchParams.get('ttv');

    const TaskTemplateDetailsData = useTaskTemplateDetails(
        workflowId,
        taskTemplateId,
        ttvFromUrl ? Number(ttvFromUrl) : null
    );

    // --- Usage API query (lifted from Usage.tsx) ---
    const taskTemplateVersionId =
        TaskTemplateDetailsData.data?.task_template_version_id;
    const taskTemplateName = TaskTemplateDetailsData.data?.task_template_name;

    // Page-drain concurrency. Three parallel useInfiniteQuery "streams"
    // each drain a strided subset of pages (stream 0: 0, 3, 6, 9...;
    // stream 1: 1, 4, 7, 10...; stream 2: 2, 5, 8, 11...). Each stream
    // drains sequentially via its own ``fetchNextPage``, so the three
    // together run 3 HTTP requests concurrently — chosen to stay under
    // the 6-per-host browser cap and leave headroom for prod MySQL
    // under load.
    const USAGE_STREAMS = 3;
    type StreamKey = readonly [
        string,
        string,
        string | number | undefined,
        string | number | undefined,
        number,
    ];
    const makeStreamKey = (streamIndex: number): StreamKey =>
        [
            'workflow_details',
            'usage_paged',
            taskTemplateVersionId,
            workflowId,
            streamIndex,
        ] as const;
    const usageStreamFn = async (
        context: QueryFunctionContext<StreamKey, number>
    ): Promise<TaskTemplateResourceUsageResponse | undefined> => {
        const { queryKey, pageParam = 0 } = context;
        if (queryKey[2] === undefined || queryKey[3] === undefined) {
            return undefined;
        }
        const response = await axios.post<TaskTemplateResourceUsageResponse>(
            usage_url,
            {
                task_template_version_id: queryKey[2],
                workflows: [queryKey[3]],
                viz: true,
                page: pageParam,
                page_size: USAGE_PAGE_SIZE,
            },
            jobmonAxiosConfig
        );
        return response.data;
    };
    const usageStreamOptions = (streamIndex: number) => ({
        queryKey: makeStreamKey(streamIndex),
        queryFn: usageStreamFn,
        initialPageParam: streamIndex,
        getNextPageParam: (
            lastPage: TaskTemplateResourceUsageResponse | undefined,
            _allPages: (TaskTemplateResourceUsageResponse | undefined)[],
            lastPageParam: number
        ) => {
            const total = lastPage?.total_count;
            if (total == null) return undefined;
            const totalPages = Math.ceil(total / USAGE_PAGE_SIZE);
            const next = lastPageParam + USAGE_STREAMS;
            return next < totalPages ? next : undefined;
        },
        staleTime: 5000,
        enabled:
            !!taskTemplateVersionId &&
            !!workflowId &&
            !TaskTemplateDetailsData.isLoading,
    });
    const usageStream0 = useInfiniteQuery<
        TaskTemplateResourceUsageResponse | undefined,
        Error,
        InfiniteData<TaskTemplateResourceUsageResponse | undefined>,
        StreamKey,
        number
    >(usageStreamOptions(0));
    const usageStream1 = useInfiniteQuery<
        TaskTemplateResourceUsageResponse | undefined,
        Error,
        InfiniteData<TaskTemplateResourceUsageResponse | undefined>,
        StreamKey,
        number
    >(usageStreamOptions(1));
    const usageStream2 = useInfiniteQuery<
        TaskTemplateResourceUsageResponse | undefined,
        Error,
        InfiniteData<TaskTemplateResourceUsageResponse | undefined>,
        StreamKey,
        number
    >(usageStreamOptions(2));

    // Eagerly drain each stream in the background.
    useEffect(() => {
        if (usageStream0.hasNextPage && !usageStream0.isFetchingNextPage) {
            usageStream0.fetchNextPage();
        }
    }, [
        usageStream0.hasNextPage,
        usageStream0.isFetchingNextPage,
        usageStream0.data?.pages.length,
        usageStream0.fetchNextPage,
    ]);
    useEffect(() => {
        if (usageStream1.hasNextPage && !usageStream1.isFetchingNextPage) {
            usageStream1.fetchNextPage();
        }
    }, [
        usageStream1.hasNextPage,
        usageStream1.isFetchingNextPage,
        usageStream1.data?.pages.length,
        usageStream1.fetchNextPage,
    ]);
    useEffect(() => {
        if (usageStream2.hasNextPage && !usageStream2.isFetchingNextPage) {
            usageStream2.fetchNextPage();
        }
    }, [
        usageStream2.hasNextPage,
        usageStream2.isFetchingNextPage,
        usageStream2.data?.pages.length,
        usageStream2.fetchNextPage,
    ]);

    const rawTaskNodesFromApi = useMemo(() => {
        const all: TaskResourceVizItem[] = [];
        for (const stream of [usageStream0, usageStream1, usageStream2]) {
            stream.data?.pages.forEach(p => {
                if (p?.result_viz) all.push(...p.result_viz);
            });
        }
        return all;
    }, [
        usageStream0.data?.pages,
        usageStream1.data?.pages,
        usageStream2.data?.pages,
    ]);

    // Page-wide filters — these feed both the aggregates query scope and
    // client-side filtering of the streaming viz rows. Declared up here so
    // the aggregates query key can depend on them.
    const [showLatestOnly, setShowLatestOnly] = useState(true);
    const [selectedWorkflowRunId, setSelectedWorkflowRunId] = useState<
        number | null
    >(null);

    // Aggregates query: fast server-computed KPIs that render immediately
    // while the paginated viz query streams in. Key includes
    // `showLatestOnly` so toggling the filter refetches a matching view.
    const aggregatesQueryKey: TaskTemplateAggregatesQueryKey = [
        'workflow_details',
        'usage_aggregates',
        taskTemplateVersionId,
        workflowId,
        showLatestOnly,
    ];
    const aggregatesQuery = useQuery<
        TaskTemplateResourceAggregatesResponse | undefined,
        Error,
        TaskTemplateResourceAggregatesResponse | undefined,
        TaskTemplateAggregatesQueryKey
    >({
        queryKey: aggregatesQueryKey,
        queryFn: getTaskTemplateAggregatesQueryFn,
        staleTime: 5000,
        enabled:
            !!taskTemplateVersionId &&
            !!workflowId &&
            !TaskTemplateDetailsData.isLoading,
    });

    // Adapt server clusters to the frontend ResourceCluster shape. The
    // server ships only numbers (runtime, memory, task_count); we derive
    // the string cluster id via the same ``createResourceClusterKey`` the
    // scatter-row predicate uses, so the two sides never rely on a
    // cross-language string contract to agree.
    const serverResourceClusters: ResourceCluster[] | undefined =
        useMemo(() => {
            const clusters = aggregatesQuery.data?.resource_clusters;
            if (!clusters) return undefined;
            return clusters.map(c => ({
                id: createResourceClusterKey(c.runtime, c.memory),
                runtime: c.runtime,
                memory: c.memory,
                taskCount: c.task_count,
                label: createResourceClusterLabel(
                    c.runtime,
                    c.memory,
                    c.task_count
                ),
            }));
        }, [aggregatesQuery.data?.resource_clusters]);

    // --- Clustered Errors query (for badge + Error Summary card) ---
    const clusteredErrorsQuery = useQuery({
        queryKey: clusteredErrorsKey({
            workflowId,
            taskTemplateId: TaskTemplateDetailsData.data?.task_template_id ?? 0,
            taskTemplateVersionId,
        }),
        queryFn: getClusteredErrorsFn,
        enabled:
            !!TaskTemplateDetailsData.data?.task_template_id &&
            !!taskTemplateVersionId &&
            !TaskTemplateDetailsData.isLoading,
    });

    const errorLogs = useMemo(
        () => clusteredErrorsQuery.data?.error_logs ?? [],
        [clusteredErrorsQuery.data?.error_logs]
    );

    // --- Fatal error breakdown (for resource error visibility) ---
    const breakdownQuery = useQuery({
        queryKey: fatalBreakdownKey({
            workflowId,
            taskTemplateVersionId,
        }),
        queryFn: getFatalErrorBreakdownFn,
        enabled: !!taskTemplateVersionId && !!workflowId,
        staleTime: 120000,
    });

    // --- Usage filters ---
    const {
        selectedResourceClusters,
        availableResourceClusters,
        setSelectedResourceClusters,
        resetFilters,
    } = useUsageFilters({ rawTaskNodesFromApi, serverResourceClusters });

    // --- Plot interaction state ---
    const [selectedData, setSelectedData] = useState<ScatterDataPoint[]>([]);
    const [showResourceZones, setShowResourceZones] = useState(false);
    const [tableFilteredInstanceIds, setTableFilteredInstanceIds] =
        useState<Set<number> | null>(null);

    // --- Resource section collapse (persisted to localStorage) ---
    const [resourceSectionExpanded, setResourceSectionExpanded] = useState(
        () => localStorage.getItem('jobmon_resourceSectionExpanded') === 'true'
    );
    useEffect(() => {
        localStorage.setItem(
            'jobmon_resourceSectionExpanded',
            String(resourceSectionExpanded)
        );
    }, [resourceSectionExpanded]);

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
                workflow_run_id: item.workflow_run_id ?? null,
            }));
    }, [rawTaskNodesFromApi, passesResourceClusterFilter]);

    // --- Available workflow run IDs (sorted descending) ---
    const availableWorkflowRunIds = useMemo(() => {
        const ids = new Set<number>();
        for (const row of filteredInstanceData) {
            if (row.workflow_run_id != null) {
                ids.add(row.workflow_run_id);
            }
        }
        return Array.from(ids).sort((a, b) => b - a);
    }, [filteredInstanceData]);

    // --- Latest attempt + workflow run filters ---
    const latestFilteredData = useMemo(() => {
        let data = filteredInstanceData;
        if (selectedWorkflowRunId != null) {
            data = data.filter(
                r => r.workflow_run_id === selectedWorkflowRunId
            );
        }
        if (!showLatestOnly) return data;
        const latest = new Map<number, TaskInstanceRow>();
        for (const row of data) {
            const existing = latest.get(row.task_id);
            if (!existing || row.attempt_number > existing.attempt_number) {
                latest.set(row.task_id, row);
            }
        }
        return Array.from(latest.values());
    }, [filteredInstanceData, showLatestOnly, selectedWorkflowRunId]);

    // --- Helper: filtered requested resource values ---
    const getFilteredRequestedResourceValues = useMemo(() => {
        return (fieldName: 'runtime' | 'memory'): (number | undefined)[] => {
            if (!rawTaskNodesFromApi) return [];
            return rawTaskNodesFromApi
                .filter(passesResourceClusterFilter)
                .map(
                    item =>
                        parseResourceJson(item.requested_resources)[fieldName]
                );
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
            map.set(id, {
                requestedRuntime: parseResourceJson(item.requested_resources)
                    .runtime,
                requestedMemory: parseResourceJson(item.requested_resources)
                    .memory,
            });
        }
        return map;
    }, [rawTaskNodesFromApi]);

    const filteredScatterData = useMemo(() => {
        return latestFilteredData
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
    }, [latestFilteredData, requestedResourcesById]);

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
            // Only cluster instances that appear in the scatter can be selected; compare against that subset so toggle-off works when some cluster instances lack usage data.
            const scatterIdSet = new Set(
                effectiveScatterData.map(d => d.task_instance_id)
            );
            const clusterScatterIds = instanceIds.filter(id =>
                scatterIdSet.has(id)
            );
            const isSame =
                selectedData.length > 0 &&
                currentIds.size === clusterScatterIds.length &&
                clusterScatterIds.every(id => currentIds.has(id));
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

    // Set of instance IDs that appear in the scatter (have valid runtime/memory), for error-cluster isActive and toggle-off.
    const scatterInstanceIds = useMemo(
        () => new Set(effectiveScatterData.map(d => d.task_instance_id)),
        [effectiveScatterData]
    );

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

    // Median requested resource for KPI: narrows to selected data when
    // a scatter selection is active, otherwise uses the full filtered set.
    const medianRequestedForKPI = useMemo(() => {
        if (!selectedInstanceIds) {
            return {
                runtime: medianRequestedRuntime,
                memory: medianRequestedMemoryGiB,
            };
        }
        const selected = rawTaskNodesFromApi.filter(
            item =>
                item.task_instance_id != null &&
                selectedInstanceIds.has(item.task_instance_id)
        );
        return {
            runtime: calculateMedian(
                selected.map(
                    item => parseResourceJson(item.requested_resources).runtime
                )
            ),
            memory: calculateMedian(
                selected.map(
                    item => parseResourceJson(item.requested_resources).memory
                )
            ),
        };
    }, [
        selectedInstanceIds,
        rawTaskNodesFromApi,
        medianRequestedRuntime,
        medianRequestedMemoryGiB,
    ]);

    const medianRequestedRuntimeForKPI = medianRequestedForKPI.runtime;
    const medianRequestedMemoryGiBForKPI = medianRequestedForKPI.memory;

    const kpiRuntimes = useMemo(
        () => dataForKPICalculations.map(d => d.runtime),
        [dataForKPICalculations]
    );
    const kpiMemoriesGiB = useMemo(
        () => dataForKPICalculations.map(d => d.memory),
        [dataForKPICalculations]
    );

    const clientKpiStats: UsageKPIStats = useMemo(
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

    const clientResourceEfficiency: ResourceEfficiencyMetrics = useMemo(() => {
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

    // Adapt the server aggregates response into the frontend KPI/efficiency
    // shapes. Backend ships memory fields in bytes; frontend uses GiB.
    const bytesToGiB = (b: number | null | undefined): number | undefined =>
        b == null ? undefined : b / 1024 ** 3;
    const serverKpiStats: UsageKPIStats | undefined = useMemo(() => {
        const a = aggregatesQuery.data;
        if (!a) return undefined;
        return {
            minRuntime: a.min_runtime ?? undefined,
            maxRuntime: a.max_runtime ?? undefined,
            meanRuntime: a.mean_runtime ?? undefined,
            medianRuntime: a.median_runtime ?? undefined,
            minMemoryGiB: bytesToGiB(a.min_mem),
            maxMemoryGiB: bytesToGiB(a.max_mem),
            meanMemoryGiB: bytesToGiB(a.mean_mem),
            medianMemoryGiB: bytesToGiB(a.median_mem),
            medianRequestedRuntime: a.median_requested_runtime ?? undefined,
            medianRequestedMemoryGiB: a.median_requested_memory ?? undefined,
        };
    }, [aggregatesQuery.data]);
    const serverResourceEfficiency: ResourceEfficiencyMetrics | undefined =
        useMemo(() => {
            const e = aggregatesQuery.data?.efficiency;
            if (!e) return undefined;
            return {
                memoryUtilization: e.memory_utilization,
                runtimeUtilization: e.runtime_utilization,
                overAllocatedMemory: e.over_allocated_memory,
                underAllocatedMemory: e.under_allocated_memory,
                overAllocatedRuntime: e.over_allocated_runtime,
                underAllocatedRuntime: e.under_allocated_runtime,
                p95Memory: e.p95_memory ?? undefined,
                p95Runtime: e.p95_runtime ?? undefined,
                outlierCount: e.outlier_count,
            };
        }, [aggregatesQuery.data?.efficiency]);

    // True when no user-driven filter narrows the page; in that state the
    // server aggregates are an exact match for what the client would
    // compute from the full streaming rowset, so we render them directly
    // for instant KPIs without waiting for pagination to drain.
    const isUnfilteredDefault =
        selectedData.length === 0 &&
        tableFilteredInstanceIds === null &&
        selectedWorkflowRunId === null &&
        selectedResourceClusters.size === availableResourceClusters.length;

    const kpiStats: UsageKPIStats =
        isUnfilteredDefault && serverKpiStats ? serverKpiStats : clientKpiStats;
    const resourceEfficiency: ResourceEfficiencyMetrics =
        isUnfilteredDefault && serverResourceEfficiency
            ? serverResourceEfficiency
            : clientResourceEfficiency;

    // --- CSV download ---
    const downloadCSV = () => {
        downloadUsageCSV(
            rawTaskNodesFromApi,
            `${taskTemplateName}_usage_data.csv`
        );
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
        if (
            !isDataFiltered &&
            !tableFilteredInstanceIds &&
            !showLatestOnly &&
            selectedWorkflowRunId == null
        )
            return errorLogs;
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
        showLatestOnly,
        selectedWorkflowRunId,
    ]);

    // Table data: pre-filtered by scatter selection
    const tableData = useMemo(() => {
        if (selectedData.length === 0) return latestFilteredData;
        const selectedIds = new Set(selectedData.map(d => d.task_instance_id));
        return latestFilteredData.filter(d =>
            selectedIds.has(d.task_instance_id)
        );
    }, [latestFilteredData, selectedData]);

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

    const usageIsLoading =
        usageStream0.isLoading ||
        usageStream1.isLoading ||
        usageStream2.isLoading;

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

            {/* --- SECTION A: Errors (full width, prominent) --- */}
            <Box sx={{ px: 1, mt: 1 }}>
                <ErrorClustersCard
                    layout="fullwidth"
                    errorLogs={errorLogsForCard}
                    isLoading={clusteredErrorsQuery.isLoading}
                    workflowId={workflowId}
                    taskTemplateId={
                        TaskTemplateDetailsData.data.task_template_id
                    }
                    selectedInstanceIds={selectedInstanceIds}
                    scatterInstanceIds={scatterInstanceIds}
                    onFilterByInstanceIds={handleErrorFilterByInstanceIds}
                    resourceErrorBreakdown={breakdownQuery.data}
                />
            </Box>

            {/* --- SECTION B: Resource Profiling (summary bar + collapsible detail) --- */}
            <Box sx={{ px: 1, mt: 1 }}>
                {usageIsLoading ? (
                    <Skeleton
                        variant="rectangular"
                        height={48}
                        sx={{ borderRadius: 2 }}
                    />
                ) : (
                    <>
                        <ResourceSummaryBar
                            resourceEfficiency={resourceEfficiency}
                            expanded={resourceSectionExpanded}
                            onToggleExpanded={() =>
                                setResourceSectionExpanded(prev => !prev)
                            }
                            selectedDataCount={selectedData.length}
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
                            hasData={
                                rawTaskNodesFromApi != null &&
                                rawTaskNodesFromApi.length > 0
                            }
                        />
                        <Collapse in={resourceSectionExpanded}>
                            <Box
                                sx={{
                                    display: 'flex',
                                    flexDirection: {
                                        xs: 'column',
                                        md: 'row',
                                    },
                                    gap: 1,
                                    mt: 1,
                                }}
                            >
                                <Box
                                    sx={{
                                        flex: {
                                            xs: '1 1 auto',
                                            md: '0 0 320px',
                                        },
                                        maxWidth: { md: 320 },
                                        minWidth: 0,
                                    }}
                                >
                                    <UsageKPICards
                                        layout={
                                            isMdUp ? 'vertical' : 'horizontal'
                                        }
                                        kpiStats={kpiStats}
                                        resourceEfficiency={resourceEfficiency}
                                        selectedDataCount={
                                            selectedData.length > 0
                                                ? selectedData.length
                                                : undefined
                                        }
                                        totalDataCount={
                                            effectiveScatterData.length
                                        }
                                    />
                                </Box>
                                <Box
                                    sx={{
                                        flex: '1 1 0',
                                        minWidth: 0,
                                    }}
                                >
                                    <UsagePlotSection
                                        isLoading={usageIsLoading}
                                        filteredScatterData={
                                            effectiveScatterData
                                        }
                                        taskTemplateName={
                                            taskTemplateName || ''
                                        }
                                        medianRequestedRuntime={
                                            medianRequestedRuntimeForKPI
                                        }
                                        medianRequestedMemoryGiB={
                                            medianRequestedMemoryGiBForKPI
                                        }
                                        showResourceZones={showResourceZones}
                                        selectedInstanceIds={
                                            selectedInstanceIds
                                        }
                                        onTaskClick={handleScatterTaskClick}
                                        onSelected={handleDataSelection}
                                        onShowResourceZonesChange={
                                            setShowResourceZones
                                        }
                                        onDownloadCSV={downloadCSV}
                                        hasData={
                                            rawTaskNodesFromApi != null &&
                                            rawTaskNodesFromApi.length > 0
                                        }
                                    />
                                </Box>
                            </Box>
                        </Collapse>
                    </>
                )}
            </Box>

            {/* --- SECTION C: Task Table (full width) --- */}
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
                    showLatestOnly={showLatestOnly}
                    onShowLatestOnlyChange={setShowLatestOnly}
                    selectedWorkflowRunId={selectedWorkflowRunId}
                    availableWorkflowRunIds={availableWorkflowRunIds}
                    onSelectedWorkflowRunIdChange={setSelectedWorkflowRunId}
                />
            </Box>
        </Box>
    );
}
