// Custom hook for processing usage data and calculating KPIs

import { useMemo } from 'react';
import {
    ScatterDataPoint,
    UsageKPIStats,
    ResourceEfficiencyMetrics,
} from '@jobmon_gui/types/Usage';
import {
    calculateMedian,
    calculateResourceEfficiency,
} from '@jobmon_gui/components/task_template_details/usage/usageCalculations';
import { bytes_to_gib } from '@jobmon_gui/utils/formatters';
import { components } from '@jobmon_gui/types/apiSchema';

type TaskResourceVizItem = components['schemas']['TaskResourceVizItem'];

interface UseUsageDataProps {
    rawTaskNodesFromApi: TaskResourceVizItem[];
    selectedAttempts: Set<string>;
    selectedStatuses: Set<string>;
    visibleTraces: Set<string>;
}

interface UseUsageDataReturn {
    filteredScatterData: ScatterDataPoint[];
    visibleScatterData: ScatterDataPoint[];
    kpiStats: UsageKPIStats;
    resourceEfficiency: ResourceEfficiencyMetrics;
    setVisibleTraces: (traces: Set<string>) => void;
    handlePlotRestyle: (eventData: { visible?: boolean[] }[]) => void;
}

export const useUsageData = ({
    rawTaskNodesFromApi,
    selectedAttempts,
    selectedStatuses,
    visibleTraces,
}: UseUsageDataProps): UseUsageDataReturn => {
    // Calculate median requested resources from FILTERED nodes
    const filteredRequestedRuntimes = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(
                d =>
                    selectedAttempts.has(
                        String(d.attempt_number_of_instance || 1)
                    ) &&
                    selectedStatuses.has(
                        String(d.status || 'UNKNOWN').toUpperCase()
                    )
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
    }, [rawTaskNodesFromApi, selectedAttempts, selectedStatuses]);

    const filteredRequestedMemoriesGiB = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(
                d =>
                    selectedAttempts.has(
                        String(d.attempt_number_of_instance || 1)
                    ) &&
                    selectedStatuses.has(
                        String(d.status || 'UNKNOWN').toUpperCase()
                    )
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
    }, [rawTaskNodesFromApi, selectedAttempts, selectedStatuses]);

    const medianRequestedRuntime = useMemo(
        () => calculateMedian(filteredRequestedRuntimes),
        [filteredRequestedRuntimes]
    );
    const medianRequestedMemoryGiB = useMemo(
        () => calculateMedian(filteredRequestedMemoriesGiB),
        [filteredRequestedMemoriesGiB]
    );

    // Transform and filter scatter data
    const filteredScatterData = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        return rawTaskNodesFromApi
            .filter(
                d =>
                    selectedAttempts.has(
                        String(d.attempt_number_of_instance || 1)
                    ) &&
                    selectedStatuses.has(
                        String(d.status || 'UNKNOWN').toUpperCase()
                    )
            )
            .map((item): ScatterDataPoint | null => {
                const runtime = typeof item.r === 'number' ? item.r : null;
                const memoryBytes = typeof item.m === 'number' ? item.m : null;
                const memoryGiB =
                    memoryBytes !== null ? bytes_to_gib(memoryBytes) : null;

                if (
                    runtime !== null &&
                    runtime > 0 &&
                    memoryGiB !== null &&
                    memoryGiB > 0
                ) {
                    return {
                        task_id: item.task_id,
                        task_instance_id: item.task_instance_id ?? 0,
                        runtime: runtime,
                        memory: memoryGiB,
                        status: String(item.status || 'UNKNOWN').toUpperCase(),
                        attempt_num: item.attempt_number_of_instance || 1,
                    };
                }
                return null;
            })
            .filter((item): item is ScatterDataPoint => item !== null);
    }, [rawTaskNodesFromApi, selectedAttempts, selectedStatuses]);

    // Filter scatter data based on legend visibility
    const visibleScatterData = useMemo(() => {
        return filteredScatterData.filter(d => {
            const traceKey = `${String(d.status).toUpperCase()}_${d.attempt_num}`;
            return visibleTraces.has(traceKey);
        });
    }, [filteredScatterData, visibleTraces]);

    // Calculate KPI stats based on visible data
    const visibleRuntimes = useMemo(
        () => visibleScatterData.map(d => d.runtime),
        [visibleScatterData]
    );
    const visibleMemoriesGiB = useMemo(
        () => visibleScatterData.map(d => d.memory),
        [visibleScatterData]
    );

    const kpiStats: UsageKPIStats = useMemo(
        () => ({
            minRuntime:
                visibleRuntimes.length > 0
                    ? Math.min(...visibleRuntimes)
                    : undefined,
            maxRuntime:
                visibleRuntimes.length > 0
                    ? Math.max(...visibleRuntimes)
                    : undefined,
            meanRuntime:
                visibleRuntimes.length > 0
                    ? visibleRuntimes.reduce((a, b) => a + b, 0) /
                      visibleRuntimes.length
                    : undefined,
            medianRuntime: calculateMedian(visibleRuntimes),
            minMemoryGiB:
                visibleMemoriesGiB.length > 0
                    ? Math.min(...visibleMemoriesGiB)
                    : undefined,
            maxMemoryGiB:
                visibleMemoriesGiB.length > 0
                    ? Math.max(...visibleMemoriesGiB)
                    : undefined,
            meanMemoryGiB:
                visibleMemoriesGiB.length > 0
                    ? visibleMemoriesGiB.reduce((a, b) => a + b, 0) /
                      visibleMemoriesGiB.length
                    : undefined,
            medianMemoryGiB: calculateMedian(visibleMemoriesGiB),
            medianRequestedRuntime,
            medianRequestedMemoryGiB,
        }),
        [
            visibleRuntimes,
            visibleMemoriesGiB,
            medianRequestedRuntime,
            medianRequestedMemoryGiB,
        ]
    );

    // Calculate resource efficiency metrics
    const resourceEfficiency = useMemo(() => {
        if (visibleScatterData.length === 0) {
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

        return calculateResourceEfficiency(visibleScatterData);
    }, [visibleScatterData]);

    // Handle Plotly restyle events (legend clicks) - using ref pattern from original
    const handlePlotRestyle = (eventData: { visible?: boolean[] }[]) => {
        if (eventData && eventData.length > 0) {
            const update = eventData[0];
            if (update && 'visible' in update) {
                // This would need to be implemented by the parent component
                // since we can't directly update visibleTraces here
                console.log('Plot restyle event:', eventData);
            }
        }
    };

    // Note: setVisibleTraces would need to be passed down from parent
    // This is a limitation of this hook structure
    const setVisibleTraces = (traces: Set<string>) => {
        // This function should be implemented by the parent component
        console.log('Setting visible traces:', traces);
    };

    return {
        filteredScatterData,
        visibleScatterData,
        kpiStats,
        resourceEfficiency,
        setVisibleTraces,
        handlePlotRestyle,
    };
};
