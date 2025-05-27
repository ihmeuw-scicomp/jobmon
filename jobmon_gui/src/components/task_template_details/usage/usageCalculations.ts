// Utility functions for usage calculations and data processing

import { ScatterDataPoint, ResourceEfficiencyMetrics } from '@jobmon_gui/types/Usage';
import { components } from '@jobmon_gui/types/apiSchema';

type TaskResourceVizItem = components["schemas"]["TaskResourceVizItem"];

/**
 * Resource zone classification for efficiency analysis
 */
export interface ResourceZoneData {
    runtimeUtilization: number;  // percentage of requested
    memoryUtilization: number;   // percentage of requested  
    zone: 'well-allocated' | 'over-allocated' | 'under-allocated' | 'mixed-efficiency';
    zoneColor: string;
    zoneLabel: string;
    riskLevel: 'low' | 'medium' | 'high';
    recommendation: string;
}

/**
 * Zone colors for visual representation
 */
export const ZONE_COLORS = {
    'well-allocated': 'rgba(76, 175, 80, 0.15)',    // Light green
    'over-allocated': 'rgba(255, 193, 7, 0.15)',    // Light amber  
    'under-allocated': 'rgba(244, 67, 54, 0.15)',   // Light red
    'mixed-efficiency': 'rgba(156, 39, 176, 0.15)'  // Light purple
} as const;

/**
 * Calculate resource efficiency zone for a single task
 */
export const calculateResourceZone = (
    actualRuntime: number,
    actualMemory: number,
    requestedRuntime: number,
    requestedMemory: number
): ResourceZoneData => {
    // Handle edge cases
    if (!requestedRuntime || !requestedMemory || requestedRuntime <= 0 || requestedMemory <= 0) {
        return {
            runtimeUtilization: 0,
            memoryUtilization: 0,
            zone: 'mixed-efficiency',
            zoneColor: ZONE_COLORS['mixed-efficiency'],
            zoneLabel: 'Unknown',
            riskLevel: 'medium',
            recommendation: 'Insufficient resource request data'
        };
    }

    const runtimeUtil = (actualRuntime / requestedRuntime) * 100;
    const memoryUtil = (actualMemory / requestedMemory) * 100;

    // Zone classification logic
    let zone: ResourceZoneData['zone'];
    let riskLevel: ResourceZoneData['riskLevel'];
    let recommendation: string;

    if (runtimeUtil >= 50 && runtimeUtil <= 90 && memoryUtil >= 50 && memoryUtil <= 90) {
        // Both resources well-utilized
        zone = 'well-allocated';
        riskLevel = 'low';
        recommendation = 'Good resource allocation!';
    } else if (runtimeUtil > 90 || memoryUtil > 90) {
        // At least one resource over-utilized (risk of failure)
        zone = 'under-allocated';
        riskLevel = 'high';
        
        if (runtimeUtil > 90 && memoryUtil > 90) {
            recommendation = `Increase both: runtime to ~${Math.ceil(actualRuntime * 1.2)}s, memory to ~${(actualMemory * 1.2).toFixed(1)}G`;
        } else if (runtimeUtil > 90) {
            recommendation = `Increase runtime to ~${Math.ceil(actualRuntime * 1.2)}s`;
        } else {
            recommendation = `Increase memory to ~${(actualMemory * 1.2).toFixed(1)}G`;
        }
    } else if (runtimeUtil < 50 && memoryUtil < 50) {
        // Both resources under-utilized (wasteful)
        zone = 'over-allocated';
        riskLevel = 'low';
        recommendation = `Reduce allocations: runtime to ~${Math.ceil(actualRuntime * 1.3)}s, memory to ~${(actualMemory * 1.3).toFixed(1)}G`;
    } else {
        // Mixed efficiency - one good, one bad
        zone = 'mixed-efficiency';
        riskLevel = 'medium';
        
        if (runtimeUtil < 50) {
            recommendation = `Reduce runtime to ~${Math.ceil(actualRuntime * 1.3)}s`;
        } else if (memoryUtil < 50) {
            recommendation = `Reduce memory to ~${(actualMemory * 1.3).toFixed(1)}G`;
        } else if (runtimeUtil > 90) {
            recommendation = `Increase runtime to ~${Math.ceil(actualRuntime * 1.2)}s`;
        } else {
            recommendation = `Increase memory to ~${(actualMemory * 1.2).toFixed(1)}G`;
        }
    }

    const zoneLabels = {
        'well-allocated': 'Well-Allocated',
        'over-allocated': 'Over-Allocated',
        'under-allocated': 'Under-Allocated', 
        'mixed-efficiency': 'Mixed Efficiency'
    };

    return {
        runtimeUtilization: runtimeUtil,
        memoryUtilization: memoryUtil,
        zone,
        zoneColor: ZONE_COLORS[zone],
        zoneLabel: zoneLabels[zone],
        riskLevel,
        recommendation
    };
};

/**
 * Calculate zone boundaries for plot background
 */
export const calculateZoneBoundaries = (
    medianRequestedRuntime: number,
    medianRequestedMemory: number,
    plotBounds: { xMin: number; xMax: number; yMin: number; yMax: number }
) => {
    if (!medianRequestedRuntime || !medianRequestedMemory) {
        return { shapes: [], annotations: [] };
    }

    const runtime50 = medianRequestedRuntime * 0.5;
    const runtime90 = medianRequestedRuntime * 0.9;
    const memory50 = medianRequestedMemory * 0.5;
    const memory90 = medianRequestedMemory * 0.9;

    // Ensure boundaries are within plot bounds
    const xMin = Math.max(plotBounds.xMin, 0);
    const xMax = plotBounds.xMax;
    const yMin = Math.max(plotBounds.yMin, 0);
    const yMax = plotBounds.yMax;

    const shapes = [
        // Over-allocated zone (bottom-left)
        {
            type: 'rect',
            x0: xMin,
            x1: Math.min(runtime50, xMax),
            y0: yMin,
            y1: Math.min(memory50, yMax),
            fillcolor: ZONE_COLORS['over-allocated'],
            line: { width: 0 },
            layer: 'below'
        },
        // Well-allocated zone (center)
        {
            type: 'rect',
            x0: Math.max(runtime50, xMin),
            x1: Math.min(runtime90, xMax),
            y0: Math.max(memory50, yMin),
            y1: Math.min(memory90, yMax),
            fillcolor: ZONE_COLORS['well-allocated'],
            line: { width: 0 },
            layer: 'below'
        },
        // Under-allocated zones (high risk) - cover all areas where runtime > 90% OR memory > 90%
        // Top strip: high memory utilization (memory > 90%, any runtime)
        {
            type: 'rect',
            x0: xMin,
            x1: xMax,
            y0: Math.max(memory90, yMin),
            y1: yMax,
            fillcolor: ZONE_COLORS['under-allocated'],
            line: { width: 0 },
            layer: 'below'
        },
        // Right strip: high runtime utilization (runtime > 90%, any memory)
        {
            type: 'rect',
            x0: Math.max(runtime90, xMin),
            x1: xMax,
            y0: yMin,
            y1: yMax,
            fillcolor: ZONE_COLORS['under-allocated'],
            line: { width: 0 },
            layer: 'below'
        },
        // Mixed efficiency zones (remaining areas)
        // Top-left: low runtime, high memory (but not > 90%)
        {
            type: 'rect',
            x0: xMin,
            x1: Math.min(runtime50, xMax),
            y0: Math.max(memory50, yMin),
            y1: Math.min(memory90, yMax),
            fillcolor: ZONE_COLORS['mixed-efficiency'],
            line: { width: 0 },
            layer: 'below'
        },
        // Bottom-right: high runtime (but not > 90%), low memory
        {
            type: 'rect',
            x0: Math.max(runtime50, xMin),
            x1: Math.min(runtime90, xMax),
            y0: yMin,
            y1: Math.min(memory50, yMax),
            fillcolor: ZONE_COLORS['mixed-efficiency'],
            line: { width: 0 },
            layer: 'below'
        }
    ];

    // Reference lines
    const referenceLines = [
        // Vertical reference lines (runtime)
        {
            type: 'line',
            x0: runtime50,
            x1: runtime50,
            y0: yMin,
            y1: yMax,
            line: { color: 'rgba(0,0,0,0.3)', width: 1, dash: 'dash' }
        },
        {
            type: 'line',
            x0: runtime90,
            x1: runtime90,
            y0: yMin,
            y1: yMax,
            line: { color: 'rgba(0,0,0,0.3)', width: 1, dash: 'dash' }
        },
        // Horizontal reference lines (memory)
        {
            type: 'line',
            x0: xMin,
            x1: xMax,
            y0: memory50,
            y1: memory50,
            line: { color: 'rgba(0,0,0,0.3)', width: 1, dash: 'dash' }
        },
        {
            type: 'line',
            x0: xMin,
            x1: xMax,
            y0: memory90,
            y1: memory90,
            line: { color: 'rgba(0,0,0,0.3)', width: 1, dash: 'dash' }
        }
    ];

    // Zone labels
    const annotations = [
        {
            x: (xMin + Math.min(runtime50, xMax)) / 2,
            y: (yMin + Math.min(memory50, yMax)) / 2,
            text: 'Over-Allocated<br>(Wasteful)',
            showarrow: false,
            font: { size: 10, color: 'rgba(0,0,0,0.6)' },
            bgcolor: 'rgba(255,255,255,0.8)',
            bordercolor: 'rgba(0,0,0,0.2)',
            borderwidth: 1
        },
        {
            x: (Math.max(runtime50, xMin) + Math.min(runtime90, xMax)) / 2,
            y: (Math.max(memory50, yMin) + Math.min(memory90, yMax)) / 2,
            text: 'Well-Allocated<br>(Optimal)',
            showarrow: false,
            font: { size: 10, color: 'rgba(0,0,0,0.6)' },
            bgcolor: 'rgba(255,255,255,0.8)',
            bordercolor: 'rgba(0,0,0,0.2)',
            borderwidth: 1
        },
        {
            x: (Math.max(runtime90, xMin) + xMax) / 2,
            y: (Math.max(memory90, yMin) + yMax) / 2,
            text: 'Under-Allocated<br>(High Risk)',
            showarrow: false,
            font: { size: 10, color: 'rgba(0,0,0,0.6)' },
            bgcolor: 'rgba(255,255,255,0.8)',
            bordercolor: 'rgba(0,0,0,0.2)',
            borderwidth: 1
        }
    ];

    return {
        shapes: [...shapes, ...referenceLines],
        annotations
    };
};

/**
 * Calculate median value from an array of numbers
 */
export const calculateMedian = (arr: (number | undefined | null)[]): number | undefined => {
    if (!arr || arr.length === 0) return undefined;
    const filteredArr = arr.filter(val => typeof val === 'number' && !isNaN(val)) as number[];
    if (filteredArr.length === 0) return undefined;
    const sortedArr = [...filteredArr].sort((a, b) => a - b);
    const mid = Math.floor(sortedArr.length / 2);
    return sortedArr.length % 2 !== 0 ? sortedArr[mid] : (sortedArr[mid - 1] + sortedArr[mid]) / 2;
};

/**
 * Calculate percentile value from an array of numbers
 */
export const calculatePercentile = (arr: number[], percentile: number): number | undefined => {
    if (!arr || arr.length === 0) return undefined;
    const sorted = [...arr].sort((a, b) => a - b);
    const index = (percentile / 100) * (sorted.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    
    if (lower === upper) return sorted[lower];
    return sorted[lower] * (upper - index) + sorted[upper] * (index - lower);
};

/**
 * Calculate resource efficiency metrics using embedded requested resources
 */
export const calculateResourceEfficiency = (
    data: ScatterDataPoint[], 
    requestedData?: TaskResourceVizItem[]  // Made optional since we now use embedded data
): ResourceEfficiencyMetrics => {
    if (!data || data.length === 0) {
        return {
            memoryUtilization: 0,
            runtimeUtilization: 0,
            overAllocatedMemory: 0,
            underAllocatedMemory: 0,
            overAllocatedRuntime: 0,
            underAllocatedRuntime: 0,
            p95Memory: undefined,
            p95Runtime: undefined,
            outlierCount: 0
        };
    }

    let totalActualMemory = 0;
    let totalRequestedMemory = 0;
    let totalActualRuntime = 0;
    let totalRequestedRuntime = 0;
    let overAllocatedMemory = 0;
    let underAllocatedMemory = 0;
    let overAllocatedRuntime = 0;
    let underAllocatedRuntime = 0;
    let validDataPoints = 0;

    data.forEach(point => {
        // Use the embedded requested resources from the ScatterDataPoint
        if (point.requestedMemory && point.requestedMemory > 0 && 
            point.requestedRuntime && point.requestedRuntime > 0) {
            
            // Sum up totals for overall efficiency calculation
            totalActualMemory += point.memory;
            totalRequestedMemory += point.requestedMemory;
            totalActualRuntime += point.runtime;
            totalRequestedRuntime += point.requestedRuntime;
            validDataPoints++;
            
            // Calculate individual utilization for threshold checks
            const memUtilization = (point.memory / point.requestedMemory) * 100;
            const runtimeUtilization = (point.runtime / point.requestedRuntime) * 100;

            // Over-allocated: using less than 50% of requested
            if (memUtilization < 50) overAllocatedMemory++;
            // Under-allocated: using more than 90% of requested
            if (memUtilization > 90) underAllocatedMemory++;
            
            if (runtimeUtilization < 50) overAllocatedRuntime++;
            if (runtimeUtilization > 90) underAllocatedRuntime++;
        }
    });

    const runtimes = data.map(d => d.runtime);
    const memories = data.map(d => d.memory);
    
    // Calculate outliers (beyond 2 standard deviations)
    const runtimeMean = runtimes.reduce((a, b) => a + b, 0) / runtimes.length;
    const runtimeStd = Math.sqrt(runtimes.reduce((sq, n) => sq + Math.pow(n - runtimeMean, 2), 0) / runtimes.length);
    const memoryMean = memories.reduce((a, b) => a + b, 0) / memories.length;
    const memoryStd = Math.sqrt(memories.reduce((sq, n) => sq + Math.pow(n - memoryMean, 2), 0) / memories.length);
    
    const outlierCount = data.filter(d => 
        Math.abs(d.runtime - runtimeMean) > 2 * runtimeStd || 
        Math.abs(d.memory - memoryMean) > 2 * memoryStd
    ).length;

    // Calculate overall efficiency based on sum of actual vs sum of requested
    const overallMemoryUtilization = totalRequestedMemory > 0 ? (totalActualMemory / totalRequestedMemory) * 100 : 0;
    const overallRuntimeUtilization = totalRequestedRuntime > 0 ? (totalActualRuntime / totalRequestedRuntime) * 100 : 0;

    return {
        memoryUtilization: overallMemoryUtilization,
        runtimeUtilization: overallRuntimeUtilization,
        overAllocatedMemory: validDataPoints > 0 ? Math.round((overAllocatedMemory / validDataPoints) * 100) : 0,
        underAllocatedMemory: validDataPoints > 0 ? Math.round((underAllocatedMemory / validDataPoints) * 100) : 0,
        overAllocatedRuntime: validDataPoints > 0 ? Math.round((overAllocatedRuntime / validDataPoints) * 100) : 0,
        underAllocatedRuntime: validDataPoints > 0 ? Math.round((underAllocatedRuntime / validDataPoints) * 100) : 0,
        p95Memory: calculatePercentile(memories, 95),
        p95Runtime: calculatePercentile(runtimes, 95),
        outlierCount
    };
};

/**
 * Resource cluster interface for grouping tasks by requested resources
 */
export interface ResourceCluster {
    id: string;
    runtime: number;
    memory: number;
    label: string;
    taskCount: number;
}

/**
 * Create a unique identifier for a resource configuration
 */
export const createResourceClusterKey = (runtime: number, memory: number): string => {
    const runtimeHours = Math.round(runtime / 3600 * 10) / 10; // Round to 1 decimal place
    const memoryGiB = Math.round(memory * 10) / 10; // Round to 1 decimal place
    return `${memoryGiB}G-${runtimeHours}h`;
};

/**
 * Create a human-readable label for a resource cluster
 */
export const createResourceClusterLabel = (runtime: number, memory: number, taskCount: number): string => {
    const runtimeHours = Math.round(runtime / 3600 * 10) / 10;
    const memoryGiB = Math.round(memory * 10) / 10;
    return `${memoryGiB}G Memory, ${runtimeHours}h Runtime (${taskCount} tasks)`;
};

/**
 * Extract and group tasks by their requested resource configurations
 */
export const extractResourceClusters = (rawData: TaskResourceVizItem[]): ResourceCluster[] => {
    if (!rawData || rawData.length === 0) return [];

    const clusterMap = new Map<string, { runtime: number; memory: number; count: number }>();

    rawData.forEach(item => {
        try {
            const reqRes = item.requested_resources ? JSON.parse(item.requested_resources) : {};
            const runtime = Number(reqRes.runtime);
            const memory = Number(reqRes.memory);

            if (!isNaN(runtime) && !isNaN(memory) && runtime > 0 && memory > 0) {
                const key = createResourceClusterKey(runtime, memory);
                
                if (clusterMap.has(key)) {
                    clusterMap.get(key)!.count++;
                } else {
                    clusterMap.set(key, { runtime, memory, count: 1 });
                }
            }
        } catch (e) {
            // Skip invalid JSON
        }
    });

    // Convert map to array and sort by task count (descending)
    return Array.from(clusterMap.entries())
        .map(([key, data]) => ({
            id: key,
            runtime: data.runtime,
            memory: data.memory,
            label: createResourceClusterLabel(data.runtime, data.memory, data.count),
            taskCount: data.count
        }))
        .sort((a, b) => b.taskCount - a.taskCount);
};

/**
 * Get the resource cluster key for a given task's requested resources
 */
export const getResourceClusterKey = (requestedResourcesJson: string | null | undefined): string | null => {
    if (!requestedResourcesJson) return null;
    
    try {
        const reqRes = JSON.parse(requestedResourcesJson);
        const runtime = Number(reqRes.runtime);
        const memory = Number(reqRes.memory);
        
        if (!isNaN(runtime) && !isNaN(memory) && runtime > 0 && memory > 0) {
            return createResourceClusterKey(runtime, memory);
        }
    } catch (e) {
        // Skip invalid JSON
    }
    
    return null;
}; 