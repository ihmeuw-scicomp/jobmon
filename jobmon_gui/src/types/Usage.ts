// Shared types for Usage component and related functionality

export interface ScatterDataPoint {
    task_id: number | string;
    task_instance_id: number;
    task_name?: string;
    runtime: number;
    memory: number;
    status: string;
    attempt_num: number;
    requestedRuntime?: number; // Actual requested runtime for this task
    requestedMemory?: number; // Actual requested memory for this task (in GiB)
}

export interface ResourceEfficiencyMetrics {
    memoryUtilization: number;
    runtimeUtilization: number;
    overAllocatedMemory: number;
    underAllocatedMemory: number;
    overAllocatedRuntime: number;
    underAllocatedRuntime: number;
    p95Memory?: number;
    p95Runtime?: number;
    outlierCount: number;
}

export interface UsageKPIStats {
    minRuntime?: number;
    maxRuntime?: number;
    meanRuntime?: number;
    medianRuntime?: number;
    minMemoryGiB?: number;
    maxMemoryGiB?: number;
    meanMemoryGiB?: number;
    medianMemoryGiB?: number;
    medianRequestedRuntime?: number;
    medianRequestedMemoryGiB?: number;
}
