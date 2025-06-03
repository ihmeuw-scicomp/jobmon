// Custom hook for managing Usage component filters and state

import { useState, useEffect, useMemo } from 'react';
import { components } from '@jobmon_gui/types/apiSchema';
import {
    extractResourceClusters,
    ResourceCluster,
} from '@jobmon_gui/components/task_template_details/usage/usageCalculations';

type TaskResourceVizItem = components['schemas']['TaskResourceVizItem'];

interface UseUsageFiltersProps {
    rawTaskNodesFromApi: TaskResourceVizItem[];
}

interface UseUsageFiltersReturn {
    selectedAttempts: Set<string>;
    selectedStatuses: Set<string>;
    selectedResourceClusters: Set<string>;
    availableAttempts: string[];
    availableStatuses: string[];
    availableResourceClusters: ResourceCluster[];
    setSelectedAttempts: (attempts: Set<string>) => void;
    setSelectedStatuses: (statuses: Set<string>) => void;
    setSelectedResourceClusters: (clusters: Set<string>) => void;
    resetFilters: () => void;
    clearFilters: () => void;
}

export const useUsageFilters = ({
    rawTaskNodesFromApi,
}: UseUsageFiltersProps): UseUsageFiltersReturn => {
    // Filter state
    const [selectedAttempts, setSelectedAttempts] = useState<Set<string>>(
        new Set()
    );
    const [selectedStatuses, setSelectedStatuses] = useState<Set<string>>(
        new Set()
    );
    const [selectedResourceClusters, setSelectedResourceClusters] = useState<
        Set<string>
    >(new Set());

    // Calculate available filter options
    const availableAttempts = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        const attempts = new Set(
            rawTaskNodesFromApi.map(d =>
                String(d.attempt_number_of_instance || 1)
            )
        );
        return Array.from(attempts).sort((a, b) => parseInt(a) - parseInt(b));
    }, [rawTaskNodesFromApi]);

    const availableStatuses = useMemo(() => {
        if (!rawTaskNodesFromApi) return [];
        const statuses = new Set(
            rawTaskNodesFromApi.map(d =>
                String(d.status || 'UNKNOWN').toUpperCase()
            )
        );
        return Array.from(statuses).sort();
    }, [rawTaskNodesFromApi]);

    const availableResourceClusters = useMemo(() => {
        return extractResourceClusters(rawTaskNodesFromApi);
    }, [rawTaskNodesFromApi]);

    // Initialize filters when data changes
    useEffect(() => {
        setSelectedAttempts(new Set(availableAttempts));
    }, [availableAttempts]);

    useEffect(() => {
        setSelectedStatuses(new Set(availableStatuses));
    }, [availableStatuses]);

    useEffect(() => {
        setSelectedResourceClusters(
            new Set(availableResourceClusters.map(cluster => cluster.id))
        );
    }, [availableResourceClusters]);

    // Reset to defaults
    const resetFilters = () => {
        setSelectedAttempts(new Set(availableAttempts));
        setSelectedStatuses(new Set(availableStatuses));
        setSelectedResourceClusters(
            new Set(availableResourceClusters.map(cluster => cluster.id))
        );
    };

    // Clear all filters
    const clearFilters = () => {
        setSelectedAttempts(new Set(availableAttempts));
        setSelectedStatuses(new Set(availableStatuses));
        setSelectedResourceClusters(
            new Set(availableResourceClusters.map(cluster => cluster.id))
        );
    };

    return {
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
    };
};
