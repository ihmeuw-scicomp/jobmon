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
    selectedResourceClusters: Set<string>;
    availableResourceClusters: ResourceCluster[];
    setSelectedResourceClusters: (clusters: Set<string>) => void;
    resetFilters: () => void;
}

export const useUsageFilters = ({
    rawTaskNodesFromApi,
}: UseUsageFiltersProps): UseUsageFiltersReturn => {
    const [selectedResourceClusters, setSelectedResourceClusters] = useState<
        Set<string>
    >(new Set());

    const availableResourceClusters = useMemo(() => {
        return extractResourceClusters(rawTaskNodesFromApi);
    }, [rawTaskNodesFromApi]);

    // Initialize filters when data changes
    useEffect(() => {
        setSelectedResourceClusters(
            new Set(availableResourceClusters.map(cluster => cluster.id))
        );
    }, [availableResourceClusters]);

    // Reset to defaults
    const resetFilters = () => {
        setSelectedResourceClusters(
            new Set(availableResourceClusters.map(cluster => cluster.id))
        );
    };

    return {
        selectedResourceClusters,
        availableResourceClusters,
        setSelectedResourceClusters,
        resetFilters,
    };
};
