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
    // Server-computed cluster list from the aggregates endpoint. When
    // present, takes precedence over deriving from streaming scatter rows
    // so the filter UI is complete on first paint.
    serverResourceClusters?: ResourceCluster[];
}

interface UseUsageFiltersReturn {
    selectedResourceClusters: Set<string>;
    availableResourceClusters: ResourceCluster[];
    setSelectedResourceClusters: (clusters: Set<string>) => void;
    resetFilters: () => void;
}

export const useUsageFilters = ({
    rawTaskNodesFromApi,
    serverResourceClusters,
}: UseUsageFiltersProps): UseUsageFiltersReturn => {
    const [selectedResourceClusters, setSelectedResourceClusters] = useState<
        Set<string>
    >(new Set());

    const availableResourceClusters = useMemo(() => {
        if (serverResourceClusters && serverResourceClusters.length > 0) {
            return serverResourceClusters;
        }
        return extractResourceClusters(rawTaskNodesFromApi);
    }, [serverResourceClusters, rawTaskNodesFromApi]);

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
