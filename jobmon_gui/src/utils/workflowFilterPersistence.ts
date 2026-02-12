/**
 * Utility functions for persisting and restoring workflow overview filter parameters
 * across navigation.
 */

const FILTER_STORAGE_KEY = 'workflowOverviewFilters';

/**
 * Save filter parameters to sessionStorage
 */
export function saveWorkflowFilters(searchParams: string): void {
    if (searchParams) {
        sessionStorage.setItem(FILTER_STORAGE_KEY, searchParams);
    }
}

/**
 * Get saved filter parameters from sessionStorage
 */
export function getSavedWorkflowFilters(): string | null {
    return sessionStorage.getItem(FILTER_STORAGE_KEY);
}

/**
 * Get filter parameters to use for navigation, preferring current location
 * search params over saved filters
 */
export function getWorkflowFiltersForNavigation(currentSearch: string): string {
    return currentSearch || getSavedWorkflowFilters() || '';
}
