// Shared task status constants and metadata
// Used across Usage.tsx, RuntimeMemoryScatterPlot.tsx, and workflow detail views

// Template-level status colors (used in DAG, detail panel, summary panel)
export const TEMPLATE_STATUS_COLORS: Record<string, string> = {
    PENDING: '#e69f00',
    SCHEDULED: '#f0e442',
    RUNNING: '#0072b2',
    DONE: '#009e73',
    FATAL: '#d55e00',
};

export const TEMPLATE_STATUS_KEYS = [
    'PENDING',
    'SCHEDULED',
    'RUNNING',
    'DONE',
    'FATAL',
] as const;

// Used across Usage.tsx and RuntimeMemoryScatterPlot.tsx for consistent status handling

export interface TaskStatusMeta {
    label: string;
    symbol: string;
    color: string;
}

// Enhanced status metadata for explicit legend and consistent coloring
// Updated to reflect specific backend statuses and user preference for symbols
// Using JobmonProgressBar colors from CSS variables for consistency
export const taskStatusMeta: Record<string, TaskStatusMeta> = {
    D: { label: 'Done', symbol: 'circle', color: '#009e73' }, // --color-done (green)
    F: { label: 'Fatal Error', symbol: 'x', color: '#d55e00' }, // --color-fatal (orange)
    Z: { label: 'Resource Error', symbol: 'x', color: '#d55e00' }, // Use fatal color for resource errors
    X: { label: 'No Heartbeat', symbol: 'x', color: '#d55e00' }, // Use fatal color for heartbeat issues
    U: { label: 'Unknown Error', symbol: 'x', color: '#d55e00' }, // Use fatal color for unknown errors
    E: { label: 'Error', symbol: 'x', color: '#d55e00' }, // Use fatal color for general errors
    UNKNOWN: { label: 'Unknown Status', symbol: 'asterisk', color: '#757575' }, // Gray fallback
};

// Helper function to get status color
export const getStatusColor = (status: string): string => {
    return (
        taskStatusMeta[status?.toUpperCase()]?.color ||
        taskStatusMeta.UNKNOWN.color
    );
};

// Helper function to get status label
export const getStatusLabel = (status: string): string => {
    return taskStatusMeta[status?.toUpperCase()]?.label || status || 'Unknown';
};

// Helper function to get status symbol
export const getStatusSymbol = (status: string): string => {
    return (
        taskStatusMeta[status?.toUpperCase()]?.symbol ||
        taskStatusMeta.UNKNOWN.symbol
    );
};
