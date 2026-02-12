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
    G: { label: 'Registered', symbol: 'diamond', color: '#e69f00' }, // --color-pending
    Q: { label: 'Queued', symbol: 'diamond', color: '#e69f00' }, // --color-pending
    I: { label: 'Instantiated', symbol: 'diamond', color: '#e69f00' }, // --color-pending
    O: { label: 'Scheduled', symbol: 'square', color: '#f0e442' }, // --color-scheduled
    R: { label: 'Running', symbol: 'triangle-up', color: '#0072b2' }, // --color-running
    D: { label: 'Done', symbol: 'circle', color: '#009e73' }, // --color-done (green)
    A: { label: 'Adjusting', symbol: 'diamond', color: '#785abd' }, // --color-aborted
    F: { label: 'Fatal Error', symbol: 'x', color: '#d55e00' }, // --color-fatal (orange)
    Z: { label: 'Resource Error', symbol: 'x', color: '#d55e00' }, // Use fatal color for resource errors
    X: { label: 'No Heartbeat', symbol: 'x', color: '#d55e00' }, // Use fatal color for heartbeat issues
    U: { label: 'Unknown Error', symbol: 'x', color: '#d55e00' }, // Use fatal color for unknown errors
    E: { label: 'Error', symbol: 'x', color: '#d55e00' }, // Use fatal color for general errors
    UNKNOWN: { label: 'Unknown Status', symbol: 'asterisk', color: '#757575' }, // Gray fallback
};

// Status codes that represent error/failure states
export const ERROR_STATUSES = ['E', 'F', 'A', 'Z', 'X', 'U'] as const;

// Helper function to get status color
export const getStatusColor = (status: string): string => {
    return (
        taskStatusMeta[status?.toUpperCase()]?.color ||
        taskStatusMeta.UNKNOWN.color
    );
};

const normalizeHex = (color: string): string => {
    const hex = color.trim().replace(/^#/, '');
    if (hex.length === 3) {
        return hex
            .split('')
            .map(c => c + c)
            .join('');
    }
    return hex;
};

const toRelativeLuminance = (hexColor: string): number => {
    const hex = normalizeHex(hexColor);
    if (!/^[0-9a-fA-F]{6}$/.test(hex)) {
        return 0;
    }

    const channels = [0, 2, 4].map(
        i => parseInt(hex.slice(i, i + 2), 16) / 255
    );
    const [r, g, b] = channels.map(channel =>
        channel <= 0.03928
            ? channel / 12.92
            : ((channel + 0.055) / 1.055) ** 2.4
    );
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};

const getContrastRatio = (foreground: string, background: string): number => {
    const fg = toRelativeLuminance(foreground);
    const bg = toRelativeLuminance(background);
    const lighter = Math.max(fg, bg);
    const darker = Math.min(fg, bg);
    return (lighter + 0.05) / (darker + 0.05);
};

// Select black/white text based on best contrast for the status color.
export const getStatusTextColor = (status: string): string => {
    const backgroundColor = getStatusColor(status);
    const whiteContrast = getContrastRatio('#ffffff', backgroundColor);
    const blackContrast = getContrastRatio('#000000', backgroundColor);
    return whiteContrast >= blackContrast ? '#fff' : '#000';
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
