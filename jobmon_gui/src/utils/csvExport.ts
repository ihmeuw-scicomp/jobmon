import { bytes_to_gib } from '@jobmon_gui/utils/formatters';

type RawTaskNode = {
    task_id: number;
    task_name?: string;
    status?: string;
    task_status_date?: string | null;
    task_command?: string;
    task_num_attempts?: number | null;
    task_max_attempts?: number | null;
    r?: number | null;
    m?: number | null;
    attempt_number_of_instance?: number;
    node_id?: number;
    requested_resources?: string | null;
    task_instance_id?: number | null;
};

const CSV_COLUMNS = [
    'task_id',
    'task_name',
    'status',
    'task_status_date',
    'task_command',
    'task_num_attempts',
    'task_max_attempts',
    'runtime_seconds',
    'memory_gib',
    'memory_bytes',
    'attempt_number',
    'requested_runtime_seconds',
    'requested_memory_gib',
    'node_id',
    'requested_resources_json',
] as const;

export const parseResourceJson = (
    json: string | null | undefined
): { runtime?: number; memory?: number } => {
    if (!json) return {};
    try {
        const parsed = JSON.parse(json);
        const runtime = Number(parsed.runtime);
        const memory = Number(parsed.memory);
        return {
            runtime: !isNaN(runtime) && runtime > 0 ? runtime : undefined,
            memory: !isNaN(memory) && memory > 0 ? memory : undefined,
        };
    } catch {
        return {};
    }
};

const formatDateForCSV = (date: string | null | undefined): string => {
    if (!date) return '';
    try {
        return new Date(date).toISOString();
    } catch {
        return String(date);
    }
};

const escapeCSVValue = (value: string | number | null | undefined): string => {
    if (value === null || value === undefined) return '';
    const s = String(value);
    if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
};

export const downloadUsageCSV = (
    rows: RawTaskNode[],
    filename: string
): void => {
    if (!rows || rows.length === 0) return;

    const csvData = rows.map(item => {
        const runtime = typeof item.r === 'number' ? item.r : null;
        const memoryBytes = typeof item.m === 'number' ? item.m : null;
        const memoryGiB =
            memoryBytes !== null ? bytes_to_gib(memoryBytes) : null;
        const req = parseResourceJson(item.requested_resources);

        return {
            task_id: item.task_id,
            task_name: item.task_name || '',
            status: item.status || 'UNKNOWN',
            task_status_date: formatDateForCSV(item.task_status_date),
            task_command: item.task_command || '',
            task_num_attempts: item.task_num_attempts ?? null,
            task_max_attempts: item.task_max_attempts ?? null,
            runtime_seconds: runtime,
            memory_gib: memoryGiB,
            memory_bytes: memoryBytes,
            attempt_number: item.attempt_number_of_instance || 1,
            requested_runtime_seconds: req.runtime,
            requested_memory_gib: req.memory,
            node_id: item.node_id,
            requested_resources_json: item.requested_resources || '',
        };
    });

    const csvContent = [
        CSV_COLUMNS.join(','),
        ...csvData.map(row =>
            CSV_COLUMNS.map(col =>
                escapeCSVValue(row[col as keyof typeof row])
            ).join(',')
        ),
    ].join('\n');

    const blob = new Blob([csvContent], {
        type: 'text/csv;charset=utf-8;',
    });
    const link = document.createElement('a');
    if (link.download !== undefined) {
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', filename);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
};
