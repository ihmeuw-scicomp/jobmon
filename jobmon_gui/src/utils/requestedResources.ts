import humanizeDuration from 'humanize-duration';

export type RequestedResources = Record<string, unknown>;

/** Parse a ``requested_resources`` value into a full dict. Unlike
 *  ``parseResourceJson`` (which projects to {memory, runtime}), this
 *  preserves every key the user put in compute_resources. */
export const parseRequestedResources = (
    json: string | Record<string, unknown> | null | undefined
): RequestedResources => {
    if (json == null) return {};
    if (typeof json === 'object') return json as RequestedResources;
    try {
        const parsed = JSON.parse(json);
        return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
        return {};
    }
};

const KNOWN_KEYS = ['memory', 'runtime', 'cores', 'num_cores', 'queue'];

// ``standard_output`` / ``standard_error`` are Slurm's ``--output`` /
// ``--error`` redirects, whereas Jobmon's ``stdout`` / ``stderr`` are
// the directories where Jobmon captures worker logs. Prefix the Slurm
// ones so the ownership is obvious if both ever show up together.
const LABEL_OVERRIDES: Record<string, string> = {
    stdout: 'Stdout Dir',
    stderr: 'Stderr Dir',
    standard_output: 'Slurm Standard Output',
    standard_error: 'Slurm Standard Error',
    working_dir: 'Working Dir',
    num_cores: 'Cores',
    cpu: 'CPU',
    io: 'I/O',
};

export const formatResourceLabel = (key: string): string => {
    if (key in LABEL_OVERRIDES) return LABEL_OVERRIDES[key];
    const spaced = key.replace(/_/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2');
    return spaced
        .split(' ')
        .filter(Boolean)
        .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
        .join(' ');
};

const formatNumeric = (v: unknown): number | null => {
    const n = typeof v === 'number' ? v : Number(v);
    return Number.isFinite(n) ? n : null;
};

const formatRuntime = (v: unknown): string => {
    const n = formatNumeric(v);
    if (n === null) return String(v);
    return humanizeDuration(n * 1000, { largest: 2, round: true });
};

const formatMemory = (v: unknown): string => {
    // requested_resources.memory is in GiB by jobmon convention.
    const n = formatNumeric(v);
    if (n === null) return String(v);
    return `${n >= 10 ? n.toFixed(0) : n.toFixed(1)} GiB`;
};

const formatCores = (v: unknown): string => {
    const n = formatNumeric(v);
    if (n === null) return String(v);
    return `${n} core${n === 1 ? '' : 's'}`;
};

const formatValue = (key: string, value: unknown): string => {
    if (value === null || value === undefined) return '';
    if (key === 'memory') return formatMemory(value);
    if (key === 'runtime') return formatRuntime(value);
    if (key === 'cores' || key === 'num_cores') return formatCores(value);
    if (
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean'
    ) {
        return String(value);
    }
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
};

/** Short single-line summary (table cell default). */
export const formatRequestedResourcesSummary = (
    blob: RequestedResources | null | undefined
): string => {
    if (!blob || typeof blob !== 'object') return '';
    const parts: string[] = [];
    // ``cores`` takes precedence over the legacy ``num_cores`` alias —
    // showing both would render "4 cores · 4 cores".
    const hasCores = blob.cores !== undefined && blob.cores !== null;
    for (const k of ['memory', 'runtime', 'cores', 'num_cores', 'queue']) {
        if (blob[k] === undefined || blob[k] === null) continue;
        if (k === 'num_cores' && hasCores) continue;
        if (k === 'queue') {
            parts.push(String(blob[k]));
        } else {
            parts.push(formatValue(k, blob[k]));
        }
    }
    return parts.join(' · ');
};

/** Ordered (key, value) pairs for full-blob expansion. Known keys first. */
export const formatRequestedResourcesFull = (
    blob: RequestedResources | null | undefined
): { key: string; value: string }[] => {
    if (!blob || typeof blob !== 'object') return [];
    const seen = new Set<string>();
    const rows: { key: string; value: string }[] = [];
    const hasCores = blob.cores !== undefined && blob.cores !== null;
    for (const k of KNOWN_KEYS) {
        if (k === 'num_cores' && hasCores) continue;
        if (k in blob && blob[k] !== null && blob[k] !== undefined) {
            rows.push({ key: k, value: formatValue(k, blob[k]) });
            seen.add(k);
        }
    }
    const rest = Object.keys(blob)
        .filter(k => !seen.has(k))
        .sort();
    for (const k of rest) {
        if (blob[k] === null || blob[k] === undefined) continue;
        rows.push({ key: k, value: formatValue(k, blob[k]) });
    }
    return rows;
};
