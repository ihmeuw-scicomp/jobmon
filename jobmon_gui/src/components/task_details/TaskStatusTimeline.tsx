import React, { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import CircularProgress from '@mui/material/CircularProgress';
import Tooltip from '@mui/material/Tooltip';
import Collapse from '@mui/material/Collapse';
import Button from '@mui/material/Button';
import Grid from '@mui/material/Grid';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import humanizeDuration from 'humanize-duration';
import { getTaskStatusAuditQueryFn } from '@jobmon_gui/queries/GetTaskStatusAudit';
import { getTaskInstanceDetailsQueryFn } from '@jobmon_gui/queries/GetTaskInstanceDetails';
import {
    getStatusColor,
    getStatusLabel,
    getStatusTextColor,
    taskStatusMeta,
} from '@jobmon_gui/constants/taskStatus';
import { components } from '@jobmon_gui/types/apiSchema';
import { TaskInstance } from '@jobmon_gui/types/TaskInstance';
import { formatBytes, bytes_to_gib } from '@jobmon_gui/utils/formatters';
import { parseResourceJson } from '@jobmon_gui/utils/csvExport';
import ResourceComparisonBar from './ResourceComparisonBar';
import { JobmonModal } from '@jobmon_gui/components/JobmonModal';
import { ScrollableCodeBlock } from '@jobmon_gui/components/ScrollableTextArea';

type AuditRecord = components['schemas']['TaskStatusAuditRecord'];

type TaskStatusTimelineProps = {
    workflowId: number | string;
    taskId: number | string;
};

type Segment = {
    status: string;
    label: string;
    color: string;
    durationMs: number;
    durationText: string;
    enteredAt: string | null;
    exitedAt: string | null;
    active: boolean;
};

type Attempt = {
    segments: Segment[];
    totalMs: number;
    outcome: string; // D, E, F, or "active"
};

type ModalState = {
    type: 'stdout' | 'stderr' | null;
    instance: TaskInstance | null;
};

function formatMs(ms: number): string {
    if (ms <= 0) return '<1s';
    if (ms < 1000) return `${ms}ms`;
    return humanizeDuration(ms, { largest: 2, round: true });
}

// Terminal statuses where exited_at is null but the task is not "active"
const TERMINAL_STATUSES = new Set(['D', 'F']);

function buildSegment(record: AuditRecord): Segment {
    const enteredAt = record.entered_at ?? null;
    const exitedAt = record.exited_at ?? null;
    const active = !exitedAt;
    const isTerminal = TERMINAL_STATUSES.has(record.new_status);
    const enteredMs = enteredAt ? Date.parse(enteredAt) : NaN;
    const exitedMs = exitedAt ? Date.parse(exitedAt) : NaN;
    const hasValidEnteredMs = Number.isFinite(enteredMs);
    const hasValidExitedMs = Number.isFinite(exitedMs);

    // Terminal statuses are endpoints, not durations — don't inflate the bar
    const durationMs =
        active && isTerminal
            ? 0
            : !hasValidEnteredMs
              ? 0
            : active
              ? Date.now() - enteredMs
              : hasValidExitedMs
                ? exitedMs - enteredMs
                : 0;
    const durationText = isTerminal
        ? ''
        : active
          ? 'active'
          : formatMs(durationMs);
    return {
        status: record.new_status,
        label: getStatusLabel(record.new_status),
        color: getStatusColor(record.new_status),
        durationMs: Math.max(0, durationMs),
        durationText,
        enteredAt,
        exitedAt,
        active: active && !isTerminal,
    };
}

function formatTimestamp(value: string | null): string {
    if (!value) return 'Unknown';
    const parsedMs = Date.parse(value);
    if (!Number.isFinite(parsedMs)) return 'Unknown';
    return new Date(parsedMs).toLocaleString();
}

function groupIntoAttempts(records: AuditRecord[]): Attempt[] {
    const attempts: Attempt[] = [];
    let current: Segment[] = [];

    for (const record of records) {
        const seg = buildSegment(record);
        // A new attempt starts when we transition into G (Registered),
        // except for the very first record which always starts attempt 1.
        if (seg.status === 'G' && current.length > 0) {
            attempts.push(finalizeAttempt(current));
            current = [];
        }
        // Skip Registered — task isn't active until Queued
        if (seg.status === 'G') continue;
        current.push(seg);
    }
    if (current.length > 0) {
        attempts.push(finalizeAttempt(current));
    }
    return attempts;
}

function finalizeAttempt(segments: Segment[]): Attempt {
    const totalMs = segments.reduce((sum, s) => sum + s.durationMs, 0);
    const last = segments[segments.length - 1];
    const isTerminal = TERMINAL_STATUSES.has(last.status);
    const outcome = last.active && !isTerminal ? 'active' : last.status;
    return { segments, totalMs, outcome };
}

// Minimum visible width percentage for very short segments
const MIN_SEGMENT_PCT = 3;

function outcomeLabel(outcome: string): string {
    if (outcome === 'active') return 'Running';
    return getStatusLabel(outcome);
}

function outcomeColor(outcome: string): string {
    if (outcome === 'active') return getStatusColor('R');
    return getStatusColor(outcome);
}

function sortTaskInstancesById(instances: TaskInstance[]): TaskInstance[] {
    return [...instances].sort((a, b) => {
        const aId =
            typeof a.ti_id === 'number'
                ? a.ti_id
                : parseInt(String(a.ti_id), 10) || 0;
        const bId =
            typeof b.ti_id === 'number'
                ? b.ti_id
                : parseInt(String(b.ti_id), 10) || 0;
        return aId - bId;
    });
}

function mapAttemptsToInstances(
    attempts: Attempt[],
    instances: TaskInstance[] | undefined
): Array<TaskInstance | null> {
    if (attempts.length === 0) return [];
    if (!instances || instances.length === 0) {
        return attempts.map(() => null);
    }

    // Audit records are capped (limit=100) and may drop older attempts.
    // Align from the newest side so latest attempts map to latest instances.
    const sortedInstances = sortTaskInstancesById(instances);
    const mapped = attempts.map(() => null as TaskInstance | null);
    const attemptOffset = Math.max(0, attempts.length - sortedInstances.length);
    const instanceOffset = Math.max(0, sortedInstances.length - attempts.length);
    const overlap = Math.min(attempts.length, sortedInstances.length);

    for (let i = 0; i < overlap; i += 1) {
        mapped[attemptOffset + i] = sortedInstances[instanceOffset + i];
    }

    return mapped;
}

// --- Sub-components ---

function AttemptDetailPanel({
    instance,
    onViewStdout,
    onViewStderr,
}: {
    instance: TaskInstance;
    onViewStdout: () => void;
    onViewStderr: () => void;
}) {
    const resources = parseResourceJson(instance.ti_resources);

    const requestedMemoryGiB = resources?.memory ?? null;
    const utilizedMemoryGiB = bytes_to_gib(
        parseInt(String(instance.ti_maxrss ?? '0'), 10) || 0
    );

    const requestedRuntimeSec = resources?.runtime ?? null;
    const utilizedRuntimeSec = instance.ti_wallclock
        ? parseInt(String(instance.ti_wallclock), 10)
        : null;

    const memoryDisplay = formatBytes(instance.ti_maxrss);
    const runtimeDisplay =
        utilizedRuntimeSec != null
            ? humanizeDuration(utilizedRuntimeSec * 1000, {
                  largest: 2,
                  round: true,
              })
            : null;

    const stderrPreview = instance.ti_stderr_log
        ? instance.ti_stderr_log
              .trim()
              .split('\n')
              .slice(-10)
              .join('\n')
        : null;

    // Metadata chips
    const metaChips: { label: string; value: string }[] = [];
    if (instance.ti_distributor_id) {
        metaChips.push({
            label: 'Job ID',
            value: String(instance.ti_distributor_id),
        });
    }
    if (instance.ti_nodename) {
        metaChips.push({
            label: 'Node',
            value: instance.ti_nodename,
        });
    }
    metaChips.push({
        label: 'Queue',
        value: instance.ti_queue_name || 'N/A',
    });
    if (instance.ti_cpu) {
        metaChips.push({ label: 'CPU', value: instance.ti_cpu });
    }
    if (instance.ti_io) {
        metaChips.push({ label: 'I/O', value: instance.ti_io });
    }

    return (
        <Box sx={{ px: 2, py: 1 }}>
            {/* Metadata row */}
            <Box
                sx={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 0.5,
                    mb: 1,
                    alignItems: 'baseline',
                }}
            >
                {metaChips.map(chip => (
                    <Typography
                        key={chip.label}
                        variant="body2"
                        sx={{ mr: 1.5, whiteSpace: 'nowrap' }}
                    >
                        <strong>{chip.label}:</strong> {chip.value}
                    </Typography>
                ))}
            </Box>

            {/* Resource bars */}
            <Box
                sx={{
                    display: 'flex',
                    gap: 3,
                    mb: 1,
                    flexWrap: 'wrap',
                }}
            >
                <Box sx={{ flex: '1 1 200px', maxWidth: 350 }}>
                    <ResourceComparisonBar
                        label="Memory"
                        requested={requestedMemoryGiB}
                        utilized={utilizedMemoryGiB}
                        requestedDisplay={
                            requestedMemoryGiB != null
                                ? `${requestedMemoryGiB} GiB`
                                : 'N/A'
                        }
                        utilizedDisplay={memoryDisplay}
                    />
                </Box>
                <Box sx={{ flex: '1 1 200px', maxWidth: 350 }}>
                    <ResourceComparisonBar
                        label="Runtime"
                        requested={requestedRuntimeSec}
                        utilized={utilizedRuntimeSec}
                        requestedDisplay={
                            requestedRuntimeSec != null
                                ? humanizeDuration(
                                      requestedRuntimeSec * 1000,
                                      { largest: 2, round: true }
                                  )
                                : 'N/A'
                        }
                        utilizedDisplay={runtimeDisplay ?? 'N/A'}
                    />
                </Box>
            </Box>

            {/* Stderr preview */}
            {stderrPreview && (
                <Box sx={{ mb: 0.5 }}>
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            mb: 0.25,
                        }}
                    >
                        <Typography
                            variant="caption"
                            fontWeight={700}
                        >
                            STDERR
                        </Typography>
                        <Button
                            size="small"
                            variant="outlined"
                            onClick={onViewStderr}
                            sx={{
                                py: 0,
                                px: 0.75,
                                fontSize: '0.7rem',
                                minHeight: 0,
                                lineHeight: 1.5,
                            }}
                        >
                            View Full Log
                        </Button>
                    </Box>
                    <Box
                        sx={{
                            fontFamily: 'Roboto Mono Variable',
                            fontSize: '0.75rem',
                            backgroundColor: '#eee',
                            px: 1,
                            py: 0.5,
                            borderRadius: 0.5,
                            maxHeight: '80px',
                            overflow: 'auto',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word',
                        }}
                    >
                        {stderrPreview}
                    </Box>
                </Box>
            )}

            {/* Stdout + stderr links */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                }}
            >
                <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                    }}
                >
                    <strong>Stdout:</strong>{' '}
                    {instance.ti_stdout || '/dev/null'}
                </Typography>
                <Button
                    size="small"
                    variant="outlined"
                    onClick={onViewStdout}
                    sx={{
                        py: 0,
                        px: 0.75,
                        fontSize: '0.7rem',
                        minHeight: 0,
                        lineHeight: 1.5,
                        flexShrink: 0,
                    }}
                >
                    View Full Log
                </Button>
                {!stderrPreview && (
                    <>
                        <Typography
                            variant="body2"
                            color="text.secondary"
                            sx={{ ml: 1 }}
                        >
                            <strong>Stderr:</strong> no output
                        </Typography>
                        <Button
                            size="small"
                            variant="outlined"
                            onClick={onViewStderr}
                            sx={{
                                py: 0,
                                px: 0.75,
                                fontSize: '0.7rem',
                                minHeight: 0,
                                lineHeight: 1.5,
                                flexShrink: 0,
                            }}
                        >
                            View Full Log
                        </Button>
                    </>
                )}
            </Box>
        </Box>
    );
}

function AttemptRow({
    attempt,
    index,
    instance,
    instanceLoading,
    expanded,
    onToggle,
    onViewStdout,
    onViewStderr,
}: {
    attempt: Attempt;
    index: number;
    instance: TaskInstance | null;
    instanceLoading: boolean;
    expanded: boolean;
    onToggle: () => void;
    onViewStdout: () => void;
    onViewStderr: () => void;
}) {
    // Compute proportional widths with minimum visibility
    const widths = useMemo(() => {
        const { segments, totalMs } = attempt;
        if (totalMs === 0) {
            // All zero-duration: equal width
            return segments.map(() => 100 / segments.length);
        }
        const raw = segments.map(s => (s.durationMs / totalMs) * 100);
        // Enforce minimum width
        const adjusted = raw.map(w => Math.max(w, MIN_SEGMENT_PCT));
        const sum = adjusted.reduce((a, b) => a + b, 0);
        return adjusted.map(w => (w / sum) * 100);
    }, [attempt]);

    return (
        <Box>
            <Box
                onClick={onToggle}
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                    cursor: 'pointer',
                    borderRadius: '4px',
                    px: 0.5,
                    '&:hover': {
                        backgroundColor: 'action.hover',
                    },
                }}
            >
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        flexShrink: 0,
                        color: 'text.secondary',
                    }}
                >
                    {expanded ? (
                        <ExpandLessIcon fontSize="small" />
                    ) : (
                        <ExpandMoreIcon fontSize="small" />
                    )}
                </Box>
                <Typography
                    variant="caption"
                    sx={{
                        width: 64,
                        flexShrink: 0,
                        fontWeight: 500,
                        color: 'text.secondary',
                    }}
                >
                    Attempt {index + 1}
                </Typography>
                <Box
                    sx={{
                        flex: 1,
                        display: 'flex',
                        height: 22,
                        borderRadius: '4px',
                        overflow: 'hidden',
                    }}
                >
                    {attempt.segments.map((seg, segIdx) => (
                        <Tooltip
                            key={segIdx}
                            arrow
                            placement="top"
                            title={
                                <span>
                                    <b>{seg.label}</b>
                                    <br />
                                    Duration: {seg.durationText}
                                    <br />
                                    Entered:{' '}
                                    {formatTimestamp(seg.enteredAt)}
                                    {seg.exitedAt && (
                                        <>
                                            <br />
                                            Exited:{' '}
                                            {formatTimestamp(
                                                seg.exitedAt
                                            )}
                                        </>
                                    )}
                                </span>
                            }
                        >
                            <Box
                                sx={{
                                    width: `${widths[segIdx]}%`,
                                    backgroundColor: seg.color,
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    color: getStatusTextColor(seg.status),
                                    fontSize: 10,
                                    fontWeight: 500,
                                    overflow: 'hidden',
                                    whiteSpace: 'nowrap',
                                    borderRight:
                                        segIdx <
                                        attempt.segments.length - 1
                                            ? '1px solid rgba(255,255,255,0.3)'
                                            : 'none',
                                }}
                            >
                                {widths[segIdx] > 12
                                    ? seg.label
                                    : ''}
                            </Box>
                        </Tooltip>
                    ))}
                </Box>
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.5,
                        width: 100,
                        flexShrink: 0,
                    }}
                >
                    <Box
                        sx={{
                            width: 8,
                            height: 8,
                            borderRadius: '50%',
                            backgroundColor: outcomeColor(
                                attempt.outcome
                            ),
                            flexShrink: 0,
                        }}
                    />
                    <Typography
                        variant="caption"
                        sx={{ fontWeight: 500 }}
                    >
                        {outcomeLabel(attempt.outcome)}
                    </Typography>
                </Box>
                <Typography
                    variant="caption"
                    sx={{
                        width: 90,
                        flexShrink: 0,
                        color: 'text.secondary',
                        textAlign: 'right',
                    }}
                >
                    {attempt.totalMs > 0
                        ? formatMs(attempt.totalMs)
                        : ''}
                </Typography>
            </Box>
            <Collapse in={expanded}>
                <Box
                    sx={{
                        ml: 4,
                        mt: 0.5,
                        mb: 1,
                        borderLeft: '2px solid',
                        borderColor: 'divider',
                        pl: 1.5,
                    }}
                >
                    {instanceLoading ? (
                        <Box sx={{ py: 1 }}>
                            <CircularProgress size={18} />
                        </Box>
                    ) : instance ? (
                        <AttemptDetailPanel
                            instance={instance}
                            onViewStdout={onViewStdout}
                            onViewStderr={onViewStderr}
                        />
                    ) : (
                        <Typography
                            variant="caption"
                            color="text.secondary"
                        >
                            No instance data available.
                        </Typography>
                    )}
                </Box>
            </Collapse>
        </Box>
    );
}

// --- Main component ---

export default function TaskStatusTimeline({
    workflowId,
    taskId,
}: TaskStatusTimelineProps) {
    const { data, isLoading, isError } = useQuery({
        queryKey: ['task_status_audit', workflowId, taskId],
        queryFn: getTaskStatusAuditQueryFn,
        enabled: !!workflowId && !!taskId,
    });

    const tiQuery = useQuery({
        queryKey: ['ti_details', taskId],
        refetchInterval: 60_000,
        queryFn: getTaskInstanceDetailsQueryFn,
    });

    const [expandedAttempt, setExpandedAttempt] = useState<number>(-1);
    const [modalState, setModalState] = useState<ModalState>({
        type: null,
        instance: null,
    });

    const attempts = useMemo(() => {
        const records = data?.audit_records;
        if (!records || records.length === 0) return [];
        // API returns reverse chronological; reverse to get chronological
        return groupIntoAttempts([...records].reverse());
    }, [data]);

    const attemptInstances = useMemo(
        () => mapAttemptsToInstances(attempts, tiQuery.data),
        [attempts, tiQuery.data]
    );

    // Collect unique statuses across all attempts for the legend
    const legendItems = useMemo(() => {
        const seen = new Set<string>();
        const items: { code: string; label: string; color: string }[] =
            [];
        for (const attempt of attempts) {
            for (const seg of attempt.segments) {
                const code = seg.status.toUpperCase();
                if (!seen.has(code) && taskStatusMeta[code]) {
                    seen.add(code);
                    items.push({
                        code,
                        label: seg.label,
                        color: seg.color,
                    });
                }
            }
        }
        return items;
    }, [attempts]);

    if (isLoading) {
        return (
            <Box sx={{ py: 1 }}>
                <CircularProgress size={20} />
            </Box>
        );
    }

    if (isError) {
        return (
            <Typography variant="caption" color="text.secondary">
                Unable to load status history.
            </Typography>
        );
    }

    if (attempts.length === 0) {
        return (
            <Typography variant="caption" color="text.secondary">
                No status history available.
            </Typography>
        );
    }

    const modalInstance = modalState.instance;

    return (
        <Box sx={{ py: 1 }}>
            <Typography
                variant="subtitle2"
                sx={{ mb: 1, fontWeight: 600 }}
            >
                Status Timeline
            </Typography>
            <Box
                sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    gap: 0.75,
                }}
            >
                {attempts.map((attempt, idx) => {
                    const instance = attemptInstances[idx] ?? null;
                    return (
                        <AttemptRow
                            key={idx}
                            attempt={attempt}
                            index={idx}
                            instance={instance}
                            instanceLoading={tiQuery.isLoading}
                            expanded={expandedAttempt === idx}
                            onToggle={() =>
                                setExpandedAttempt(
                                    expandedAttempt === idx
                                        ? -1
                                        : idx
                                )
                            }
                            onViewStdout={() =>
                                setModalState({
                                    type: 'stdout',
                                    instance,
                                })
                            }
                            onViewStderr={() =>
                                setModalState({
                                    type: 'stderr',
                                    instance,
                                })
                            }
                        />
                    );
                })}
            </Box>
            {/* Compact legend */}
            <Box
                sx={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 1.5,
                    mt: 1,
                }}
            >
                {legendItems.map(item => (
                    <Box
                        key={item.code}
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 0.5,
                        }}
                    >
                        <Box
                            sx={{
                                width: 8,
                                height: 8,
                                borderRadius: '50%',
                                backgroundColor: item.color,
                                flexShrink: 0,
                            }}
                        />
                        <Typography
                            variant="caption"
                            color="text.secondary"
                        >
                            {item.label}
                        </Typography>
                    </Box>
                ))}
            </Box>

            {/* Stdout modal */}
            <JobmonModal
                title="Standard Out"
                open={modalState.type === 'stdout' && !!modalInstance}
                onClose={() =>
                    setModalState({ type: null, instance: null })
                }
                width="80%"
            >
                <Grid container spacing={2}>
                    <Grid item xs={12}>
                        <Typography variant="h6">
                            Standard Out Path:
                        </Typography>
                    </Grid>
                    <Grid item xs={12}>
                        <ScrollableCodeBlock>
                            {modalInstance?.ti_stdout}
                        </ScrollableCodeBlock>
                    </Grid>
                    <Grid item xs={12}>
                        <Typography variant="h6">
                            Standard Out Log:
                        </Typography>
                    </Grid>
                    <Grid item xs={12}>
                        <ScrollableCodeBlock>
                            <pre>{modalInstance?.ti_stdout_log}</pre>
                        </ScrollableCodeBlock>
                    </Grid>
                </Grid>
            </JobmonModal>

            {/* Stderr modal */}
            <JobmonModal
                title="Standard Error"
                open={modalState.type === 'stderr' && !!modalInstance}
                onClose={() =>
                    setModalState({ type: null, instance: null })
                }
                width="80%"
            >
                <Grid container spacing={2}>
                    <Grid item xs={12}>
                        <Typography variant="h6">
                            Standard Error Path:
                        </Typography>
                    </Grid>
                    <Grid item xs={12}>
                        <ScrollableCodeBlock>
                            {modalInstance?.ti_stderr}
                        </ScrollableCodeBlock>
                    </Grid>
                    <Grid item xs={12}>
                        <Typography variant="h6">
                            Standard Error Log:
                        </Typography>
                    </Grid>
                    <Grid item xs={12}>
                        <ScrollableCodeBlock>
                            <pre>{modalInstance?.ti_stderr_log}</pre>
                        </ScrollableCodeBlock>
                    </Grid>
                    <Grid item xs={12}>
                        <Typography variant="h6">
                            Standard Error Description:
                        </Typography>
                    </Grid>
                    <Grid item xs={12}>
                        <ScrollableCodeBlock>
                            {modalInstance?.ti_error_log_description}
                        </ScrollableCodeBlock>
                    </Grid>
                </Grid>
            </JobmonModal>
        </Box>
    );
}
