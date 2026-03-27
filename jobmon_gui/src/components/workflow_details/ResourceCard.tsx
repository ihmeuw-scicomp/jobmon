import { useState } from 'react';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import CircularProgress from '@mui/material/CircularProgress';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import useMediaQuery from '@mui/material/useMediaQuery';
import { useTheme } from '@mui/material/styles';
import CloseIcon from '@mui/icons-material/Close';
import MemoryIcon from '@mui/icons-material/Memory';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import NavigateBeforeIcon from '@mui/icons-material/NavigateBefore';
import humanizeDuration from 'humanize-duration';
import axios from 'axios';
import { useQuery } from '@tanstack/react-query';
import { Link, useLocation } from 'react-router-dom';
import { FatalErrorBreakdown } from '@jobmon_gui/queries/GetFatalErrorBreakdown';
import { getTaskDetailsQueryFn } from '@jobmon_gui/queries/GetTaskDetails';
import { RESOURCE_ERROR_COLORS } from '@jobmon_gui/constants/taskStatus';
import { error_log_viz_url } from '@jobmon_gui/configs/ApiUrls';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios';
import { ErrorLog } from '@jobmon_gui/types/ClusteredErrors';
import CopyButton from '@jobmon_gui/components/common/CopyButton';
import ResourceComparisonBar from '@jobmon_gui/components/task_details/ResourceComparisonBar';
import { formatBytes, bytes_to_gib } from '@jobmon_gui/utils/formatters';
import { parseResourceJson } from '@jobmon_gui/utils/csvExport';

function fmtRuntime(seconds: number | null | undefined): string {
    if (seconds == null) return 'N/A';
    return humanizeDuration(seconds * 1000, {
        largest: 1,
        round: true,
    });
}

function fmtMemory(bytes: number | null | undefined): string {
    if (bytes == null) return 'N/A';
    const gib = bytes / 1073741824;
    return `${gib.toFixed(2)} GiB`;
}

interface ResourceCardProps {
    workflowId: string | number;
    taskTemplateId: string | number;
    usageData: {
        median_runtime?: number | null;
        min_runtime?: number | null;
        max_runtime?: number | null;
        median_mem?: number | null;
        min_mem?: number | null;
        max_mem?: number | null;
    } | null;
    usageLoading: boolean;
    breakdown: FatalErrorBreakdown | null | undefined;
    breakdownLoading: boolean;
}

function StatColumn({
    label,
    value,
}: {
    label: string;
    value: string;
}) {
    return (
        <Grid item xs={12} sm={4}>
            <Typography
                variant="caption"
                color="text.secondary"
                fontWeight="medium"
                sx={{
                    textTransform: 'uppercase',
                    letterSpacing: 0.5,
                    fontSize: '0.6rem',
                }}
            >
                {label}
            </Typography>
            <Typography
                variant="body2"
                fontWeight="bold"
                sx={{ mt: 0.25 }}
            >
                {value}
            </Typography>
        </Grid>
    );
}

export default function ResourceCard({
    workflowId,
    taskTemplateId,
    usageData,
    usageLoading,
    breakdown,
    breakdownLoading: _breakdownLoading,
}: ResourceCardProps) {
    const theme = useTheme();
    const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
    const location = useLocation();
    const [selectedTiIdx, setSelectedTiIdx] = useState<
        number | null
    >(null);

    const hasUsage =
        usageData &&
        (usageData.median_runtime != null ||
            usageData.median_mem != null);
    const hasResourceErrors =
        breakdown && breakdown.resource_error_total > 0;
    const tiIds = breakdown?.resource_error_ti_ids ?? [];
    const currentTiId =
        selectedTiIdx !== null ? tiIds[selectedTiIdx] : null;

    const retried = breakdown
        ? Math.max(
              0,
              breakdown.resource_error_total -
                  breakdown.resource
          )
        : 0;

    const errorDetailQuery = useQuery<{
        error_logs: ErrorLog[];
    }>({
        queryKey: [
            'workflow_details',
            'resource_error_detail',
            workflowId,
            taskTemplateId,
            currentTiId,
        ],
        queryFn: async () => {
            return axios
                .get<{ error_logs: ErrorLog[] }>(
                    `${error_log_viz_url}${workflowId}/${taskTemplateId}/${currentTiId}`,
                    { ...jobmonAxiosConfig, data: null }
                )
                .then(r => r.data);
        },
        enabled: currentTiId != null,
        staleTime: 120000,
    });

    const errorLog =
        errorDetailQuery.data?.error_logs?.[0] ?? null;
    const taskDetailsQuery = useQuery({
        queryKey: ['task_details', errorLog?.task_id],
        queryFn: getTaskDetailsQueryFn,
        enabled: errorLog?.task_id != null,
        staleTime: 300000,
    });

    const tiDetailsQuery = useQuery({
        queryKey: ['ti_details', errorLog?.task_id],
        queryFn: async () => {
            const resp = await axios.get<{
                taskinstances: Array<{
                    ti_id: number;
                    ti_maxrss: string | null;
                    ti_wallclock: string | null;
                    ti_resources: string | null;
                }>;
            }>(
                `${import.meta.env.VITE_APP_BASE_URL}/task/get_ti_details_viz/${errorLog!.task_id}`,
                { ...jobmonAxiosConfig, data: null }
            );
            return resp.data.taskinstances;
        },
        enabled: errorLog?.task_id != null,
        staleTime: 120000,
    });

    const tiDetail = tiDetailsQuery.data?.find(
        ti => ti.ti_id === currentTiId
    );

    if (usageLoading) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                    p: 1,
                }}
            >
                <CircularProgress size={16} />
                <Typography
                    variant="caption"
                    color="text.secondary"
                >
                    Loading resources...
                </Typography>
            </Box>
        );
    }

    if (!hasUsage && !hasResourceErrors && !usageLoading) return null;

    const drawerContent = () => {
        if (errorDetailQuery.isLoading) {
            return (
                <Box
                    sx={{
                        display: 'flex',
                        justifyContent: 'center',
                        p: 4,
                    }}
                >
                    <CircularProgress />
                </Box>
            );
        }
        if (!errorLog) {
            return (
                <Typography sx={{ p: 2 }}>
                    No error details available.
                </Typography>
            );
        }

        const resources = tiDetail
            ? parseResourceJson(tiDetail.ti_resources)
            : null;
        const requestedMemGiB = resources?.memory ?? null;
        const utilizedMemGiB = tiDetail
            ? bytes_to_gib(
                  parseInt(
                      String(tiDetail.ti_maxrss ?? '0'),
                      10
                  ) || 0
              )
            : null;
        const requestedRuntimeSec =
            resources?.runtime ?? null;
        const utilizedRuntimeSec = tiDetail?.ti_wallclock
            ? parseInt(String(tiDetail.ti_wallclock), 10)
            : null;

        return (
            <Box sx={{ p: 2, overflow: 'auto' }}>
                {/* Metadata */}
                <Box
                    sx={{
                        display: 'grid',
                        gridTemplateColumns: '1fr 1fr',
                        gap: 1.5,
                        mb: 2,
                    }}
                >
                    <Box>
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            fontWeight="bold"
                        >
                            Task ID
                        </Typography>
                        {errorLog.task_id ? (
                            <Link
                                to={{
                                    pathname: `/task_details/${errorLog.task_id}`,
                                    search: location.search,
                                }}
                                style={{
                                    color: '#1976d2',
                                    fontSize: '0.875rem',
                                    display: 'block',
                                }}
                            >
                                {errorLog.task_id}
                            </Link>
                        ) : (
                            <Typography variant="body2">
                                —
                            </Typography>
                        )}
                    </Box>
                    <Box>
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            fontWeight="bold"
                        >
                            TI Status
                        </Typography>
                        <Typography variant="body2">
                            Resource Error
                        </Typography>
                    </Box>
                </Box>

                {/* Resource comparison bars */}
                {tiDetail && (
                    <Box sx={{ mb: 2 }}>
                        <Typography
                            variant="subtitle2"
                            fontWeight="bold"
                            sx={{ mb: 1 }}
                        >
                            Resource Usage
                        </Typography>
                        <Box
                            sx={{
                                display: 'flex',
                                gap: 3,
                                flexWrap: 'wrap',
                            }}
                        >
                            <Box
                                sx={{
                                    flex: '1 1 200px',
                                    maxWidth: 300,
                                }}
                            >
                                <ResourceComparisonBar
                                    label="Memory"
                                    requested={requestedMemGiB}
                                    utilized={utilizedMemGiB}
                                    requestedDisplay={
                                        requestedMemGiB != null
                                            ? `${requestedMemGiB} GiB`
                                            : 'N/A'
                                    }
                                    utilizedDisplay={formatBytes(
                                        tiDetail.ti_maxrss
                                    )}
                                />
                            </Box>
                            <Box
                                sx={{
                                    flex: '1 1 200px',
                                    maxWidth: 300,
                                }}
                            >
                                <ResourceComparisonBar
                                    label="Runtime"
                                    requested={
                                        requestedRuntimeSec
                                    }
                                    utilized={
                                        utilizedRuntimeSec
                                    }
                                    requestedDisplay={
                                        requestedRuntimeSec !=
                                        null
                                            ? humanizeDuration(
                                                  requestedRuntimeSec *
                                                      1000,
                                                  {
                                                      largest: 2,
                                                      round: true,
                                                  }
                                              )
                                            : 'N/A'
                                    }
                                    utilizedDisplay={
                                        utilizedRuntimeSec !=
                                        null
                                            ? humanizeDuration(
                                                  utilizedRuntimeSec *
                                                      1000,
                                                  {
                                                      largest: 2,
                                                      round: true,
                                                  }
                                              )
                                            : 'N/A'
                                    }
                                />
                            </Box>
                        </Box>
                    </Box>
                )}

                {/* Error message / stderr */}
                {errorLog.error && (
                    <Box sx={{ mb: 2 }}>
                        <Typography
                            variant="subtitle2"
                            fontWeight="bold"
                            sx={{ mb: 0.5 }}
                        >
                            Error
                        </Typography>
                        <Box
                            sx={{
                                maxHeight: '20vh',
                                overflow: 'auto',
                                bgcolor: '#eee',
                                borderRadius: 1,
                                px: 1.5,
                                py: 1,
                                fontFamily: 'monospace',
                                fontSize: '0.75rem',
                                whiteSpace: 'pre-wrap',
                                wordBreak: 'break-word',
                            }}
                        >
                            {errorLog.error}
                        </Box>
                    </Box>
                )}

                {/* Command */}
                {taskDetailsQuery.data?.task_command && (
                    <Box>
                        <Box
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent:
                                    'space-between',
                                mb: 0.5,
                            }}
                        >
                            <Typography
                                variant="subtitle2"
                                fontWeight="bold"
                            >
                                Command
                            </Typography>
                            <CopyButton
                                text={
                                    taskDetailsQuery.data
                                        .task_command
                                }
                                tooltip="Copy command"
                            />
                        </Box>
                        <Box
                            sx={{
                                maxHeight: '15vh',
                                overflow: 'auto',
                                bgcolor: '#eee',
                                borderRadius: 1,
                                px: 1.5,
                                py: 1,
                                fontFamily: 'monospace',
                                fontSize: '0.75rem',
                                whiteSpace: 'pre-wrap',
                                wordBreak: 'break-word',
                            }}
                        >
                            {
                                taskDetailsQuery.data
                                    .task_command
                            }
                        </Box>
                    </Box>
                )}
            </Box>
        );
    };

    return (
        <>
            <Box
                sx={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 1,
                }}
            >
                {/* Resource error card */}
                {hasResourceErrors && (
                    <Card
                        elevation={0}
                        sx={{
                            flex: '1 1 0',
                            minWidth: 0,
                            border: '1px solid',
                            borderColor:
                                RESOURCE_ERROR_COLORS.main,
                            borderLeft: '3px solid',
                            borderLeftColor:
                                RESOURCE_ERROR_COLORS.main,
                            borderRadius: 2,
                        }}
                    >
                        <CardContent
                            sx={{
                                p: 1.5,
                                '&:last-child': { pb: 1.5 },
                            }}
                        >
                            <Typography
                                variant="caption"
                                sx={{
                                    color: RESOURCE_ERROR_COLORS.main,
                                    fontWeight: 'bold',
                                    textTransform: 'uppercase',
                                    letterSpacing: 0.5,
                                    mb: 0.5,
                                    display: 'block',
                                }}
                            >
                                Resource Errors
                            </Typography>
                            <Box
                                sx={{
                                    display: 'flex',
                                    flexWrap: 'wrap',
                                    gap: 0.5,
                                    mb:
                                        tiIds.length > 0
                                            ? 1
                                            : 0,
                                }}
                            >
                                <Chip
                                    icon={
                                        <MemoryIcon
                                            sx={{
                                                fontSize: 12,
                                                color: '#fff !important',
                                            }}
                                        />
                                    }
                                    label={`${breakdown!.resource_error_total} total`}
                                    size="small"
                                    sx={{
                                        height: 22,
                                        fontSize: '0.7rem',
                                        fontWeight: 'bold',
                                        backgroundColor:
                                            RESOURCE_ERROR_COLORS.main,
                                        color: '#fff',
                                    }}
                                />
                                {retried > 0 && (
                                    <Chip
                                        label={`${retried} retried`}
                                        size="small"
                                        sx={{
                                            height: 22,
                                            fontSize: '0.7rem',
                                            backgroundColor:
                                                RESOURCE_ERROR_COLORS.retriedBg,
                                            color: RESOURCE_ERROR_COLORS.retried,
                                            fontWeight: 600,
                                        }}
                                    />
                                )}
                                {breakdown!.resource > 0 && (
                                    <Chip
                                        label={`${breakdown!.resource} fatal`}
                                        size="small"
                                        sx={{
                                            height: 22,
                                            fontSize: '0.7rem',
                                            backgroundColor:
                                                RESOURCE_ERROR_COLORS.fatal,
                                            color: '#fff',
                                            fontWeight: 600,
                                        }}
                                    />
                                )}
                            </Box>
                            {/* Clickable TI sample list */}
                            {tiIds.length > 0 && (
                                <Box
                                    sx={{
                                        display: 'flex',
                                        flexDirection: 'column',
                                        gap: 0.25,
                                        maxHeight: 100,
                                        overflow: 'auto',
                                    }}
                                >
                                    {tiIds
                                        .slice(0, 5)
                                        .map((tiId, idx) => (
                                            <Typography
                                                key={tiId}
                                                variant="caption"
                                                onClick={() =>
                                                    setSelectedTiIdx(
                                                        idx
                                                    )
                                                }
                                                sx={{
                                                    fontFamily:
                                                        'monospace',
                                                    cursor: 'pointer',
                                                    color: 'primary.main',
                                                    '&:hover': {
                                                        textDecoration:
                                                            'underline',
                                                    },
                                                }}
                                            >
                                                TI {tiId}
                                            </Typography>
                                        ))}
                                    {tiIds.length > 5 && (
                                        <Typography
                                            variant="caption"
                                            color="text.secondary"
                                        >
                                            +{tiIds.length - 5}{' '}
                                            more
                                        </Typography>
                                    )}
                                </Box>
                            )}
                        </CardContent>
                    </Card>
                )}

                {/* Runtime card */}
                {hasUsage &&
                    usageData!.median_runtime != null && (
                        <Card
                            elevation={0}
                            sx={{
                                flex: '1 1 0',
                                minWidth: 0,
                                border: '1px solid',
                                borderColor: 'divider',
                                borderLeft: '3px solid',
                                borderLeftColor:
                                    'primary.main',
                                borderRadius: 2,
                            }}
                        >
                            <CardContent
                                sx={{
                                    p: 1.5,
                                    '&:last-child': {
                                        pb: 1.5,
                                    },
                                }}
                            >
                                <Typography
                                    variant="caption"
                                    color="primary.main"
                                    fontWeight="bold"
                                    sx={{
                                        textTransform:
                                            'uppercase',
                                        letterSpacing: 0.5,
                                        mb: 0.5,
                                        display: 'block',
                                    }}
                                >
                                    Runtime
                                </Typography>
                                <Grid
                                    container
                                    spacing={0.5}
                                >
                                    <StatColumn
                                        label="Minimum"
                                        value={fmtRuntime(
                                            usageData!
                                                .min_runtime
                                        )}
                                    />
                                    <StatColumn
                                        label="Maximum"
                                        value={fmtRuntime(
                                            usageData!
                                                .max_runtime
                                        )}
                                    />
                                    <StatColumn
                                        label="Median"
                                        value={fmtRuntime(
                                            usageData!
                                                .median_runtime
                                        )}
                                    />
                                </Grid>
                            </CardContent>
                        </Card>
                    )}

                {/* Memory card */}
                {hasUsage &&
                    usageData!.median_mem != null && (
                        <Card
                            elevation={0}
                            sx={{
                                flex: '1 1 0',
                                minWidth: 0,
                                border: '1px solid',
                                borderColor: 'divider',
                                borderLeft: '3px solid',
                                borderLeftColor:
                                    'secondary.main',
                                borderRadius: 2,
                            }}
                        >
                            <CardContent
                                sx={{
                                    p: 1.5,
                                    '&:last-child': {
                                        pb: 1.5,
                                    },
                                }}
                            >
                                <Typography
                                    variant="caption"
                                    color="secondary.main"
                                    fontWeight="bold"
                                    sx={{
                                        textTransform:
                                            'uppercase',
                                        letterSpacing: 0.5,
                                        mb: 0.5,
                                        display: 'block',
                                    }}
                                >
                                    Memory
                                </Typography>
                                <Grid
                                    container
                                    spacing={0.5}
                                >
                                    <StatColumn
                                        label="Minimum"
                                        value={fmtMemory(
                                            usageData!
                                                .min_mem
                                        )}
                                    />
                                    <StatColumn
                                        label="Maximum"
                                        value={fmtMemory(
                                            usageData!
                                                .max_mem
                                        )}
                                    />
                                    <StatColumn
                                        label="Median"
                                        value={fmtMemory(
                                            usageData!
                                                .median_mem
                                        )}
                                    />
                                </Grid>
                            </CardContent>
                        </Card>
                    )}
            </Box>

            {/* Resource error detail drawer */}
            <Drawer
                anchor="right"
                open={selectedTiIdx !== null}
                onClose={() => setSelectedTiIdx(null)}
                PaperProps={{
                    sx: {
                        width: isMobile ? '100%' : 640,
                    },
                }}
            >
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        px: 2,
                        py: 1,
                        borderBottom: '1px solid',
                        borderColor: 'divider',
                    }}
                >
                    <Typography variant="subtitle1" fontWeight="bold">
                        Resource Error: TI{' '}
                        {currentTiId ?? ''}
                    </Typography>
                    <Tooltip title="Close">
                        <IconButton
                            size="small"
                            onClick={() =>
                                setSelectedTiIdx(null)
                            }
                        >
                            <CloseIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>
                </Box>
                {/* Navigation */}
                {tiIds.length > 1 && (
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            gap: 1,
                            py: 0.5,
                            borderBottom: '1px solid',
                            borderColor: 'divider',
                        }}
                    >
                        <Tooltip title="Previous">
                            <span>
                                <IconButton
                                    size="small"
                                    disabled={
                                        selectedTiIdx === 0
                                    }
                                    onClick={() =>
                                        setSelectedTiIdx(
                                            i =>
                                                i !== null
                                                    ? i - 1
                                                    : null
                                        )
                                    }
                                >
                                    <NavigateBeforeIcon fontSize="small" />
                                </IconButton>
                            </span>
                        </Tooltip>
                        <Typography variant="caption">
                            {(selectedTiIdx ?? 0) + 1} of{' '}
                            {tiIds.length}
                        </Typography>
                        <Tooltip title="Next">
                            <span>
                                <IconButton
                                    size="small"
                                    disabled={
                                        selectedTiIdx ===
                                        tiIds.length - 1
                                    }
                                    onClick={() =>
                                        setSelectedTiIdx(
                                            i =>
                                                i !== null
                                                    ? i + 1
                                                    : null
                                        )
                                    }
                                >
                                    <NavigateNextIcon fontSize="small" />
                                </IconButton>
                            </span>
                        </Tooltip>
                    </Box>
                )}
                {drawerContent()}
            </Drawer>
        </>
    );
}
