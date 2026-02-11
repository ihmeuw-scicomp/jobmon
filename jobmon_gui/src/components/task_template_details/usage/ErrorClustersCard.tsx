import React, { useEffect, useState } from 'react';
import {
    Box,
    Button,
    Card,
    CardContent,
    Chip,
    CircularProgress,
    Divider,
    Drawer,
    Grid,
    IconButton,
    Skeleton,
    Typography,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import NavigateBeforeIcon from '@mui/icons-material/NavigateBefore';
import axios from 'axios';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useLocation } from 'react-router-dom';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { error_log_viz_url } from '@jobmon_gui/configs/ApiUrls';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios';
import HtmlTooltip from '@jobmon_gui/components/HtmlToolTip';
import { formatJobmonDate } from '@jobmon_gui/utils/DayTime.ts';
import { getTaskDetailsQueryFn } from
    '@jobmon_gui/queries/GetTaskDetails.ts';
import {
    ClusteredError,
    ErrorDetails,
    ErrorSampleDetails,
} from '@jobmon_gui/types/ClusteredErrors.ts';

interface ErrorClustersCardProps {
    errorLogs: ClusteredError[];
    isLoading: boolean;
    workflowId: number | string;
    taskTemplateId: number | string;
    selectedInstanceIds?: Set<number>;
    onFilterByInstanceIds?: (instanceIds: number[]) => void;
    maxListHeight?: number;
}

const ErrorClustersCard: React.FC<ErrorClustersCardProps> = ({
    errorLogs,
    isLoading,
    workflowId,
    taskTemplateId,
    selectedInstanceIds,
    onFilterByInstanceIds,
    maxListHeight = 180,
}) => {
    const queryClient = useQueryClient();
    const location = useLocation();
    const [errorDetailIndex, setErrorDetailIndex] = useState<
        ErrorSampleDetails | null
    >(null);
    const [language, setLanguage] = useState('python');

    // --- Error detail query ---
    const errorDetails = useQuery({
        queryKey: [
            'workflow_details',
            'error_details',
            workflowId,
            taskTemplateId,
            errorDetailIndex,
        ],
        queryFn: async () => {
            if (errorDetailIndex === null) {
                return;
            }
            if (
                errorDetailIndex.sample_index >=
                errorDetailIndex.sample_ids.length
            ) {
                return;
            }
            const ti_id =
                errorDetailIndex.sample_ids[
                    errorDetailIndex.sample_index
                ];
            return axios
                .get(
                    `${error_log_viz_url}${workflowId}/${taskTemplateId}/${ti_id}`,
                    { ...jobmonAxiosConfig, data: null }
                )
                .then(r => r.data);
        },
        enabled: !!taskTemplateId && errorDetailIndex !== null,
    });

    // Prefetch next sample
    const prefetchErrorDetails = async (
        nextIdx: ErrorSampleDetails
    ) => {
        await queryClient.prefetchQuery({
            queryKey: [
                'workflow_details',
                'error_details',
                workflowId,
                taskTemplateId,
                nextIdx,
            ],
            queryFn: async () => {
                if (
                    nextIdx.sample_index >=
                    nextIdx.sample_ids.length
                ) {
                    return;
                }
                const ti_id =
                    nextIdx.sample_ids[nextIdx.sample_index];
                return axios
                    .get(
                        `${error_log_viz_url}${workflowId}/${taskTemplateId}/${ti_id}`,
                        { ...jobmonAxiosConfig, data: null }
                    )
                    .then(r => r.data);
            },
        });
    };

    useEffect(() => {
        if (errorDetailIndex === null) {
            return;
        }
        if (
            errorDetailIndex.sample_index <
            errorDetailIndex.sample_ids.length - 1
        ) {
            const nextErrorDetails = {
                ...errorDetailIndex,
                sample_index: errorDetailIndex.sample_index + 1,
            };
            void prefetchErrorDetails(nextErrorDetails);
        }
    }, [errorDetailIndex]);

    // --- Computed metrics ---
    const errorClusterCount = errorLogs.length;
    const totalFailures = errorLogs.reduce(
        (sum, el) => sum + (el.group_instance_count || 0),
        0
    );
    const hasErrors = errorClusterCount > 0 || totalFailures > 0;

    // --- Sample navigation ---
    const nextSample = () => {
        if (errorDetailIndex === null) {
            return;
        }
        setErrorDetailIndex({
            ...errorDetailIndex,
            sample_index: errorDetailIndex.sample_index + 1,
        });
    };

    const previousSample = () => {
        if (
            errorDetailIndex === null ||
            errorDetailIndex.sample_index === 0
        ) {
            return;
        }
        setErrorDetailIndex({
            ...errorDetailIndex,
            sample_index: errorDetailIndex.sample_index - 1,
        });
    };

    const toggleLanguage = () => {
        setLanguage(prev => (prev === 'python' ? 'r' : 'python'));
    };

    const currentTiID = () => {
        if (errorDetailIndex === null) return '';
        return errorDetailIndex.sample_ids[
            errorDetailIndex.sample_index
        ];
    };

    // --- Drawer content ---
    const drawerContent = () => {
        if (errorDetails.isLoading) {
            return (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                    <CircularProgress />
                </Box>
            );
        }
        const error =
            (errorDetails as unknown as ErrorDetails)?.data
                ?.error_logs?.[0] ?? null;
        if (errorDetails.isError || !error) {
            return (
                <Typography sx={{ p: 2 }}>
                    Failed to retrieve error details. Please
                    refresh and try again
                </Typography>
            );
        }

        const labelStyles = {
            fontWeight: 'bold',
            color: 'text.secondary',
            fontSize: '0.75rem',
        };

        return (
            <Box sx={{ p: 2, overflow: 'auto' }}>
                {/* Metadata */}
                <Box sx={{
                    display: 'grid',
                    gridTemplateColumns: '1fr 1fr',
                    gap: 1.5,
                    mb: 2,
                }}>
                    <Box>
                        <Typography sx={labelStyles}>
                            Error Time
                        </Typography>
                        <Typography variant="body2">
                            {formatJobmonDate(error.error_time)}
                        </Typography>
                    </Box>
                    <Box>
                        <Typography sx={labelStyles}>
                            Task ID
                        </Typography>
                        {error.task_id ? (
                            <Link
                                to={{
                                    pathname: `/task_details/${error.task_id}`,
                                    search: location.search,
                                }}
                                style={{
                                    color: '#1976d2',
                                    fontSize: '0.875rem',
                                }}
                                onMouseEnter={async () => {
                                    queryClient.prefetchQuery({
                                        queryKey: [
                                            'task_details',
                                            error.task_id,
                                        ],
                                        queryFn:
                                            getTaskDetailsQueryFn,
                                    });
                                }}
                            >
                                {error.task_id}
                            </Link>
                        ) : (
                            <Typography variant="body2">
                                —
                            </Typography>
                        )}
                    </Box>
                    <Box>
                        <Typography sx={labelStyles}>
                            Error ID
                        </Typography>
                        <Typography variant="body2">
                            {error.task_instance_err_id}
                        </Typography>
                    </Box>
                    <Box>
                        <Typography sx={labelStyles}>
                            WF Run ID
                        </Typography>
                        <Typography variant="body2">
                            {error.workflow_run_id}
                        </Typography>
                    </Box>
                </Box>

                <Divider sx={{ mb: 2 }} />

                {/* Error Message */}
                <Box sx={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    mb: 1,
                }}>
                    <Typography
                        variant="subtitle2"
                        fontWeight="bold"
                    >
                        Error Message
                    </Typography>
                    <Button
                        size="small"
                        variant="outlined"
                        onClick={toggleLanguage}
                        sx={{ minWidth: 0, px: 1 }}
                    >
                        {language === 'python' ? 'R' : 'Python'}
                    </Button>
                </Box>
                <Box sx={{
                    maxHeight: '40vh',
                    overflow: 'auto',
                    bgcolor: '#eee',
                    borderRadius: 1,
                    fontSize: '0.75rem',
                    '& pre': {
                        whiteSpace: 'pre-wrap !important',
                        wordBreak: 'break-word !important',
                        m: '0 !important',
                    },
                }}>
                    <SyntaxHighlighter
                        language={language}
                        wrapLongLines
                    >
                        {error.error}
                    </SyntaxHighlighter>
                </Box>

                {/* stderr */}
                <Typography
                    variant="subtitle2"
                    fontWeight="bold"
                    sx={{ mt: 2, mb: 1 }}
                >
                    Task Instance stderr
                </Typography>
                <Box sx={{
                    maxHeight: '40vh',
                    overflow: 'auto',
                    bgcolor: '#eee',
                    borderRadius: 1,
                    fontSize: '0.75rem',
                    '& pre': {
                        whiteSpace: 'pre-wrap !important',
                        wordBreak: 'break-word !important',
                        m: '0 !important',
                    },
                }}>
                    <SyntaxHighlighter
                        language={language}
                        wrapLongLines
                    >
                        {error.task_instance_stderr_log ||
                            'No stderr output found'}
                    </SyntaxHighlighter>
                </Box>
            </Box>
        );
    };

    // --- Loading state ---
    if (isLoading) {
        return <Skeleton variant="rectangular" height={200} />;
    }

    return (
        <>
            <Card
                elevation={0}
                sx={{
                    height: '100%',
                    border: '1px solid',
                    borderColor: hasErrors
                        ? 'error.light'
                        : 'divider',
                    borderLeft: '3px solid',
                    borderLeftColor: hasErrors
                        ? 'error.main'
                        : 'grey.400',
                    borderRadius: 2,
                    transition: 'all 0.2s ease-in-out',
                    '&:hover': { boxShadow: 3 },
                    display: 'flex',
                    flexDirection: 'column',
                }}
            >
                <CardContent
                    sx={{
                        p: 1.5,
                        '&:last-child': { pb: 1.5 },
                        display: 'flex',
                        flexDirection: 'column',
                        flex: 1,
                        minHeight: 0,
                    }}
                >
                    <Typography
                        variant="caption"
                        color={
                            hasErrors
                                ? 'error.main'
                                : 'text.secondary'
                        }
                        fontWeight="bold"
                        sx={{
                            textTransform: 'uppercase',
                            letterSpacing: 0.5,
                            mb: 0.5,
                        }}
                    >
                        Errors
                    </Typography>

                    {/* Metrics row */}
                    <Grid container spacing={1} sx={{ mb: 1 }}>
                        <Grid item xs={6}>
                            <Typography
                                variant="caption"
                                color="text.secondary"
                                fontWeight="medium"
                                sx={{
                                    textTransform: 'uppercase',
                                    letterSpacing: 0.5,
                                }}
                            >
                                Error Clusters
                            </Typography>
                            <Typography
                                variant="body2"
                                fontWeight="bold"
                                color={
                                    hasErrors
                                        ? 'error.main'
                                        : 'text.primary'
                                }
                                sx={{ mt: 0.5 }}
                            >
                                {errorClusterCount}
                            </Typography>
                        </Grid>
                        <Grid item xs={6}>
                            <Typography
                                variant="caption"
                                color="text.secondary"
                                fontWeight="medium"
                                sx={{
                                    textTransform: 'uppercase',
                                    letterSpacing: 0.5,
                                }}
                            >
                                Total Failures
                            </Typography>
                            <Typography
                                variant="body2"
                                fontWeight="bold"
                                color={
                                    hasErrors
                                        ? 'error.main'
                                        : 'text.primary'
                                }
                                sx={{ mt: 0.5 }}
                            >
                                {totalFailures}
                            </Typography>
                        </Grid>
                    </Grid>

                    {/* Scrollable error cluster list */}
                    {hasErrors ? (
                        <Box
                            sx={{
                                flex: 1,
                                maxHeight: maxListHeight,
                                overflow: 'auto',
                                border: '1px solid',
                                borderColor: 'grey.200',
                                borderRadius: 1,
                                bgcolor: 'grey.50',
                            }}
                        >
                            {errorLogs.map((cluster, idx) => {
                                const truncated =
                                    cluster.sample_error.length >
                                    80
                                        ? `...${cluster.sample_error.slice(-80)}`
                                        : cluster.sample_error;
                                const isActive =
                                    !!selectedInstanceIds &&
                                    selectedInstanceIds.size > 0 &&
                                    cluster.task_instance_ids.every(
                                        id =>
                                            selectedInstanceIds.has(
                                                id
                                            )
                                    ) &&
                                    cluster.task_instance_ids
                                        .length > 0;
                                return (
                                    <Box
                                        key={idx}
                                        sx={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            px: 1,
                                            py: 0.5,
                                            borderBottom:
                                                idx <
                                                errorLogs.length -
                                                    1
                                                    ? '1px solid'
                                                    : 'none',
                                            borderColor:
                                                'grey.200',
                                            bgcolor: isActive
                                                ? 'primary.50'
                                                : 'transparent',
                                            borderLeft: isActive
                                                ? '3px solid'
                                                : '3px solid transparent',
                                            borderLeftColor:
                                                isActive
                                                    ? 'primary.main'
                                                    : 'transparent',
                                            '&:hover': {
                                                bgcolor: isActive
                                                    ? 'primary.100'
                                                    : 'grey.100',
                                            },
                                        }}
                                    >
                                        <Button
                                            sx={{
                                                textTransform:
                                                    'none',
                                                textAlign: 'left',
                                                flex: 1,
                                                minWidth: 0,
                                                justifyContent:
                                                    'flex-start',
                                                px: 0.5,
                                                py: 0,
                                            }}
                                            size="small"
                                            onClick={() =>
                                                setErrorDetailIndex(
                                                    {
                                                        sample_index: 0,
                                                        sample_ids:
                                                            cluster.task_instance_ids,
                                                    }
                                                )
                                            }
                                        >
                                            <Typography
                                                variant="caption"
                                                noWrap
                                                sx={{
                                                    fontFamily:
                                                        'monospace',
                                                    fontSize:
                                                        '0.75rem',
                                                }}
                                            >
                                                {truncated}
                                            </Typography>
                                        </Button>
                                        <Chip
                                            label={
                                                cluster.group_instance_count
                                            }
                                            size="small"
                                            color="error"
                                            variant="outlined"
                                            sx={{
                                                mx: 0.5,
                                                height: 20,
                                                fontSize:
                                                    '0.7rem',
                                            }}
                                        />
                                        {onFilterByInstanceIds && (
                                            <HtmlTooltip
                                                title={
                                                    isActive
                                                        ? 'Clear filter'
                                                        : 'Select instances with this error on scatter plot'
                                                }
                                                arrow
                                                placement="right"
                                            >
                                                <IconButton
                                                    size="small"
                                                    color={
                                                        isActive
                                                            ? 'primary'
                                                            : 'default'
                                                    }
                                                    onClick={() =>
                                                        onFilterByInstanceIds(
                                                            cluster.task_instance_ids
                                                        )
                                                    }
                                                    sx={{
                                                        p: 0.25,
                                                    }}
                                                >
                                                    <FilterAltIcon
                                                        sx={{
                                                            fontSize: 16,
                                                        }}
                                                    />
                                                </IconButton>
                                            </HtmlTooltip>
                                        )}
                                    </Box>
                                );
                            })}
                        </Box>
                    ) : (
                        <Box
                            sx={{
                                bgcolor: 'success.50',
                                p: 1,
                                borderRadius: 1,
                                border: '1px solid',
                                borderColor: 'success.200',
                            }}
                        >
                            <Typography
                                variant="body2"
                                color="success.main"
                                fontWeight="medium"
                            >
                                No errors detected
                            </Typography>
                        </Box>
                    )}
                </CardContent>
            </Card>

            {/* Error detail drawer */}
            <Drawer
                anchor="right"
                open={errorDetailIndex !== null}
                onClose={() => setErrorDetailIndex(null)}
                PaperProps={{
                    sx: { width: { xs: '100%', md: 640 } },
                }}
            >
                {/* Header */}
                <Box sx={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    pt: 3,
                    px: 2,
                    pb: 1,
                }}>
                    <Typography variant="subtitle1" fontWeight="bold">
                        Error Sample: TI {currentTiID()}
                    </Typography>
                    <IconButton
                        onClick={() => setErrorDetailIndex(null)}
                        size="small"
                    >
                        <CloseIcon />
                    </IconButton>
                </Box>

                {/* Sample navigation */}
                <Box sx={{
                    display: 'flex',
                    alignItems: 'center',
                    px: 2,
                    pb: 1,
                }}>
                    <IconButton
                        onClick={previousSample}
                        size="small"
                        disabled={
                            errorDetailIndex !== null &&
                            errorDetailIndex.sample_index === 0
                        }
                    >
                        <NavigateBeforeIcon />
                    </IconButton>
                    <Typography variant="body2" sx={{ mx: 1 }}>
                        {errorDetailIndex
                            ? `${errorDetailIndex.sample_index + 1} of ${errorDetailIndex.sample_ids.length}`
                            : 'No error logs available'}
                    </Typography>
                    <IconButton
                        onClick={nextSample}
                        size="small"
                        disabled={
                            errorDetailIndex !== null &&
                            errorDetailIndex.sample_index ===
                                errorDetailIndex.sample_ids.length - 1
                        }
                    >
                        <NavigateNextIcon />
                    </IconButton>
                </Box>

                <Divider />

                {/* Scrollable body */}
                {drawerContent()}
            </Drawer>
        </>
    );
};

export default ErrorClustersCard;
