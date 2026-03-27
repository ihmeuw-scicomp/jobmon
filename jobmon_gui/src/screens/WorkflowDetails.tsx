import React, { useState, useEffect, useCallback, useMemo } from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import WorkflowHeader from '@jobmon_gui/components/workflow_details/WorkflowHeader';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Dialog from '@mui/material/Dialog';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import useMediaQuery from '@mui/material/useMediaQuery';
import { useTheme } from '@mui/material/styles';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import Typography from '@mui/material/Typography';
import CloseIcon from '@mui/icons-material/Close';
import FullscreenIcon from '@mui/icons-material/Fullscreen';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PauseIcon from '@mui/icons-material/Pause';
import RefreshIcon from '@mui/icons-material/Refresh';
import BarChartIcon from '@mui/icons-material/BarChart';
import TimelineIcon from '@mui/icons-material/Timeline';
import { useTaskTableStore } from '@jobmon_gui/stores/TaskTable.ts';
import { getWorkflowTTStatusQueryFn } from '@jobmon_gui/queries/GetWorkflowTTStatus.ts';
import { getWorkflowDetailsQueryFn } from '@jobmon_gui/queries/GetWorkflowDetails.ts';
import { getWorkflowUsageQueryFn } from '@jobmon_gui/queries/GetWorkflowUsage.ts';
import { getClusteredErrorsFn } from '@jobmon_gui/queries/GetClusteredErrors.ts';
import {
    AppBreadcrumbs,
    BreadcrumbItem,
} from '@jobmon_gui/components/common/AppBreadcrumbs';
import WorkflowDAG from '@jobmon_gui/components/workflow_details/WorkflowDAG.tsx';
import TaskConcurrencyTab from '@jobmon_gui/components/workflow_details/TaskConcurrencyTab.tsx';
import TemplateTimelineTab from '@jobmon_gui/components/workflow_details/TemplateTimelineTab.tsx';
import TemplateDetailPanel from '@jobmon_gui/components/workflow_details/TemplateDetailPanel.tsx';
import WorkflowSummaryBar from '@jobmon_gui/components/workflow_details/WorkflowSummaryBar.tsx';
import TemplateListPanel from '@jobmon_gui/components/workflow_details/TemplateListPanel.tsx';
import WorkflowManagePanel from '@jobmon_gui/components/workflow_details/WorkflowManagePanel.tsx';
import ResizableSplitPane from '@jobmon_gui/components/common/ResizableSplitPane.tsx';
import { getWorkflowFiltersForNavigation } from '@jobmon_gui/utils/workflowFilterPersistence';
import { TTStatus } from '@jobmon_gui/types/TaskTemplateStatus';
import { compare } from 'compare-versions';

type LeftPanelView = 'list' | 'template' | 'manage';

function WorkflowDetails() {
    const { workflowId } = useParams();
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    const location = useLocation();
    const theme = useTheme();
    const isSmall = useMediaQuery(theme.breakpoints.down('md'));

    const [selectedTemplateName, setSelectedTemplateName] = useState<
        string | null
    >(null);
    const [hoveredTemplateName, setHoveredTemplateName] = useState<
        string | null
    >(null);
    const [autoRefresh, setAutoRefresh] = useState(false);
    const [leftPanelView, setLeftPanelView] = useState<LeftPanelView>('list');
    const [bottomPanelView, setBottomPanelView] = useState<
        'concurrency' | 'timeline'
    >('timeline');
    const [dagNodeCount, setDagNodeCount] = useState<number | null>(null);
    const [dagFullscreen, setDagFullscreen] = useState(false);

    // Page-level auto-refresh: invalidate all workflow queries periodically
    useEffect(() => {
        if (!autoRefresh || !workflowId) return;
        const interval = setInterval(() => {
            queryClient.invalidateQueries({
                queryKey: ['workflow_details'],
            });
        }, 30000);
        return () => clearInterval(interval);
    }, [autoRefresh, workflowId, queryClient]);

    const handleRefreshNow = useCallback(() => {
        queryClient.invalidateQueries({
            queryKey: ['workflow_details'],
        });
    }, [queryClient]);

    // Lifted wfDetails query — shared by header + summary bar + manage panel
    const wfDetails = useQuery({
        queryKey: ['workflow_details', 'details', workflowId],
        queryFn: getWorkflowDetailsQueryFn,
        staleTime: 60000,
        refetchOnMount: true,
    });

    const wfTTStatus = useQuery({
        queryKey: ['workflow_details', 'tt_status', workflowId],
        queryFn: getWorkflowTTStatusQueryFn,
        refetchOnMount: true,
        refetchOnWindowFocus: true,
    });

    const ttStatusByName = useMemo(() => {
        if (!wfTTStatus.data) return {} as Record<string, TTStatus>;
        return Object.fromEntries(
            Object.values(wfTTStatus.data).map(tt => [tt.name, tt])
        ) as Record<string, TTStatus>;
    }, [wfTTStatus.data]);

    const handleHomeClick = () => {
        const search = getWorkflowFiltersForNavigation(location.search);
        navigate({
            pathname: '/',
            search: search || '',
        });
    };

    const breadcrumbItems: BreadcrumbItem[] = [
        { label: 'Home', to: '/', onClick: handleHomeClick },
        { label: `Workflow ID ${workflowId}`, active: true },
    ];

    function normalizeVersion(version: string): string {
        return version
            .replace(/\.dev/, '-dev')
            .replace(/(\d+)rc(\d+)/, '$1-rc.$2');
    }

    const disabled = wfDetails.data
        ? !compare(
              normalizeVersion(wfDetails.data.wfr_jobmon_version),
              '3.3',
              '>'
          )
        : true;

    const handleManageClose = () => {
        queryClient.invalidateQueries({
            queryKey: ['workflow_details', 'tt_status', workflowId],
        });
    };

    const selectedTemplateData = useMemo(() => {
        if (!selectedTemplateName || !wfTTStatus.data) return null;
        return (
            Object.values(wfTTStatus.data).find(
                tt => tt.name === selectedTemplateName
            ) ?? null
        );
    }, [selectedTemplateName, wfTTStatus.data]);

    const handleTemplateSelect = (name: string) => {
        setSelectedTemplateName(name);
        setLeftPanelView('template');
    };

    const handleTemplateBack = () => {
        setSelectedTemplateName(null);
        setLeftPanelView('list');
    };

    const handleManageClick = () => {
        setLeftPanelView('manage');
    };

    const handleManageBack = () => {
        setLeftPanelView('list');
    };

    const resetStoresAndNavigate = (ttId: string | number) => {
        useTaskTableStore.setState({
            ...useTaskTableStore.getState(),
            filters: [],
        });
        navigate(`/workflow/${workflowId}/task_template/${ttId}`);
    };

    const prefetchTemplateData = (taskTemplate: {
        task_template_version_id: string | number;
        name: string;
    }) => {
        void queryClient.prefetchQuery({
            queryKey: [
                'workflow_details',
                'usage',
                taskTemplate.task_template_version_id,
                workflowId,
            ],
            queryFn: getWorkflowUsageQueryFn,
        });
        void queryClient.prefetchQuery({
            queryKey: [
                'workflow_details',
                'clustered_errors',
                workflowId,
                taskTemplate.task_template_version_id,
            ],
            queryFn: getClusteredErrorsFn,
        });
    };

    const leftPanel = wfTTStatus.isLoading ? (
        <Box
            sx={{
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                height: '100%',
            }}
        >
            <CircularProgress size={28} />
        </Box>
    ) : wfTTStatus.isError ? (
        <Box sx={{ p: 2 }}>
            <Typography color="error">
                Error loading templates. Try refreshing.
            </Typography>
        </Box>
    ) : leftPanelView === 'template' && selectedTemplateData ? (
        <TemplateDetailPanel
            workflowId={workflowId}
            templateData={selectedTemplateData}
            onBack={handleTemplateBack}
            onNavigate={() => resetStoresAndNavigate(selectedTemplateData.id)}
            disabled={disabled}
        />
    ) : leftPanelView === 'manage' ? (
        <WorkflowManagePanel
            wfId={workflowId!}
            workflowDetails={wfDetails.data}
            onBack={handleManageBack}
            onClose={handleManageClose}
        />
    ) : (
        <TemplateListPanel
            ttData={wfTTStatus.data}
            hoveredTemplateName={hoveredTemplateName}
            onTemplateSelect={handleTemplateSelect}
            onTemplateHover={setHoveredTemplateName}
            onPrefetch={prefetchTemplateData}
        />
    );

    const handleDagNodeCount = useCallback((count: number) => {
        setDagNodeCount(count);
    }, []);

    // Auto-size: small DAGs get less space, large ones get more
    // 1-3 nodes → 25%, 4-8 → 35%, 9-15 → 45%, 16+ → 55%
    const dagPercent =
        dagNodeCount == null
            ? 35
            : dagNodeCount <= 3
              ? 25
              : dagNodeCount <= 8
                ? 35
                : dagNodeCount <= 15
                  ? 45
                  : 55;
    const leftPercent = 100 - dagPercent;

    const dagContent = (
        <WorkflowDAG
            workflowId={workflowId}
            ttStatusByName={ttStatusByName}
            selectedTemplateName={selectedTemplateName}
            hoveredTemplateName={hoveredTemplateName}
            onTemplateSelect={handleTemplateSelect}
            onTemplateHover={setHoveredTemplateName}
            onNodeCount={handleDagNodeCount}
            height="100%"
        />
    );

    const dagPanel = (
        <Box
            sx={{
                position: 'relative',
                height: '100%',
            }}
        >
            {dagContent}
            <Tooltip title="Fullscreen DAG">
                <IconButton
                    size="small"
                    onClick={() => setDagFullscreen(true)}
                    sx={{
                        position: 'absolute',
                        top: 4,
                        right: 4,
                        zIndex: 5,
                        backgroundColor: 'rgba(255,255,255,0.8)',
                        '&:hover': {
                            backgroundColor: 'rgba(255,255,255,0.95)',
                        },
                    }}
                >
                    <FullscreenIcon fontSize="small" />
                </IconButton>
            </Tooltip>
        </Box>
    );

    return (
        <Box
            sx={{
                display: 'flex',
                flexDirection: 'column',
                height: '100vh',
                overflow: 'hidden',
            }}
        >
            {/* Header row: Breadcrumbs + Workflow name + actions */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1.5,
                    flexShrink: 0,
                    px: 1,
                    py: 0.5,
                }}
            >
                <AppBreadcrumbs items={breadcrumbItems} />
                <WorkflowHeader wf_id={workflowId} wfDetails={wfDetails.data} />
                <Box sx={{ flex: 1 }} />
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.5,
                        flexShrink: 0,
                    }}
                >
                    <Tooltip title="Refresh all data">
                        <IconButton size="small" onClick={handleRefreshNow}>
                            <RefreshIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>
                    <Tooltip
                        title={
                            autoRefresh
                                ? 'Disable auto-refresh'
                                : 'Auto-refresh (30s)'
                        }
                    >
                        <IconButton
                            size="small"
                            onClick={() => setAutoRefresh(v => !v)}
                            color={autoRefresh ? 'primary' : 'default'}
                        >
                            {autoRefresh ? (
                                <PauseIcon fontSize="small" />
                            ) : (
                                <PlayArrowIcon fontSize="small" />
                            )}
                        </IconButton>
                    </Tooltip>
                </Box>
            </Box>

            {/* Workflow-level summary bar — full width */}
            <WorkflowSummaryBar
                workflowDetails={wfDetails.data}
                ttData={wfTTStatus.data}
                onManageClick={handleManageClick}
            />

            {/* Middle + Bottom with vertical resize */}
            <ResizableSplitPane
                orientation="vertical"
                leftPercent={50}
                minLeftPercent={20}
                maxLeftPercent={80}
                left={
                    isSmall ? (
                        <Box
                            sx={{
                                overflow: 'auto',
                                display: 'flex',
                                flexDirection: 'column',
                                height: '100%',
                            }}
                        >
                            <Box sx={{ flex: '1 1 auto' }}>{leftPanel}</Box>
                            <Box
                                sx={{
                                    height: 400,
                                    flexShrink: 0,
                                    borderTop: '1px solid',
                                    borderColor: 'divider',
                                }}
                            >
                                {dagPanel}
                            </Box>
                        </Box>
                    ) : (
                        <ResizableSplitPane
                            left={leftPanel}
                            right={dagPanel}
                            leftPercent={leftPercent}
                            minLeftPercent={25}
                            maxLeftPercent={80}
                        />
                    )
                }
                right={
                    <Box
                        sx={{
                            display: 'flex',
                            flexDirection: 'column',
                            height: '100%',
                        }}
                    >
                        <Box
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                px: 1,
                                py: 0.25,
                                borderBottom: '1px solid',
                                borderColor: 'divider',
                                flexShrink: 0,
                            }}
                        >
                            <ToggleButtonGroup
                                value={bottomPanelView}
                                exclusive
                                onChange={(_, val) => {
                                    if (val) setBottomPanelView(val);
                                }}
                                size="small"
                                sx={{
                                    '& .MuiToggleButton-root': {
                                        py: 0.25,
                                        px: 1,
                                        textTransform: 'none',
                                        fontSize: '0.75rem',
                                    },
                                }}
                            >
                                <ToggleButton value="concurrency">
                                    <Tooltip title="Task status counts over time">
                                        <Box
                                            sx={{
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: 0.5,
                                            }}
                                        >
                                            <BarChartIcon
                                                sx={{ fontSize: 16 }}
                                            />
                                            Status
                                        </Box>
                                    </Tooltip>
                                </ToggleButton>
                                <ToggleButton value="timeline">
                                    <Tooltip title="Template execution spans">
                                        <Box
                                            sx={{
                                                display: 'flex',
                                                alignItems: 'center',
                                                gap: 0.5,
                                            }}
                                        >
                                            <TimelineIcon
                                                sx={{ fontSize: 16 }}
                                            />
                                            Gantt
                                        </Box>
                                    </Tooltip>
                                </ToggleButton>
                            </ToggleButtonGroup>
                        </Box>
                        <Box sx={{ flex: 1, minHeight: 0, overflow: 'auto' }}>
                            {bottomPanelView === 'concurrency' ? (
                                <TaskConcurrencyTab
                                    workflowId={workflowId}
                                    highlightedTemplates={
                                        selectedTemplateName
                                            ? [selectedTemplateName]
                                            : undefined
                                    }
                                />
                            ) : (
                                <TemplateTimelineTab
                                    workflowId={workflowId}
                                    highlightedTemplates={
                                        selectedTemplateName
                                            ? [selectedTemplateName]
                                            : undefined
                                    }
                                    onTemplateClick={handleTemplateSelect}
                                />
                            )}
                        </Box>
                    </Box>
                }
            />

            {/* Fullscreen DAG dialog */}
            {dagFullscreen && (
                <Dialog open onClose={() => setDagFullscreen(false)} fullScreen>
                    <Box
                        sx={{
                            display: 'flex',
                            flexDirection: 'column',
                            height: '100vh',
                        }}
                    >
                        <Box
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                px: 1,
                                py: 0.5,
                                borderBottom: '1px solid',
                                borderColor: 'divider',
                                flexShrink: 0,
                            }}
                        >
                            <Typography variant="subtitle2" sx={{ flex: 1 }}>
                                Workflow {workflowId} — DAG
                            </Typography>
                            <Tooltip title="Close fullscreen">
                                <IconButton
                                    size="small"
                                    onClick={() => setDagFullscreen(false)}
                                >
                                    <CloseIcon fontSize="small" />
                                </IconButton>
                            </Tooltip>
                        </Box>
                        <Box sx={{ flex: 1, minHeight: 0 }}>{dagContent}</Box>
                    </Box>
                </Dialog>
            )}
        </Box>
    );
}

export default WorkflowDetails;
