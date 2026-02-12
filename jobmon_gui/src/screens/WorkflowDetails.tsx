import React, { useState, useEffect, useCallback, useMemo } from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import WorkflowHeader from '@jobmon_gui/components/workflow_details/WorkflowHeader';
import Box from '@mui/material/Box';
import {
    CircularProgress,
    IconButton,
    Tooltip,
    useMediaQuery,
    useTheme,
} from '@mui/material';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import Typography from '@mui/material/Typography';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PauseIcon from '@mui/icons-material/Pause';
import RefreshIcon from '@mui/icons-material/Refresh';
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
import TemplateDetailPanel from '@jobmon_gui/components/workflow_details/TemplateDetailPanel.tsx';
import WorkflowSummaryPanel from '@jobmon_gui/components/workflow_details/WorkflowSummaryPanel.tsx';
import WorkflowManagePanel from '@jobmon_gui/components/workflow_details/WorkflowManagePanel.tsx';
import { getWorkflowFiltersForNavigation } from '@jobmon_gui/utils/workflowFilterPersistence';
import { TTStatus } from '@jobmon_gui/types/TaskTemplateStatus';
import { compare } from 'compare-versions';

type RightPanelView = 'summary' | 'template' | 'manage';

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
    const [rightPanelView, setRightPanelView] =
        useState<RightPanelView>('summary');

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

    // Lifted wfDetails query — shared by header + summary panel + manage panel
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

    if (wfTTStatus.isLoading) {
        return <CircularProgress />;
    }
    if (wfTTStatus.isError) {
        return (
            <Typography>
                Error loading workflow task template details. Please refresh and
                try again.
            </Typography>
        );
    }

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

    const selectedTemplateData = (() => {
        if (!selectedTemplateName || !wfTTStatus.data) return null;
        return (
            Object.values(wfTTStatus.data).find(
                tt => tt.name === selectedTemplateName
            ) ?? null
        );
    })();

    const handleTemplateSelect = (name: string) => {
        setSelectedTemplateName(name);
        setRightPanelView('template');
    };

    const handleTemplateBack = () => {
        setSelectedTemplateName(null);
        setRightPanelView('summary');
    };

    const handleManageClick = () => {
        setRightPanelView('manage');
    };

    const handleManageBack = () => {
        setRightPanelView('summary');
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

            {/* Two-panel area — takes remaining space, shares with timeline */}
            <Box
                display="flex"
                flexDirection={isSmall ? 'column' : 'row'}
                sx={{
                    flex: '1 1 45%',
                    minHeight: 250,
                    overflow: 'hidden',
                }}
            >
                {/* LEFT: DAG */}
                <Box
                    sx={{
                        flex: isSmall ? '1 1 auto' : '0 0 60%',
                        height: isSmall ? 400 : '100%',
                        borderRight: isSmall ? 'none' : '1px solid',
                        borderBottom: isSmall ? '1px solid' : 'none',
                        borderColor: 'divider',
                    }}
                >
                    <WorkflowDAG
                        workflowId={workflowId}
                        ttStatusByName={ttStatusByName}
                        selectedTemplateName={selectedTemplateName}
                        hoveredTemplateName={hoveredTemplateName}
                        onTemplateSelect={handleTemplateSelect}
                        onTemplateHover={setHoveredTemplateName}
                        height="100%"
                    />
                </Box>

                {/* RIGHT: Detail panel */}
                <Box
                    sx={{
                        flex: '1 1 40%',
                        height: isSmall ? 'auto' : '100%',
                        overflow: 'auto',
                    }}
                >
                    {rightPanelView === 'template' && selectedTemplateData ? (
                        <TemplateDetailPanel
                            workflowId={workflowId}
                            templateData={selectedTemplateData}
                            onBack={handleTemplateBack}
                            onNavigate={() =>
                                resetStoresAndNavigate(selectedTemplateData.id)
                            }
                            disabled={disabled}
                        />
                    ) : rightPanelView === 'manage' ? (
                        <WorkflowManagePanel
                            wfId={workflowId!}
                            workflowDetails={wfDetails.data}
                            onBack={handleManageBack}
                            onClose={handleManageClose}
                        />
                    ) : (
                        <WorkflowSummaryPanel
                            ttData={wfTTStatus.data}
                            hoveredTemplateName={hoveredTemplateName}
                            onTemplateSelect={handleTemplateSelect}
                            onTemplateHover={setHoveredTemplateName}
                            onPrefetch={prefetchTemplateData}
                            workflowDetails={wfDetails.data}
                            onManageClick={handleManageClick}
                        />
                    )}
                </Box>
            </Box>

            {/* BOTTOM: Concurrency timeline */}
            <Box
                sx={{
                    borderTop: 1,
                    borderColor: 'divider',
                    flex: '1 1 45%',
                    minHeight: 0,
                    overflow: 'auto',
                }}
            >
                <TaskConcurrencyTab
                    workflowId={workflowId}
                    highlightedTemplates={
                        selectedTemplateName
                            ? [selectedTemplateName]
                            : undefined
                    }
                />
            </Box>
        </Box>
    );
}

export default WorkflowDetails;
