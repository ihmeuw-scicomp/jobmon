import React, { useState, useMemo, useRef, useCallback } from 'react';
import Plotly from 'plotly.js-dist';
import createPlotlyComponent from 'react-plotly.js/factory';

const Plot = createPlotlyComponent(Plotly);

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';
import CircularProgress from '@mui/material/CircularProgress';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import ZoomInIcon from '@mui/icons-material/ZoomIn';
import PanToolIcon from '@mui/icons-material/PanTool';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import SyncIcon from '@mui/icons-material/Sync';
import RefreshIcon from '@mui/icons-material/Refresh';
import { useQuery } from '@tanstack/react-query';
import {
    getTemplateTimelineQueryFn,
    TemplateTimelineResponse,
} from '@jobmon_gui/queries/GetTaskConcurrency.ts';
import type { Layout, PlotMouseEvent, Data as PlotlyData } from 'plotly.js';

// Status display order (bottom to top in stacked area)
const STATUS_ORDER = [
    'REGISTERED',
    'PENDING',
    'LAUNCHED',
    'RUNNING',
    'ERROR',
    'DONE',
];

const STATUS_COLORS: Record<string, string> = {
    REGISTERED: '#999999',
    PENDING: '#e69f00',
    LAUNCHED: '#f0e442',
    RUNNING: '#0072b2',
    ERROR: '#d55e00',
    DONE: '#009e73',
};

const STATUS_DISPLAY_LABEL: Record<string, string> = {
    REGISTERED: 'REGISTERED',
    PENDING: 'PENDING',
    LAUNCHED: 'SCHEDULED',
    RUNNING: 'RUNNING',
    ERROR: 'ERROR',
    DONE: 'DONE',
};

const MIN_CHART_HEIGHT = 300;
const SUBPLOT_HEIGHT = 80;
const SUBPLOT_GAP = 0.04;

function hexToRgba(hex: string, alpha: number): string {
    const h = hex.replace('#', '');
    const r = parseInt(h.substring(0, 2), 16);
    const g = parseInt(h.substring(2, 4), 16);
    const b = parseInt(h.substring(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

interface TemplateTimelineTabProps {
    workflowId: string | number | undefined;
    highlightedTemplates?: string[];
    onTemplateClick?: (templateName: string) => void;
}

export default function TemplateTimelineTab({
    workflowId,
    highlightedTemplates,
    onTemplateClick,
}: TemplateTimelineTabProps) {
    const [dragMode, setDragMode] = useState<'zoom' | 'pan'>('zoom');
    const [syncScales, setSyncScales] = useState(true);
    const [uiRevision, setUiRevision] = useState(0);
    const graphDivRef = useRef<
        HTMLDivElement & {
            _fullLayout?: {
                xaxis?: { range?: [number | string, number | string] };
            };
        }
    >(null);

    const {
        data: timelineData,
        isLoading,
        isError,
        error,
        refetch,
    } = useQuery({
        queryKey: ['workflow_details', 'template_timeline', workflowId],
        queryFn: getTemplateTimelineQueryFn,
        refetchOnMount: true,
        refetchOnWindowFocus: false,
        enabled: !!workflowId,
    });

    const handleResetZoom = useCallback(() => {
        setUiRevision(prev => prev + 1);
    }, []);

    const handlePlotInitialized = useCallback(
        (_figure: object, graphDiv: typeof graphDivRef.current) => {
            graphDivRef.current = graphDiv;
        },
        []
    );

    const rangeValueToMs = (value: number | string): number =>
        typeof value === 'number' ? value : new Date(value).getTime();

    const handleZoom = useCallback((scaleFactor: number) => {
        const gd = graphDivRef.current;
        if (!gd?._fullLayout?.xaxis?.range) return;

        const [rangeStart, rangeEnd] = gd._fullLayout.xaxis.range;
        const startMs = rangeValueToMs(rangeStart);
        const endMs = rangeValueToMs(rangeEnd);

        const duration = endMs - startMs;
        const center = startMs + duration / 2;
        const newDuration = duration * scaleFactor;

        const newStartMs = center - newDuration / 2;
        const newEndMs = center + newDuration / 2;

        Plotly.relayout(gd, { 'xaxis.range': [newStartMs, newEndMs] });
    }, []);

    const handleZoomIn = useCallback(() => handleZoom(0.5), [handleZoom]);
    const handleZoomOut = useCallback(() => handleZoom(2), [handleZoom]);

    const handlePlotClick = useCallback(
        (event: Readonly<PlotMouseEvent>) => {
            if (!onTemplateClick || !event.points?.length) return;
            const point = event.points[0];
            const templateName = (
                point as { customdata?: unknown }
            ).customdata;
            if (typeof templateName === 'string') {
                onTemplateClick(templateName);
            }
        },
        [onTemplateClick]
    );

    // Build Plotly traces — one subplot per template, each a
    // normalized (0-100%) stacked area showing status proportions.
    // Each template has its own timestamps array (event-driven,
    // no bucketing), so x values are per-template.
    const { traces, layout } = useMemo(() => {
        if (!timelineData?.templates?.length) {
            return {
                traces: [] as PlotlyData[],
                layout: {} as Partial<Layout>,
            };
        }

        const { templates } = timelineData;
        const numTemplates = templates.length;
        const highlightSet = highlightedTemplates
            ? new Set(highlightedTemplates)
            : null;

        // Pre-compute which statuses have data across ALL templates
        // so the legend shows every active status globally.
        const globalActiveStatuses = new Set<string>();
        for (const tmpl of templates) {
            for (const status of STATUS_ORDER) {
                const vals = tmpl.series[status];
                if (vals?.some((v: number) => v > 0)) {
                    globalActiveStatuses.add(status);
                }
            }
        }

        const resultTraces: PlotlyData[] = [];

        // When sync scales is on, use the same y-axis range across
        // all subplots so areas are visually comparable.
        const globalMaxTasks = syncScales
            ? Math.max(...templates.map(t => t.total_tasks))
            : 0;

        // Compute subplot domain ranges (top to bottom = first to last)
        const totalGap = SUBPLOT_GAP * (numTemplates - 1);
        const availableHeight = 1 - totalGap;
        const subplotHeight = availableHeight / numTemplates;

        // Build layout with shared x-axis and per-template y-axes
        const layoutObj: Record<string, unknown> = {
            autosize: true,
            height: Math.max(
                MIN_CHART_HEIGHT,
                numTemplates * SUBPLOT_HEIGHT + 80
            ),
            showlegend: true,
            legend: {
                orientation: 'h',
                y: 1.02,
                yanchor: 'bottom',
                x: 0.5,
                xanchor: 'center',
            },
            margin: { l: 100, r: 60, t: 30, b: 50, pad: 0 },
            hovermode: 'x unified' as const,
            hoverlabel: {
                bgcolor: 'white',
                bordercolor: '#ccc',
                font: { color: '#333', size: 12 },
            },
            dragmode: dragMode,
            uirevision: uiRevision,
            plot_bgcolor: 'rgba(0,0,0,0)',
            paper_bgcolor: 'rgba(0,0,0,0)',
        };

        // x-axis: shared, anchored to bottom subplot
        const bottomYKey = numTemplates === 1 ? 'y' : `y${numTemplates}`;
        layoutObj.xaxis = {
            type: 'date',
            title: {
                text: 'Time',
                font: { size: 12, family: 'Roboto, sans-serif' },
            },
            tickfont: { size: 10 },
            gridcolor: 'rgba(0,0,0,0.08)',
            anchor: bottomYKey,
            autorange: true,
        };

        // Per-template: create y-axis and traces
        for (let tIdx = 0; tIdx < numTemplates; tIdx++) {
            const tmpl = templates[tIdx];
            const isDimmed =
                highlightSet && !highlightSet.has(tmpl.template_name);

            // Domain: top-down (first template at top)
            const domainTop = 1 - tIdx * (subplotHeight + SUBPLOT_GAP);
            const domainBottom = domainTop - subplotHeight;

            const yAxisKey = tIdx === 0 ? 'yaxis' : `yaxis${tIdx + 1}`;
            const yRef = tIdx === 0 ? 'y' : `y${tIdx + 1}`;

            layoutObj[yAxisKey] = {
                domain: [Math.max(0, domainBottom), domainTop],
                showticklabels: false,
                showgrid: false,
                zeroline: false,
                fixedrange: true,
                ...(syncScales
                    ? { range: [0, globalMaxTasks * 1.05] }
                    : {}),
                title: {
                    text: tmpl.template_name,
                    font: { size: 11, family: 'Roboto, sans-serif' },
                    standoff: 5,
                },
            };

            // Create stacked area traces (one per status).
            // Each template has its own timestamps array — use it as x.
            // When sync scales is off, groupnorm:'percent' normalizes
            // each timestamp to 100%.  When on, raw counts are used.
            for (let sIdx = 0; sIdx < STATUS_ORDER.length; sIdx++) {
                const status = STATUS_ORDER[sIdx];
                const counts = tmpl.series[status] ?? tmpl.timestamps.map(() => 0);

                const hasAny = counts.some((c: number) => c > 0);
                const baseColor = STATUS_COLORS[status];
                const fillColor = isDimmed
                    ? hexToRgba(baseColor, 0.15)
                    : hexToRgba(baseColor, 0.7);
                const lineColor = isDimmed
                    ? hexToRgba(baseColor, 0.25)
                    : hexToRgba(baseColor, 0.9);

                resultTraces.push({
                    type: 'scatter',
                    x: tmpl.timestamps,
                    y: counts,
                    customdata: tmpl.timestamps.map(() => tmpl.template_name),
                    xaxis: 'x',
                    yaxis: yRef,
                    stackgroup: `tmpl_${tIdx}`,
                    groupnorm: !syncScales && sIdx === 0 ? 'percent' : '',
                    mode: 'none',
                    fillcolor: fillColor,
                    line: { width: 0.5, color: lineColor, shape: 'hv' },
                    name: STATUS_DISPLAY_LABEL[status] ?? status,
                    showlegend: tIdx === 0 && globalActiveStatuses.has(status),
                    legendgroup: status,
                    hovertemplate: hasAny
                        ? `${STATUS_DISPLAY_LABEL[status]}: %{y}<extra>${tmpl.template_name}</extra>`
                        : undefined,
                    hoverinfo: hasAny ? undefined : ('skip' as const),
                } as PlotlyData);
            }
        }

        // Annotations: template total tasks on the right side
        const annotations = templates.map(
            (tmpl: TemplateTimelineResponse['templates'][0], tIdx: number) => {
                const yRef = tIdx === 0 ? 'y' : `y${tIdx + 1}`;
                return {
                    text: `<b>${tmpl.total_tasks}</b> task${tmpl.total_tasks === 1 ? '' : 's'}`,
                    xref: 'paper' as const,
                    yref: yRef as string,
                    x: 1.005,
                    y: syncScales ? (globalMaxTasks * 1.05) / 2 : 50,
                    showarrow: false,
                    font: {
                        size: 9,
                        color: 'rgba(0,0,0,0.5)',
                        family: 'Roboto, sans-serif',
                    },
                    xanchor: 'left' as const,
                    yanchor: 'middle' as const,
                };
            }
        );
        layoutObj.annotations = annotations;

        return {
            traces: resultTraces,
            layout: layoutObj as Partial<Layout>,
        };
    }, [timelineData, highlightedTemplates, dragMode, uiRevision, syncScales]);

    // Loading state
    if (isLoading) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: MIN_CHART_HEIGHT,
                    gap: 2,
                }}
            >
                <CircularProgress />
                <Typography color="text.secondary">
                    Loading template timeline...
                </Typography>
            </Box>
        );
    }

    // Error state
    if (isError) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: MIN_CHART_HEIGHT,
                    gap: 2,
                }}
            >
                <Typography color="error">
                    Error loading template timeline:{' '}
                    {error instanceof Error ? error.message : 'Unknown error'}
                </Typography>
                <Button
                    variant="contained"
                    startIcon={<RefreshIcon />}
                    onClick={() => refetch()}
                >
                    Retry
                </Button>
            </Box>
        );
    }

    // Empty state
    if (!timelineData?.templates?.length) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: MIN_CHART_HEIGHT,
                    gap: 2,
                }}
            >
                <Typography color="text.secondary">
                    No template timeline data available.
                </Typography>
                <Button
                    variant="outlined"
                    startIcon={<RefreshIcon />}
                    onClick={() => refetch()}
                >
                    Refresh
                </Button>
            </Box>
        );
    }

    return (
        <Box
            sx={{
                display: 'flex',
                flexDirection: 'column',
                width: '100%',
                height: '100%',
            }}
        >
            {/* Toolbar */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    px: 1.5,
                    py: 0.5,
                    borderBottom: '1px solid',
                    borderColor: 'divider',
                }}
            >
                <ButtonGroup size="small" variant="outlined">
                    <Tooltip title="Zoom mode">
                        <Button
                            variant={
                                dragMode === 'zoom' ? 'contained' : 'outlined'
                            }
                            onClick={() => setDragMode('zoom')}
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <ZoomInIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Pan mode">
                        <Button
                            variant={
                                dragMode === 'pan' ? 'contained' : 'outlined'
                            }
                            onClick={() => setDragMode('pan')}
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <PanToolIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Zoom in">
                        <Button
                            onClick={handleZoomIn}
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <AddIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Zoom out">
                        <Button
                            onClick={handleZoomOut}
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <RemoveIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Reset zoom">
                        <Button
                            onClick={handleResetZoom}
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <RestartAltIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                </ButtonGroup>
                <Tooltip
                    title={
                        syncScales
                            ? 'Sync scales on — areas are comparable'
                            : 'Sync scales off — each row normalized to 100%'
                    }
                >
                    <Button
                        size="small"
                        variant={syncScales ? 'contained' : 'outlined'}
                        onClick={() => setSyncScales(v => !v)}
                        sx={{
                            minWidth: 0,
                            px: 0.75,
                            ml: 1,
                            textTransform: 'none',
                            fontSize: '0.75rem',
                        }}
                        startIcon={<SyncIcon fontSize="small" />}
                    >
                        Sync
                    </Button>
                </Tooltip>
            </Box>

            {/* Chart area */}
            <Box
                sx={{
                    flex: 1,
                    minHeight: 0,
                    minWidth: 0,
                    bgcolor: 'background.paper',
                    position: 'relative',
                    overflow: 'auto',
                }}
            >
                <Plot
                    data={traces}
                    layout={layout}
                    style={{ width: '100%', minHeight: MIN_CHART_HEIGHT }}
                    useResizeHandler={true}
                    onInitialized={handlePlotInitialized}
                    onUpdate={handlePlotInitialized}
                    onClick={handlePlotClick}
                    config={{
                        responsive: true,
                        displaylogo: false,
                        displayModeBar: false,
                        scrollZoom: true,
                    }}
                />
            </Box>
        </Box>
    );
}
