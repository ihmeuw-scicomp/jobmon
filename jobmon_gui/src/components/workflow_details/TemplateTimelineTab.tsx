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
import RefreshIcon from '@mui/icons-material/Refresh';
import SyncIcon from '@mui/icons-material/Sync';
import { useQuery } from '@tanstack/react-query';
import {
    getTemplateTimelineQueryFn,
    TemplateTimelineResponse,
} from '@jobmon_gui/queries/GetTaskConcurrency.ts';
import type { Layout, PlotMouseEvent, Data as PlotlyData } from 'plotly.js';
import { TEMPLATE_STATUS_COLORS } from '@jobmon_gui/constants/taskStatus';

const STATUS_ORDER = [
    'REGISTERED',
    'PENDING',
    'LAUNCHED',
    'RUNNING',
    'ERROR',
    'DONE',
] as const;

const STATUS_COLORS: Record<string, string> = {
    REGISTERED: '#757575',
    PENDING: TEMPLATE_STATUS_COLORS.PENDING,
    LAUNCHED: TEMPLATE_STATUS_COLORS.SCHEDULED,
    RUNNING: TEMPLATE_STATUS_COLORS.RUNNING,
    ERROR: TEMPLATE_STATUS_COLORS.FATAL,
    DONE: TEMPLATE_STATUS_COLORS.DONE,
};

const STATUS_DISPLAY: Record<string, string> = {
    REGISTERED: 'Registered',
    PENDING: 'Pending',
    LAUNCHED: 'Scheduled',
    RUNNING: 'Running',
    ERROR: 'Error',
    DONE: 'Done',
};

const MIN_CHART_HEIGHT = 300;
const BAR_HEIGHT = 28;
const BAR_GAP = 8;

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
    const [syncScales, setSyncScales] = useState(false);
    const [uiRevision, setUiRevision] = useState(0);
    const graphDivRef = useRef<
        HTMLDivElement & {
            _fullLayout?: {
                xaxis?: {
                    range?: [number | string, number | string];
                };
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
        queryKey: [
            'workflow_details',
            'template_timeline',
            workflowId,
        ],
        queryFn: getTemplateTimelineQueryFn,
        refetchOnMount: true,
        refetchOnWindowFocus: false,
        enabled: !!workflowId,
    });

    const handleResetZoom = useCallback(() => {
        setUiRevision(prev => prev + 1);
    }, []);

    const handlePlotInitialized = useCallback(
        (
            _figure: object,
            graphDiv: typeof graphDivRef.current
        ) => {
            graphDivRef.current = graphDiv;
        },
        []
    );

    const rangeValueToMs = (value: number | string): number =>
        typeof value === 'number'
            ? value
            : new Date(value).getTime();

    const handleZoom = useCallback((scaleFactor: number) => {
        const gd = graphDivRef.current;
        if (!gd?._fullLayout?.xaxis?.range) return;
        const [rangeStart, rangeEnd] = gd._fullLayout.xaxis.range;
        const startMs = rangeValueToMs(rangeStart);
        const endMs = rangeValueToMs(rangeEnd);
        const duration = endMs - startMs;
        const center = startMs + duration / 2;
        const newDuration = duration * scaleFactor;
        Plotly.relayout(gd, {
            'xaxis.range': [
                center - newDuration / 2,
                center + newDuration / 2,
            ],
        });
    }, []);

    const handleZoomIn = useCallback(
        () => handleZoom(0.5),
        [handleZoom]
    );
    const handleZoomOut = useCallback(
        () => handleZoom(2),
        [handleZoom]
    );

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

        const resultTraces: PlotlyData[] = [];
        const legendShown = new Set<string>();
        const ROW_HEIGHT = 0.7;

        const globalMaxTasks = syncScales
            ? Math.max(...templates.map(t => t.total_tasks))
            : 0;

        // One trace per (template, status) pair instead of
        // one trace per (template, timestamp, status).
        for (let tIdx = 0; tIdx < numTemplates; tIdx++) {
            const tmpl = templates[tIdx];
            const isDimmed =
                highlightSet &&
                !highlightSet.has(tmpl.template_name);
            const yCenter = numTemplates - tIdx;

            if (tmpl.timestamps.length === 0) continue;

            // Pre-compute per-timestamp totals and hover text
            const totals: number[] = [];
            const hoverTexts: string[] = [];
            for (
                let i = 0;
                i < tmpl.timestamps.length - 1;
                i++
            ) {
                let total = 0;
                for (const status of STATUS_ORDER) {
                    total +=
                        tmpl.series[status]?.[i] ?? 0;
                }
                totals.push(total);

                const hoverLines = STATUS_ORDER.map(s => {
                    const val =
                        tmpl.series[s]?.[i] ?? 0;
                    if (val === 0) return null;
                    return `${STATUS_DISPLAY[s]}: ${val}`;
                }).filter(Boolean);
                hoverTexts.push(
                    `<b>${tmpl.template_name}</b><br>` +
                    hoverLines.join('<br>') +
                    '<extra></extra>'
                );
            }

            // Collect segments per status into arrays
            for (const status of STATUS_ORDER) {
                const yArr: number[] = [];
                const xArr: number[] = [];
                const baseArr: number[] = [];
                const widthArr: number[] = [];
                const colorArr: string[] = [];
                const customArr: string[] = [];
                const hoverArr: string[] = [];

                const baseColor =
                    STATUS_COLORS[status] ?? '#999';
                const alpha = isDimmed ? 0.2 : 0.85;
                const rgba = hexToRgba(baseColor, alpha);

                for (
                    let i = 0;
                    i < tmpl.timestamps.length - 1;
                    i++
                ) {
                    const total = totals[i];
                    if (total === 0) continue;

                    const count =
                        tmpl.series[status]?.[i] ?? 0;
                    if (count === 0) continue;

                    const t0ms = new Date(
                        tmpl.timestamps[i]
                    ).getTime();
                    const t1ms = new Date(
                        tmpl.timestamps[i + 1]
                    ).getTime();

                    const rowScale =
                        syncScales && globalMaxTasks > 0
                            ? (total / globalMaxTasks) *
                              ROW_HEIGHT
                            : ROW_HEIGHT;

                    // Compute y offset by summing heights
                    // of statuses that appear below this one
                    let yOffset = yCenter - rowScale / 2;
                    for (const s of STATUS_ORDER) {
                        if (s === status) break;
                        const c =
                            tmpl.series[s]?.[i] ?? 0;
                        if (c > 0) {
                            yOffset +=
                                (c / total) * rowScale;
                        }
                    }

                    const fraction = count / total;
                    const barH = fraction * rowScale;

                    yArr.push(yOffset + barH / 2);
                    xArr.push(t1ms - t0ms);
                    baseArr.push(t0ms);
                    widthArr.push(barH);
                    colorArr.push(rgba);
                    customArr.push(tmpl.template_name);
                    hoverArr.push(hoverTexts[i]);
                }

                if (xArr.length === 0) continue;

                const showLegend =
                    !legendShown.has(status);
                if (showLegend) legendShown.add(status);

                resultTraces.push({
                    type: 'bar',
                    orientation: 'h',
                    y: yArr,
                    x: xArr,
                    base: baseArr,
                    marker: {
                        color: colorArr,
                        line: { width: 0 },
                    },
                    width: widthArr,
                    name:
                        STATUS_DISPLAY[status] ?? status,
                    legendgroup: status,
                    showlegend: showLegend,
                    customdata: customArr,
                    hovertemplate: hoverArr,
                } as PlotlyData);
            }
        }

        // Y-axis tick labels = template names
        const yTickVals = templates.map(
            (_, i) => numTemplates - i
        );
        const yTickText = templates.map(t => {
            const name = t.template_name;
            return name.length > 30
                ? name.substring(0, 27) + '...'
                : name;
        });

        const chartHeight = Math.max(
            MIN_CHART_HEIGHT,
            numTemplates * (BAR_HEIGHT + BAR_GAP) + 100
        );

        const layoutObj: Partial<Layout> = {
            autosize: true,
            height: chartHeight,
            barmode: 'stack',
            showlegend: true,
            legend: {
                orientation: 'h',
                y: 1.02,
                yanchor: 'bottom',
                x: 0.5,
                xanchor: 'center',
                font: { size: 11 },
            },
            margin: { l: 160, r: 60, t: 30, b: 50, pad: 0 },
            hovermode: 'closest' as const,
            hoverlabel: {
                bgcolor: 'white',
                bordercolor: '#ccc',
                font: { color: '#333', size: 12 },
            },
            dragmode: dragMode,
            uirevision: uiRevision,
            plot_bgcolor: 'rgba(0,0,0,0)',
            paper_bgcolor: 'rgba(0,0,0,0)',
            xaxis: {
                type: 'date',
                title: {
                    text: 'Time',
                    font: {
                        size: 12,
                        family: 'Roboto, sans-serif',
                    },
                },
                tickfont: { size: 10 },
                gridcolor: 'rgba(0,0,0,0.08)',
                autorange: true,
            },
            yaxis: {
                tickvals: yTickVals,
                ticktext: yTickText,
                tickfont: {
                    size: 11,
                    family: 'Roboto, sans-serif',
                },
                autorange: true,
                fixedrange: true,
                showgrid: true,
                gridcolor: 'rgba(0,0,0,0.04)',
                zeroline: false,
            },
        };

        // Annotations: total tasks on right side
        const annotations = templates.map(
            (
                tmpl: TemplateTimelineResponse['templates'][0],
                tIdx: number
            ) => ({
                text: `${tmpl.total_tasks.toLocaleString()}`,
                xref: 'paper' as const,
                yref: 'y' as const,
                x: 1.005,
                y: numTemplates - tIdx,
                showarrow: false,
                font: {
                    size: 10,
                    color: 'rgba(0,0,0,0.5)',
                    family: 'Roboto, sans-serif',
                },
                xanchor: 'left' as const,
                yanchor: 'middle' as const,
            })
        );
        layoutObj.annotations = annotations;

        return { traces: resultTraces, layout: layoutObj };
    }, [
        timelineData,
        highlightedTemplates,
        dragMode,
        uiRevision,
        syncScales,
    ]);

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
                    Error loading timeline:{' '}
                    {error instanceof Error
                        ? error.message
                        : 'Unknown error'}
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
                                dragMode === 'zoom'
                                    ? 'contained'
                                    : 'outlined'
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
                                dragMode === 'pan'
                                    ? 'contained'
                                    : 'outlined'
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
                            ? 'Sync on — bar height shows absolute task count'
                            : 'Sync off — all rows same height'
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
                    style={{
                        width: '100%',
                        minHeight: MIN_CHART_HEIGHT,
                    }}
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
