import React, { useRef, useEffect, memo, useState } from 'react';
import Plot from 'react-plotly.js';
import { Box, useMediaQuery, useTheme } from '@mui/material';
import {
    taskStatusMeta,
    getStatusColor,
} from '@jobmon_gui/constants/taskStatus';
import {
    calculateZoneBoundaries,
    calculateResourceZone,
} from './usageCalculations';
import { Layout, PlotMouseEvent, PlotSelectionEvent } from 'plotly.js';
import * as Plotly from 'plotly.js';

// Import the shared ScatterDataPoint type
import { ScatterDataPoint } from '@jobmon_gui/types/Usage';

interface RuntimeMemoryScatterPlotProps {
    data: ScatterDataPoint[]; // Expects already filtered data
    onTaskClick: (taskId: number | string) => void;
    medianRequestedRuntime?: number;
    medianRequestedMemory?: number;
    taskTemplateName?: string;
    onSelected?: (selectedData: ScatterDataPoint[]) => void; // Add selection callback
    showResourceZones?: boolean; // New prop for zone toggle
}

const RuntimeMemoryScatterPlot: React.FC<RuntimeMemoryScatterPlotProps> = ({
    data,
    onTaskClick,
    medianRequestedRuntime,
    medianRequestedMemory,
    taskTemplateName,
    onSelected,
    showResourceZones = false,
}) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const plotRef = useRef<Plot | null>(null);
    const selectionTimeoutRef = useRef<NodeJS.Timeout | null>(null);
    const lastSelectionRef = useRef<ScatterDataPoint[]>([]);
    const lastSelectionTimeRef = useRef<number>(0);
    const theme = useTheme();
    const isSmallScreen = useMediaQuery(theme.breakpoints.down('md'));

    // Track container dimensions with state
    const [containerDimensions, setContainerDimensions] = useState({
        width: 800,
        height: 600,
    });

    // Update container dimensions when mounted and on resize
    useEffect(() => {
        const updateDimensions = () => {
            if (containerRef.current) {
                const rect = containerRef.current.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) {
                    setContainerDimensions({
                        width: rect.width,
                        height: rect.height,
                    });
                }
            }
        };

        // Initial measurement
        updateDimensions();

        // Set up ResizeObserver for container size changes
        let resizeObserver: ResizeObserver | null = null;
        if (containerRef.current && window.ResizeObserver) {
            resizeObserver = new ResizeObserver(updateDimensions);
            resizeObserver.observe(containerRef.current);
        }

        return () => {
            if (resizeObserver) {
                resizeObserver.disconnect();
            }
        };
    }, []);

    // Cleanup timeout on unmount
    useEffect(() => {
        return () => {
            if (selectionTimeoutRef.current) {
                clearTimeout(selectionTimeoutRef.current);
            }
        };
    }, []);

    if (!data || data.length === 0) {
        // Render a message if no data after filtering, or if initial data was empty
        return <p>No data available for the current filter selection.</p>;
    }

    // Calculate plot bounds for zone boundaries
    const plotBounds = {
        xMin: Math.min(...data.map(d => d.runtime)) * 0.9,
        xMax: Math.max(...data.map(d => d.runtime)) * 1.1,
        yMin: Math.min(...data.map(d => d.memory)) * 0.9,
        yMax: Math.max(...data.map(d => d.memory)) * 1.1,
    };

    // Group data by status only for cleaner legend
    const groupedData = data.reduce(
        (acc, d) => {
            const status = String(d.status || 'UNKNOWN').toUpperCase();
            if (!acc[status]) {
                acc[status] = [];
            }
            acc[status].push(d);
            return acc;
        },
        {} as Record<string, ScatterDataPoint[]>
    );

    const plotTraces = Object.keys(groupedData)
        .sort((a, b) => {
            // Sort by status label
            const aLabel = taskStatusMeta[a]?.label || a;
            const bLabel = taskStatusMeta[b]?.label || b;
            return aLabel.localeCompare(bLabel);
        })
        .map(statusKey => {
            const statusData = groupedData[statusKey];
            const statusConfig =
                taskStatusMeta[statusKey] || taskStatusMeta.UNKNOWN;

            return {
                name: statusConfig.label,
                x: statusData.map(d => d.runtime),
                y: statusData.map(d => d.memory),
                mode: 'markers',
                type: 'scatter',
                visible: true,
                text: statusData.map(d => {
                    const pointStatusMeta =
                        taskStatusMeta[String(d.status).toUpperCase()] ||
                        taskStatusMeta.UNKNOWN;

                    // Generate simplified tooltip with actual requested resources
                    let tooltip = `<b>Task ID: ${d.task_id}</b> (${pointStatusMeta.label})<br>`;
                    
                    // Add task name if available
                    if (d.task_name) {
                        tooltip += `<b>Task Name:</b> ${d.task_name}<br>`;
                    }

                    // Runtime information
                    if (d.requestedRuntime) {
                        const runtimeUtil =
                            (d.runtime / d.requestedRuntime) * 100;
                        tooltip += `<b>Runtime:</b> ${d.runtime.toFixed(1)}s / ${d.requestedRuntime.toFixed(1)}s (${runtimeUtil.toFixed(0)}%)<br>`;
                    } else {
                        tooltip += `<b>Runtime:</b> ${d.runtime.toFixed(1)}s<br>`;
                    }

                    // Memory information
                    if (d.requestedMemory) {
                        const memoryUtil = (d.memory / d.requestedMemory) * 100;
                        tooltip += `<b>Memory:</b> ${d.memory.toFixed(1)} / ${d.requestedMemory.toFixed(1)} GiB (${memoryUtil.toFixed(0)}%)<br>`;
                    } else {
                        tooltip += `<b>Memory:</b> ${d.memory.toFixed(1)} GiB<br>`;
                    }

                    // Add efficiency summary if we have requested resources for this task
                    if (d.requestedRuntime && d.requestedMemory) {
                        const zoneData = calculateResourceZone(
                            d.runtime,
                            d.memory,
                            d.requestedRuntime,
                            d.requestedMemory
                        );
                        tooltip += `<b>Efficiency:</b> ${zoneData.zoneLabel}`;
                    }

                    return tooltip;
                }),
                hoverinfo: 'text',
                marker: {
                    color: getStatusColor(statusKey),
                    symbol: statusConfig.symbol,
                    size: 8,
                },
                selected: {
                    marker: {
                        color: getStatusColor(statusKey),
                        size: 12,
                        line: {
                            color: '#000000',
                            width: 2,
                        },
                    },
                },
                unselected: {
                    marker: {
                        opacity: 0.3,
                        size: 6,
                    },
                },
                customdata: statusData.map(d => ({
                    taskId: d.task_id,
                    traceKey: statusKey,
                })), // Add traceKey for identification
            };
        });

    const layout: Partial<Layout> = {
        xaxis: {
            title: {
                text: 'Runtime (s)',
                font: { size: 16, family: 'Roboto, sans-serif' },
            },
            tickfont: { size: 12 },
            gridcolor: 'rgba(0,0,0,0.1)',
            zerolinecolor: 'rgba(0,0,0,0.2)',
            type: 'linear',
        },
        yaxis: {
            title: {
                text: 'Max RSS (GiB)',
                font: { size: 16, family: 'Roboto, sans-serif' },
            },
            tickfont: { size: 12 },
            tickformat: '.2f',
            gridcolor: 'rgba(0,0,0,0.1)',
            zerolinecolor: 'rgba(0,0,0,0.2)',
            type: 'linear',
        },
        title: {
            text: taskTemplateName
                ? `Runtime vs. Memory Usage: ${taskTemplateName}`
                : 'Runtime vs. Memory Usage',
            font: {
                size: isSmallScreen ? 16 : 18,
                family: 'Roboto, sans-serif',
                color: '#1976d2',
            },
            x: 0.02,
            xanchor: 'left',
        },
        hovermode: 'closest',
        dragmode: 'zoom', // Default to zoom for navigation, users can switch to select
        selectdirection: 'any', // Allow selection in any direction
        width: containerDimensions.width,
        height: containerDimensions.height,
        autosize: false, // Disable autosize and use explicit dimensions
        showlegend: true,
        legend: isSmallScreen
            ? {
                orientation: 'h',
                x: 0.5,
                xanchor: 'center',
                y: -0.2,
                yanchor: 'top',
                bgcolor: 'rgba(255,255,255,0.95)',
                bordercolor: '#E0E0E0',
                borderwidth: 1,
                font: { size: 9 },
                itemsizing: 'constant',
                itemwidth: 30,
                itemclick: false,
                itemdoubleclick: false,
            }
            : {
                orientation: 'v',
                x: 1.02,
                xanchor: 'left',
                y: 1,
                yanchor: 'top',
                bgcolor: 'rgba(255,255,255,0.95)',
                bordercolor: '#E0E0E0',
                borderwidth: 1,
                font: { size: 10 },
                itemsizing: 'constant',
                itemwidth: 30,
                itemclick: false,
                itemdoubleclick: false,
            },
        margin: isSmallScreen
            ? {
                l: 50,
                r: 30,
                t: 40,
                b: 100, // Increased bottom margin for horizontal legend
                pad: 3,
            }
            : {
                l: 60,
                r: 120, // Reduced right margin for legend
                t: 50,
                b: 60,
                pad: 5,
            },
        plot_bgcolor: 'rgba(0,0,0,0)',
        paper_bgcolor: 'rgba(0,0,0,0)',
        shapes: [], // Initialize shapes array
        annotations: [], // Initialize annotations array
    };

    // Add resource efficiency zones if enabled
    if (showResourceZones && medianRequestedRuntime && medianRequestedMemory) {
        const zoneBoundaries = calculateZoneBoundaries(
            medianRequestedRuntime,
            medianRequestedMemory,
            plotBounds
        );
        layout.shapes = [...layout.shapes, ...zoneBoundaries.shapes];
        layout.annotations = [
            ...layout.annotations,
            ...zoneBoundaries.annotations,
        ];
    }

    const handleClick = (event: Readonly<PlotMouseEvent>) => {
        if (event.points && event.points.length > 0) {
            const point = event.points[0];
            const customData = point.customdata as
                | { taskId: number | string }
                | undefined;
            if (customData && customData.taskId) {
                onTaskClick(customData.taskId);
            }
        }
    };

    const handleSelected = (event: Readonly<PlotSelectionEvent>) => {
        if (!onSelected) return;

        // Clear any pending timeout
        if (selectionTimeoutRef.current) {
            clearTimeout(selectionTimeoutRef.current);
            selectionTimeoutRef.current = null;
        }

        if (event && event.points && event.points.length > 0) {
            // Extract selected data points from all traces
            const selectedData: ScatterDataPoint[] = [];

            event.points.forEach((point: Plotly.PlotDatum) => {
                const traceKey = point.customdata?.traceKey;
                const pointIndex = point.pointIndex;

                if (
                    traceKey &&
                    groupedData[traceKey] &&
                    groupedData[traceKey][pointIndex]
                ) {
                    const dataPoint = groupedData[traceKey][pointIndex];
                    selectedData.push(dataPoint);
                }
            });

            lastSelectionRef.current = selectedData;
            lastSelectionTimeRef.current = Date.now();
            onSelected(selectedData);
        } else {
            // No selection or selection cleared
            const timeSinceLastSelection =
                Date.now() - lastSelectionTimeRef.current;

            // Ignore clearing events that happen too soon after a selection (likely due to re-render)
            if (timeSinceLastSelection < 500) {
                return;
            }

            lastSelectionRef.current = [];
            onSelected([]);
        }
    };

    return (
        <Box sx={{ width: '100%', height: '100%', position: 'relative' }}>
            <div ref={containerRef} style={{ width: '100%', height: '100%' }}>
                <Plot
                    ref={plotRef}
                    key="scatter-plot" // Use static key to prevent remounting
                    data={plotTraces as Plotly.Data[]}
                    layout={layout}
                    onClick={handleClick}
                    onSelected={handleSelected}
                    onDeselect={() => {
                        // Add a small delay to prevent race conditions with selection
                        selectionTimeoutRef.current = setTimeout(() => {
                            if (onSelected) {
                                lastSelectionRef.current = [];
                                onSelected([]);
                            }
                        }, 100);
                    }}
                    style={{ width: '100%', height: '100%' }}
                    useResizeHandler={true}
                    config={{
                        responsive: true,
                        displaylogo: false,
                        modeBarButtonsToRemove: ['pan2d', 'autoScale2d'],
                        displayModeBar: true,
                        modeBarButtonsToAdd: ['select2d', 'lasso2d'],
                        staticPlot: false,
                        showTips: true, // Show tooltips for mode bar buttons
                        toImageButtonOptions: {
                            format: 'png',
                            filename: 'runtime_memory_analysis',
                            height: 600,
                            width: 1000,
                            scale: 2,
                        },
                    }}
                />
            </div>
        </Box>
    );
};

// Memoize the component to prevent unnecessary re-renders that clear selection
const arePropsEqual = (
    prevProps: RuntimeMemoryScatterPlotProps,
    nextProps: RuntimeMemoryScatterPlotProps
) => {
    // Only re-render if the actual plot data or configuration changes
    // Don't re-render when callbacks change (which happens when parent state updates)
    // Don't compare medians to prevent re-renders when selection changes median calculations
    return (
        prevProps.data === nextProps.data &&
        prevProps.taskTemplateName === nextProps.taskTemplateName &&
        prevProps.showResourceZones === nextProps.showResourceZones
        // Deliberately NOT comparing callback props (onSelected, onTaskClick, etc.)
        // Deliberately NOT comparing median props to prevent selection-clearing re-renders
    );
};

export default memo(RuntimeMemoryScatterPlot, arePropsEqual);
