import React, { useState, useMemo, useRef, useCallback } from 'react';
import Plotly from 'plotly.js-dist';
import createPlotlyComponent from 'react-plotly.js/factory';

const Plot = createPlotlyComponent(Plotly);
import {
    Box,
    Button,
    ButtonGroup,
    CircularProgress,
    Divider,
    FormControl,
    IconButton,
    InputLabel,
    MenuItem,
    Popover,
    Select,
    Tooltip,
    Typography,
    useMediaQuery,
    useTheme,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import DownloadIcon from '@mui/icons-material/Download';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import PauseIcon from '@mui/icons-material/Pause';
import CalendarTodayIcon from '@mui/icons-material/CalendarToday';
import ZoomInIcon from '@mui/icons-material/ZoomIn';
import PanToolIcon from '@mui/icons-material/PanTool';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { DateTimePicker } from '@mui/x-date-pickers/DateTimePicker';
import { useQuery } from '@tanstack/react-query';
import {
    getTaskConcurrencyQueryFn,
    getWorkflowTaskTemplatesQueryFn,
} from '@jobmon_gui/queries/GetTaskConcurrency.ts';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import type { Layout, PlotRelayoutEvent, Data as PlotlyData } from 'plotly.js';

dayjs.extend(utc);

// Colors for status categories - semantically meaningful
// Using darker line colors for better contrast on buttons and chart
const STATUS_COLORS: Record<string, { line: string; fill: string; rgb: [number, number, number] }> = {
    PENDING: { line: '#b8960a', fill: 'rgba(255, 215, 0, 0.3)', rgb: [184, 150, 10] }, // dark gold - waiting
    LAUNCHED: { line: '#f57c00', fill: 'rgba(255, 167, 38, 0.4)', rgb: [245, 124, 0] }, // amber/orange - submitted
    RUNNING: { line: '#1565c0', fill: 'rgba(25, 118, 210, 0.4)', rgb: [21, 101, 192] }, // dark blue - active
    ERROR: { line: '#b71c1c', fill: 'rgba(183, 28, 28, 0.4)', rgb: [183, 28, 28] }, // deep red - error
    DONE: { line: '#2e7d32', fill: 'rgba(76, 175, 80, 0.4)', rgb: [46, 125, 50] }, // dark green - completed
};

// Order for display: PENDING first (waiting), LAUNCHED (submitted), RUNNING (active), ERROR (failed), DONE (completed)
const STATUS_DISPLAY_ORDER = ['PENDING', 'LAUNCHED', 'RUNNING', 'ERROR', 'DONE'];

// Convert RGB to HSL
function rgbToHsl(r: number, g: number, b: number): [number, number, number] {
    r /= 255; g /= 255; b /= 255;
    const max = Math.max(r, g, b), min = Math.min(r, g, b);
    let h = 0, s = 0;
    const l = (max + min) / 2;

    if (max !== min) {
        const d = max - min;
        s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
        switch (max) {
            case r: h = ((g - b) / d + (g < b ? 6 : 0)) / 6; break;
            case g: h = ((b - r) / d + 2) / 6; break;
            case b: h = ((r - g) / d + 4) / 6; break;
        }
    }
    return [h * 360, s * 100, l * 100];
}

// Convert HSL to RGB
function hslToRgb(h: number, s: number, l: number): [number, number, number] {
    h /= 360; s /= 100; l /= 100;
    let r, g, b;

    if (s === 0) {
        r = g = b = l;
    } else {
        const hue2rgb = (p: number, q: number, t: number) => {
            if (t < 0) t += 1;
            if (t > 1) t -= 1;
            if (t < 1/6) return p + (q - p) * 6 * t;
            if (t < 1/2) return q;
            if (t < 2/3) return p + (q - p) * (2/3 - t) * 6;
            return p;
        };
        const q = l < 0.5 ? l * (1 + s) : l + s - l * s;
        const p = 2 * l - q;
        r = hue2rgb(p, q, h + 1/3);
        g = hue2rgb(p, q, h);
        b = hue2rgb(p, q, h - 1/3);
    }
    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
}

// Generate shades of a color for template breakdown using HSL for better gradients
function generateColorShades(rgb: [number, number, number], count: number): { line: string; fill: string }[] {
    const [h, s] = rgbToHsl(rgb[0], rgb[1], rgb[2]);
    const shades: { line: string; fill: string }[] = [];

    // Create a wide range of lightness values (20% to 80%)
    // and vary saturation slightly for more distinction
    for (let i = 0; i < count; i++) {
        const t = count === 1 ? 0.5 : i / (count - 1);
        // Lightness: dark (25%) to light (75%)
        const newL = 25 + t * 50;
        // Saturation: boost for darker shades, reduce for lighter
        const newS = Math.min(100, s * (1.2 - t * 0.4));
        // Slight hue shift for additional distinction
        const newH = (h + t * 15 - 7.5 + 360) % 360;

        const [r, g, b] = hslToRgb(newH, newS, newL);
        shades.push({
            line: `rgb(${r}, ${g}, ${b})`,
            fill: `rgba(${r}, ${g}, ${b}, 0.7)`,
        });
    }
    return shades;
}

// Auto-refresh interval in milliseconds
const AUTO_REFRESH_INTERVAL_MS = 30000;

// Default bucket size for time series aggregation (seconds)
const DEFAULT_BUCKET_SECONDS = 10;

// Available bucket size options
const BUCKET_SIZE_OPTIONS = [
    { value: 5, label: '5 sec' },
    { value: 10, label: '10 sec' },
    { value: 30, label: '30 sec' },
    { value: 60, label: '1 min' },
    { value: 300, label: '5 min' },
    { value: 600, label: '10 min' },
    { value: 1800, label: '30 min' },
    { value: 3600, label: '1 hour' },
];

// Chart dimension constraints
const MIN_CHART_HEIGHT = 400;

// Maximum templates to show in hover tooltip
const MAX_TEMPLATES_IN_HOVER = 5;

// Date format for time range display
const TIME_RANGE_DISPLAY_FORMAT = 'MMM D, HH:mm';

type TimeRangePreset = 'all' | '1h' | '6h' | '24h' | '7d';

interface TimeRangeSelection {
    type: 'preset' | 'custom';
    preset?: TimeRangePreset;
    customStart?: dayjs.Dayjs;
    customEnd?: dayjs.Dayjs;
    fromZoom?: boolean; // True if range was set by plot zoom (don't override Plotly's state)
}

interface BucketIndexRange {
    start: number;
    end: number;
}

// Preset configurations with their durations
const PRESET_CONFIGS: Record<TimeRangePreset, { label: string; amount?: number; unit?: 'hour' | 'hours' | 'days' }> = {
    all: { label: 'All time' },
    '1h': { label: 'Last 1 hour', amount: 1, unit: 'hour' },
    '6h': { label: 'Last 6 hours', amount: 6, unit: 'hours' },
    '24h': { label: 'Last 24 hours', amount: 24, unit: 'hours' },
    '7d': { label: 'Last 7 days', amount: 7, unit: 'days' },
};

// Filter buckets based on time range selection
function filterBuckets(
    buckets: string[],
    selection: TimeRangeSelection
): { range: BucketIndexRange | null; axisRange: [string, string] | undefined } {
    if (buckets.length === 0) {
        return { range: null, axisRange: undefined };
    }

    const dataEndTime = dayjs(buckets[buckets.length - 1]);

    if (selection.type === 'preset') {
        const preset = selection.preset || 'all';
        if (preset === 'all') {
            return { range: null, axisRange: undefined };
        }

        const config = PRESET_CONFIGS[preset];
        if (!config.amount || !config.unit) {
            return { range: null, axisRange: undefined };
        }

        const cutoff = dataEndTime.subtract(config.amount, config.unit);
        const cutoffMs = cutoff.valueOf();
        const startIdx = buckets.findIndex(b => dayjs(b).valueOf() >= cutoffMs);

        if (startIdx < 0) {
            return { range: null, axisRange: [cutoff.toISOString(), dataEndTime.toISOString()] };
        }

        return {
            range: { start: startIdx, end: buckets.length },
            axisRange: [cutoff.toISOString(), dataEndTime.toISOString()],
        };
    }

    // Custom range
    if (!selection.customStart || !selection.customEnd) {
        return { range: null, axisRange: undefined };
    }

    // If range came from zoom, don't return axisRange - let Plotly maintain its own state
    const shouldSetAxisRange = !selection.fromZoom;

    const customStartMs = selection.customStart.valueOf();
    const customEndMs = selection.customEnd.valueOf();

    const startIdx = buckets.findIndex(b => dayjs(b).valueOf() >= customStartMs);
    const endIdx = buckets.findIndex(b => dayjs(b).valueOf() > customEndMs);

    const effectiveEndIdx = endIdx < 0 ? buckets.length : endIdx;
    const effectiveStartIdx = startIdx < 0 ? buckets.length : startIdx;

    if (effectiveStartIdx >= effectiveEndIdx) {
        // No data in range, but still set axis range (unless from zoom)
        return {
            range: { start: 0, end: 0 },
            axisRange: shouldSetAxisRange
                ? [selection.customStart.toISOString(), selection.customEnd.toISOString()]
                : undefined,
        };
    }

    return {
        range: { start: effectiveStartIdx, end: effectiveEndIdx },
        axisRange: shouldSetAxisRange
            ? [selection.customStart.toISOString(), selection.customEnd.toISOString()]
            : undefined,
    };
}

// Format the time range for display in the button
function formatTimeRangeLabel(selection: TimeRangeSelection): string {
    if (selection.type === 'preset') {
        return PRESET_CONFIGS[selection.preset || 'all'].label;
    }
    if (selection.customStart && selection.customEnd) {
        return `${selection.customStart.format(TIME_RANGE_DISPLAY_FORMAT)} - ${selection.customEnd.format(TIME_RANGE_DISPLAY_FORMAT)}`;
    }
    return 'Custom range';
}

interface TaskConcurrencyTabProps {
    workflowId: string | number | undefined;
}

export default function TaskConcurrencyTab({
    workflowId,
}: TaskConcurrencyTabProps) {
    const [timeRangeSelection, setTimeRangeSelection] = useState<TimeRangeSelection>({
        type: 'preset',
        preset: 'all',
    });
    const [bucketSeconds, setBucketSeconds] = useState(DEFAULT_BUCKET_SECONDS);
    const [selectedTemplates, setSelectedTemplates] = useState<string[]>([]);
    const [autoRefresh, setAutoRefresh] = useState(false);
    const [hiddenStatuses, setHiddenStatuses] = useState<Set<string>>(new Set());
    const [groupByTemplate, setGroupByTemplate] = useState(false);
    const [normalizeYAxis, setNormalizeYAxis] = useState(false);
    const [datePickerAnchor, setDatePickerAnchor] = useState<HTMLElement | null>(null);
    const [tempCustomStart, setTempCustomStart] = useState<dayjs.Dayjs | null>(null);
    const [tempCustomEnd, setTempCustomEnd] = useState<dayjs.Dayjs | null>(null);
    const [dragMode, setDragMode] = useState<'zoom' | 'pan'>('zoom');
    const [uiRevision, setUiRevision] = useState(0); // Increment to reset Plotly's UI state
    const containerRef = useRef<HTMLDivElement>(null);
    const graphDivRef = useRef<any>(null); // Store the Plotly graph div for direct manipulation
    const theme = useTheme();
    const isSmallScreen = useMediaQuery(theme.breakpoints.down('md'));

    // Fetch available templates for dropdown
    const { data: templatesData } = useQuery({
        queryKey: ['workflow_details', 'task_templates', workflowId],
        queryFn: getWorkflowTaskTemplatesQueryFn,
        refetchOnMount: true,
        refetchOnWindowFocus: false,
        enabled: !!workflowId,
    });

    const {
        data: concurrencyData,
        isLoading,
        isError,
        error,
        refetch,
    } = useQuery({
        queryKey: ['workflow_details', 'task_concurrency', workflowId, bucketSeconds],
        queryFn: getTaskConcurrencyQueryFn,
        refetchOnMount: true,
        refetchOnWindowFocus: false,
        refetchInterval: autoRefresh ? AUTO_REFRESH_INTERVAL_MS : false,
        enabled: !!workflowId,
    });

    // Filter data and compute axis range based on time selection
    const { filteredData, xAxisRange } = useMemo(() => {
        if (!concurrencyData?.buckets?.length) {
            return { filteredData: null, xAxisRange: undefined };
        }

        // If range came from zoom, don't filter data - let Plotly handle the zoom
        if (timeRangeSelection.fromZoom) {
            return { filteredData: concurrencyData, xAxisRange: undefined };
        }

        const { buckets, series, template_breakdown } = concurrencyData;
        const { range, axisRange } = filterBuckets(buckets, timeRangeSelection);

        if (!range) {
            return { filteredData: concurrencyData, xAxisRange: axisRange };
        }

        // Handle empty range (no data in selected window)
        if (range.start >= range.end) {
            return {
                filteredData: { buckets: [], series: {}, template_breakdown: undefined },
                xAxisRange: axisRange,
            };
        }

        const filteredBuckets = buckets.slice(range.start, range.end);
        const filteredSeries: Record<string, number[]> = {};

        for (const [name, counts] of Object.entries(series)) {
            filteredSeries[name] = counts.slice(range.start, range.end);
        }

        // Also filter template_breakdown if present
        let filteredBreakdown: Record<string, Record<string, number>[]> | undefined;
        if (template_breakdown) {
            filteredBreakdown = {};
            for (const [status, breakdownArray] of Object.entries(template_breakdown)) {
                filteredBreakdown[status] = breakdownArray.slice(range.start, range.end);
            }
        }

        return {
            filteredData: {
                buckets: filteredBuckets,
                series: filteredSeries,
                template_breakdown: filteredBreakdown,
            },
            xAxisRange: axisRange,
        };
    }, [concurrencyData, timeRangeSelection]);

    // Initialize custom date picker with data bounds when popover opens
    const handleDatePickerOpen = (event: React.MouseEvent<HTMLElement>) => {
        setDatePickerAnchor(event.currentTarget);
        if (concurrencyData?.buckets?.length) {
            const buckets = concurrencyData.buckets;
            // Default custom range to full data range
            setTempCustomStart(dayjs(buckets[0]));
            setTempCustomEnd(dayjs(buckets[buckets.length - 1]));
        }
    };

    const handleDatePickerClose = () => {
        setDatePickerAnchor(null);
    };

    const handlePresetSelect = (preset: TimeRangePreset) => {
        setTimeRangeSelection({ type: 'preset', preset });
        setUiRevision(prev => prev + 1); // Force Plotly to apply the new range
        handleDatePickerClose();
    };

    const handleCustomRangeApply = () => {
        if (tempCustomStart && tempCustomEnd) {
            setTimeRangeSelection({
                type: 'custom',
                customStart: tempCustomStart,
                customEnd: tempCustomEnd,
            });
            setUiRevision(prev => prev + 1); // Force Plotly to apply the new range
        }
        handleDatePickerClose();
    };

    // Handle plot zoom/pan - sync date picker with plot's x-axis range
    const handlePlotRelayout = useCallback((event: PlotRelayoutEvent) => {
        // Ignore events that are just layout property changes (dragmode, autosize, etc.)
        // Only respond to actual x-axis range changes from user interaction
        const eventKeys = Object.keys(event);
        const isOnlyLayoutChange = eventKeys.every(key =>
            key === 'dragmode' ||
            key === 'autosize' ||
            key === 'width' ||
            key === 'height' ||
            key.startsWith('yaxis')
        );
        if (isOnlyLayoutChange) return;

        // Check if this is a zoom/pan event with x-axis range
        if (event['xaxis.range[0]'] && event['xaxis.range[1]']) {
            const newStart = dayjs(event['xaxis.range[0]']);
            const newEnd = dayjs(event['xaxis.range[1]']);
            if (newStart.isValid() && newEnd.isValid()) {
                setTimeRangeSelection({
                    type: 'custom',
                    customStart: newStart,
                    customEnd: newEnd,
                    fromZoom: true, // Don't override Plotly's zoom state
                });
            }
        } else if (event['xaxis.range']) {
            // Alternative format for range
            const range = event['xaxis.range'] as [string, string];
            const newStart = dayjs(range[0]);
            const newEnd = dayjs(range[1]);
            if (newStart.isValid() && newEnd.isValid()) {
                setTimeRangeSelection({
                    type: 'custom',
                    customStart: newStart,
                    customEnd: newEnd,
                    fromZoom: true, // Don't override Plotly's zoom state
                });
            }
        }
        // Note: We don't handle xaxis.autorange here anymore - reset is done via handleResetZoom
    }, []);

    // Reset zoom to show all data
    const handleResetZoom = useCallback(() => {
        setTimeRangeSelection({ type: 'preset', preset: 'all' });
        setUiRevision(prev => prev + 1); // Force Plotly to reset UI state
    }, []);

    // Store reference to Plotly graph div when initialized
    const handlePlotInitialized = useCallback((_figure: any, graphDiv: any) => {
        graphDivRef.current = graphDiv;
    }, []);

    // Convert Plotly range value to milliseconds (handles both number and string formats)
    const rangeValueToMs = (value: number | string): number =>
        typeof value === 'number' ? value : new Date(value).getTime();

    // Zoom by scale factor: 0.5 = zoom in (halve range), 2 = zoom out (double range)
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

    // Toggle status visibility
    const toggleStatusVisibility = (status: string) => {
        setHiddenStatuses(prev => {
            const next = new Set(prev);
            if (next.has(status)) {
                next.delete(status);
            } else {
                next.add(status);
            }
            return next;
        });
    };

    // Format template breakdown for hover display, optionally filtered by selected templates
    const formatTemplateBreakdown = useCallback((breakdown: Record<string, number> | undefined): string => {
        if (!breakdown || Object.keys(breakdown).length === 0) {
            return '';
        }
        // Filter by selected templates if any
        let entries = Object.entries(breakdown);
        if (selectedTemplates.length > 0) {
            entries = entries.filter(([name]) => selectedTemplates.includes(name));
        }
        if (entries.length === 0) {
            return '';
        }
        // Sort by count descending, limit to top N
        const sorted = entries.sort((a, b) => b[1] - a[1]).slice(0, MAX_TEMPLATES_IN_HOVER);
        const lines = sorted.map(([name, count]) => `  ${name}: ${count}`);
        if (entries.length > MAX_TEMPLATES_IN_HOVER) {
            const othersCount = entries
                .slice(MAX_TEMPLATES_IN_HOVER)
                .reduce((sum, [, count]) => sum + count, 0);
            lines.push(`  (${entries.length - MAX_TEMPLATES_IN_HOVER} more): ${othersCount}`);
        }
        return '<br>' + lines.join('<br>');
    }, [selectedTemplates]);

    // Get unique template names from breakdown data, sorted by total count
    const getTemplatesForStatus = (
        templateBreakdown: Record<string, number>[] | undefined
    ): string[] => {
        if (!templateBreakdown) return [];
        const totals: Record<string, number> = {};
        for (const bucket of templateBreakdown) {
            for (const [name, count] of Object.entries(bucket)) {
                totals[name] = (totals[name] || 0) + count;
            }
        }
        return Object.entries(totals)
            .sort((a, b) => b[1] - a[1])
            .map(([name]) => name);
    };

    // Calculate filtered series totals when templates are selected
    const getFilteredSeriesForStatus = (
        statusName: string,
        buckets: string[],
        series: Record<string, number[]>,
        template_breakdown: Record<string, Record<string, number>[]> | undefined,
        templateFilter: string[]
    ): number[] => {
        // If no templates selected, use original series
        if (templateFilter.length === 0) {
            return series[statusName];
        }
        // If no breakdown data, fall back to original series
        if (!template_breakdown?.[statusName]) {
            return series[statusName];
        }
        // Calculate filtered totals from template breakdown
        return buckets.map((_, bucketIdx) => {
            const breakdown = template_breakdown[statusName][bucketIdx];
            if (!breakdown) return 0;
            return templateFilter.reduce((sum, templateName) => {
                return sum + (breakdown[templateName] || 0);
            }, 0);
        });
    };

    // Get visible statuses for subplot layout
    const visibleStatuses = useMemo(() => {
        if (!filteredData?.series) return [];
        return STATUS_DISPLAY_ORDER.filter(
            name => name in filteredData.series && !hiddenStatuses.has(name)
        );
    }, [filteredData, hiddenStatuses]);

    // Generate Plotly traces - one subplot per status, stacked vertically
    const traces = useMemo(() => {
        if (!filteredData || filteredData.buckets.length === 0) {
            return [];
        }

        const { buckets, series, template_breakdown } = filteredData;
        const resultTraces: PlotlyData[] = [];

        visibleStatuses.forEach((statusName, statusIndex) => {
            const yAxisKey = statusIndex === 0 ? 'y' : `y${statusIndex + 1}`;

            if (groupByTemplate && template_breakdown?.[statusName]) {
                // Show grouped bars by template
                let templates = getTemplatesForStatus(template_breakdown[statusName]);
                if (selectedTemplates.length > 0) {
                    templates = templates.filter(t => selectedTemplates.includes(t));
                }
                const statusColor = STATUS_COLORS[statusName];
                const shades = generateColorShades(statusColor.rgb, templates.length);

                templates.forEach((templateName, idx) => {
                    const templateCounts = buckets.map((_, bucketIdx) =>
                        template_breakdown[statusName][bucketIdx]?.[templateName] || 0
                    );
                    const colors = shades[idx] || shades[shades.length - 1];

                    resultTraces.push({
                        x: buckets,
                        y: templateCounts,
                        name: templateName,
                        type: 'bar',
                        marker: { color: colors.fill, line: { color: colors.line, width: 1 } },
                        yaxis: yAxisKey,
                        hovertemplate: `<b>${templateName}</b><br>Count: %{y}<extra>${statusName}</extra>`,
                    });
                });
            } else {
                // Show aggregate status bar
                const colors = STATUS_COLORS[statusName];
                const filteredSeries = getFilteredSeriesForStatus(
                    statusName, buckets, series, template_breakdown, selectedTemplates
                );

                const hoverText = buckets.map((_, idx) => {
                    const breakdown = template_breakdown?.[statusName]?.[idx];
                    return formatTemplateBreakdown(breakdown);
                });

                resultTraces.push({
                    x: buckets,
                    y: filteredSeries,
                    name: statusName,
                    type: 'bar',
                    marker: { color: colors.fill, line: { color: colors.line, width: 1 } },
                    yaxis: yAxisKey,
                    text: hoverText,
                    hovertemplate: `<b>${statusName}</b>: %{y}%{text}<extra></extra>`,
                });
            }
        });

        return resultTraces;
    }, [filteredData, visibleStatuses, groupByTemplate, selectedTemplates, formatTemplateBreakdown]);


    const chartTitle = useMemo(() => {
        let title = 'Active Tasks by Status';
        if (selectedTemplates.length > 0) {
            title += `: ${selectedTemplates.length} template${selectedTemplates.length > 1 ? 's' : ''} selected`;
        }
        if (groupByTemplate) {
            title += ' — Grouped by Template';
        }
        return title;
    }, [selectedTemplates, groupByTemplate]);

    const layout: Partial<Layout> = useMemo(
        () => {
            const numSubplots = visibleStatuses.length || 1;
            const gap = 0.03; // Gap between subplots
            const plotHeight = (1 - gap * (numSubplots - 1)) / numSubplots;

            // Calculate max y value across all visible statuses for normalization
            let maxY = 0;
            if (normalizeYAxis && filteredData?.series) {
                for (const statusName of visibleStatuses) {
                    const seriesData = filteredData.series[statusName];
                    if (seriesData) {
                        const statusMax = Math.max(...seriesData);
                        if (statusMax > maxY) maxY = statusMax;
                    }
                }
                // Add 10% padding
                maxY = Math.ceil(maxY * 1.1);
            }

            // Build y-axis configs for each subplot
            const yAxes: Record<string, any> = {};
            visibleStatuses.forEach((statusName, idx) => {
                const axisKey = idx === 0 ? 'yaxis' : `yaxis${idx + 1}`;
                const domainStart = 1 - (idx + 1) * plotHeight - idx * gap;
                const domainEnd = 1 - idx * plotHeight - idx * gap;

                yAxes[axisKey] = {
                    title: {
                        text: statusName,
                        font: { size: 12, family: 'Roboto, sans-serif', color: STATUS_COLORS[statusName]?.line },
                    },
                    tickfont: { size: 10 },
                    gridcolor: 'rgba(0,0,0,0.1)',
                    rangemode: 'tozero',
                    domain: [Math.max(0, domainStart), domainEnd],
                    anchor: 'x',
                    ...(normalizeYAxis && maxY > 0 ? { range: [0, maxY] } : {}),
                };
            });

            return {
                title: {
                    text: chartTitle,
                    font: {
                        size: isSmallScreen ? 14 : 18,
                        family: 'Roboto, sans-serif',
                        color: '#1976d2',
                    },
                    x: 0.02,
                    xanchor: 'left',
                },
                xaxis: {
                    title: {
                        text: 'Time',
                        font: { size: 14, family: 'Roboto, sans-serif' },
                    },
                    tickfont: { size: 11 },
                    gridcolor: 'rgba(0,0,0,0.2)',
                    type: 'date',
                    dtick: bucketSeconds * 1000,
                    tick0: filteredData?.buckets?.[0],
                    ticks: 'outside',
                    ticklen: 5,
                    tickwidth: 1,
                    tickcolor: 'rgba(0,0,0,0.3)',
                    showgrid: true,
                    // Anchor to bottom subplot
                    anchor: numSubplots > 1 ? `y${numSubplots}` : 'y',
                    ...(xAxisRange
                        ? { range: xAxisRange, autorange: false }
                        : { autorange: true }
                    ),
                },
                ...yAxes,
                barmode: groupByTemplate ? 'group' : 'stack',
                autosize: true,
                showlegend: groupByTemplate, // Show legend when grouped by template
                margin: isSmallScreen
                    ? { l: 70, r: 20, t: 50, b: 60, pad: 3 }
                    : { l: 80, r: 30, t: 60, b: 60, pad: 5 },
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
        },
        [isSmallScreen, chartTitle, xAxisRange, dragMode, uiRevision, bucketSeconds, filteredData, visibleStatuses, groupByTemplate, normalizeYAxis]
    );

    const handleExportCSV = useCallback(() => {
        if (!filteredData?.buckets?.length) return;

        const { buckets, series } = filteredData;
        const seriesNames = Object.keys(series).sort();

        const header = ['timestamp', ...seriesNames].join(',');
        const rows = buckets.map((timestamp, idx) => {
            const counts = seriesNames.map(name => series[name][idx] || 0);
            return [timestamp, ...counts].join(',');
        });

        const csvContent = [header, ...rows].join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);

        const link = document.createElement('a');
        link.href = url;
        link.download = `workflow_${workflowId}_concurrency_${dayjs().format('YYYY-MM-DD_HH-mm')}.csv`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, [filteredData, workflowId]);

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
                    Loading concurrency data...
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
                    Error loading concurrency data:{' '}
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
    const hasNoData =
        !concurrencyData?.buckets?.length ||
        !concurrencyData?.series ||
        Object.keys(concurrencyData.series).length === 0;

    if (hasNoData) {
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
                    No task concurrency data available.
                </Typography>
                <Typography variant="body2" color="text.secondary">
                    Run the workflow to generate concurrency data, or this
                    workflow may have been created before concurrency tracking
                    was enabled.
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
        <Box sx={{ width: '100%', p: 2 }}>
            {/* Toolbar - Row 1: Data & Time Controls */}
            <Box
                sx={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 2,
                    mb: 1.5,
                    alignItems: 'center',
                    justifyContent: 'space-between',
                }}
            >
                {/* Left: Data Filters */}
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <FormControl size="small" sx={{ minWidth: 180 }}>
                        <InputLabel>Templates</InputLabel>
                        <Select
                            multiple
                            value={selectedTemplates}
                            label="Templates"
                            onChange={(e) => setSelectedTemplates(e.target.value as string[])}
                            renderValue={(selected) =>
                                selected.length === 0 ? 'All' : `${selected.length} selected`
                            }
                        >
                            {templatesData?.task_templates?.map(t => (
                                <MenuItem key={t} value={t}>{t}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>

                    <FormControl size="small" sx={{ minWidth: 90 }}>
                        <InputLabel>Bucket</InputLabel>
                        <Select
                            value={bucketSeconds}
                            label="Bucket"
                            onChange={(e) => setBucketSeconds(e.target.value as number)}
                        >
                            {BUCKET_SIZE_OPTIONS.map(opt => (
                                <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>

                    <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />

                    {/* Time Range */}
                    <Button
                        variant="outlined"
                        size="small"
                        startIcon={<CalendarTodayIcon />}
                        onClick={handleDatePickerOpen}
                        sx={{ textTransform: 'none', minWidth: 140 }}
                    >
                        {formatTimeRangeLabel(timeRangeSelection)}
                    </Button>

                    <ButtonGroup size="small" variant="outlined">
                        <Tooltip title="Zoom mode">
                            <Button
                                variant={dragMode === 'zoom' ? 'contained' : 'outlined'}
                                onClick={() => setDragMode('zoom')}
                            >
                                <ZoomInIcon fontSize="small" />
                            </Button>
                        </Tooltip>
                        <Tooltip title="Pan mode">
                            <Button
                                variant={dragMode === 'pan' ? 'contained' : 'outlined'}
                                onClick={() => setDragMode('pan')}
                            >
                                <PanToolIcon fontSize="small" />
                            </Button>
                        </Tooltip>
                    </ButtonGroup>

                    <ButtonGroup size="small" variant="outlined">
                        <Tooltip title="Zoom in">
                            <Button onClick={handleZoomIn}>
                                <AddIcon fontSize="small" />
                            </Button>
                        </Tooltip>
                        <Tooltip title="Zoom out">
                            <Button onClick={handleZoomOut}>
                                <RemoveIcon fontSize="small" />
                            </Button>
                        </Tooltip>
                        <Tooltip title="Reset zoom">
                            <Button onClick={handleResetZoom}>
                                <RestartAltIcon fontSize="small" />
                            </Button>
                        </Tooltip>
                    </ButtonGroup>

                    <Popover
                        open={Boolean(datePickerAnchor)}
                        anchorEl={datePickerAnchor}
                        onClose={handleDatePickerClose}
                        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
                        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                        slotProps={{
                            paper: { sx: { bgcolor: 'background.paper', boxShadow: 4 } },
                        }}
                    >
                        <LocalizationProvider dateAdapter={AdapterDayjs}>
                            <Box sx={{ p: 2, width: 320 }}>
                                <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                                    Quick select
                                </Typography>
                                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 2 }}>
                                    {(Object.entries(PRESET_CONFIGS) as [TimeRangePreset, { label: string }][]).map(
                                        ([key, config]) => (
                                            <Button
                                                key={key}
                                                size="small"
                                                variant={
                                                    timeRangeSelection.type === 'preset' &&
                                                    timeRangeSelection.preset === key
                                                        ? 'contained'
                                                        : 'outlined'
                                                }
                                                onClick={() => handlePresetSelect(key)}
                                                sx={{ textTransform: 'none' }}
                                            >
                                                {config.label}
                                            </Button>
                                        )
                                    )}
                                </Box>
                                <Divider sx={{ my: 2 }} />
                                <Typography variant="subtitle2" color="text.secondary" gutterBottom>
                                    Custom range
                                </Typography>
                                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                                    <DateTimePicker
                                        label="Start"
                                        value={tempCustomStart}
                                        onChange={setTempCustomStart}
                                        slotProps={{ textField: { size: 'small', fullWidth: true } }}
                                    />
                                    <DateTimePicker
                                        label="End"
                                        value={tempCustomEnd}
                                        onChange={setTempCustomEnd}
                                        slotProps={{ textField: { size: 'small', fullWidth: true } }}
                                    />
                                    <Button
                                        variant="contained"
                                        onClick={handleCustomRangeApply}
                                        disabled={!tempCustomStart || !tempCustomEnd}
                                    >
                                        Apply
                                    </Button>
                                </Box>
                            </Box>
                        </LocalizationProvider>
                    </Popover>
                </Box>

                {/* Right: Actions */}
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                    <Tooltip
                        title={
                            autoRefresh
                                ? 'Disable auto-refresh'
                                : `Enable auto-refresh (${AUTO_REFRESH_INTERVAL_MS / 1000}s)`
                        }
                    >
                        <IconButton
                            size="small"
                            onClick={() => setAutoRefresh(!autoRefresh)}
                            color={autoRefresh ? 'primary' : 'default'}
                        >
                            {autoRefresh ? <PauseIcon /> : <PlayArrowIcon />}
                        </IconButton>
                    </Tooltip>
                    <Tooltip title="Refresh now">
                        <IconButton size="small" onClick={() => refetch()}>
                            <RefreshIcon />
                        </IconButton>
                    </Tooltip>
                    <Tooltip title="Export CSV">
                        <IconButton size="small" onClick={handleExportCSV}>
                            <DownloadIcon />
                        </IconButton>
                    </Tooltip>
                </Box>
            </Box>

            {/* Toolbar - Row 2: View Options */}
            <Box
                sx={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 1.5,
                    mb: 2,
                    alignItems: 'center',
                    pb: 1.5,
                    borderBottom: '1px solid',
                    borderColor: 'divider',
                }}
            >
                {/* Status toggle buttons */}
                <ButtonGroup size="small" variant="outlined">
                    {STATUS_DISPLAY_ORDER.map(status => {
                        const isHidden = hiddenStatuses.has(status);
                        return (
                            <Tooltip
                                key={status}
                                title={isHidden ? 'Click to show' : 'Click to hide'}
                            >
                                <Button
                                    variant={isHidden ? 'outlined' : 'contained'}
                                    onClick={() => toggleStatusVisibility(status)}
                                    sx={{
                                        borderColor: STATUS_COLORS[status].line,
                                        color: isHidden ? 'rgba(0,0,0,0.3)' : 'white',
                                        backgroundColor: isHidden ? 'rgba(0,0,0,0.05)' : STATUS_COLORS[status].line,
                                        textDecoration: isHidden ? 'line-through' : 'none',
                                        opacity: isHidden ? 0.6 : 1,
                                        '&:hover': {
                                            backgroundColor: isHidden
                                                ? 'rgba(0,0,0,0.1)'
                                                : STATUS_COLORS[status].line,
                                            borderColor: STATUS_COLORS[status].line,
                                        },
                                    }}
                                >
                                    {status}
                                </Button>
                            </Tooltip>
                        );
                    })}
                </ButtonGroup>

                <Divider orientation="vertical" flexItem />

                {/* View toggles */}
                <Tooltip title={groupByTemplate ? 'Show aggregated' : 'Group by template'}>
                    <Button
                        size="small"
                        variant={groupByTemplate ? 'contained' : 'outlined'}
                        onClick={() => setGroupByTemplate(!groupByTemplate)}
                    >
                        By Template
                    </Button>
                </Tooltip>

                <Tooltip title={normalizeYAxis ? 'Use independent scales' : 'Sync y-axis scales'}>
                    <Button
                        size="small"
                        variant={normalizeYAxis ? 'contained' : 'outlined'}
                        onClick={() => setNormalizeYAxis(!normalizeYAxis)}
                    >
                        Sync Scales
                    </Button>
                </Tooltip>
            </Box>

            {/* Chart */}
            <Box
                ref={containerRef}
                sx={{
                    width: '100%',
                    minHeight: MIN_CHART_HEIGHT,
                    border: '1px solid #e0e0e0',
                    borderRadius: 1,
                    bgcolor: 'background.paper',
                }}
            >
                {traces.length > 0 ? (
                    <Plot
                        data={traces}
                        layout={layout}
                        style={{ width: '100%', height: '100%' }}
                        useResizeHandler={true}
                        onInitialized={handlePlotInitialized}
                        onUpdate={handlePlotInitialized}
                        onRelayout={handlePlotRelayout}
                        config={{
                            responsive: true,
                            displaylogo: false,
                            displayModeBar: false, // We use custom controls in the header
                            scrollZoom: true, // Enable scroll wheel zoom
                        }}
                    />
                ) : (
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            height: MIN_CHART_HEIGHT,
                        }}
                    >
                        <Typography color="text.secondary">
                            No data available for the selected time range.
                        </Typography>
                    </Box>
                )}
            </Box>
        </Box>
    );
}
