import React, { useMemo, useState } from 'react';
import Plot from 'react-plotly.js';
import dayjs from 'dayjs';
import { useQuery } from '@tanstack/react-query';
import { 
    Box, 
    CircularProgress, 
    Typography, 
    FormControl, 
    InputLabel, 
    Select, 
    MenuItem, 
    Button,
    Alert
} from '@mui/material';
import { SelectChangeEvent } from '@mui/material/Select';

interface TaskConcurrencyTimelineProps {
    workflowId: string | number;
    taskTemplateInfo: { tt_version_id: string | number; name: string }[];
}

interface ConcurrencyDataPoint {
    timestamp: string;
    task_template_name: string;
    concurrent_count: number;
}

interface ConcurrencyResponse {
    data?: ConcurrencyDataPoint[];
}

// Color palette for consistent task template visualization
const TEMPLATE_COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
];

const TIME_RANGES = {
    '1h': { label: '1 Hour', minutes: 60 },
    '6h': { label: '6 Hours', minutes: 360 },
    '24h': { label: '24 Hours', minutes: 1440 },
    '7d': { label: '7 Days', minutes: 10080 }
};

const TaskConcurrencyTimeline: React.FC<TaskConcurrencyTimelineProps> = ({
    workflowId,
    taskTemplateInfo
}) => {
    const [timeRange, setTimeRange] = useState<keyof typeof TIME_RANGES>('1h');
    const [isExporting, setIsExporting] = useState(false);

    // Fetch task concurrency data - DISABLED FOR DEMO
    const { data, isLoading, refetch } = useQuery({
        queryKey: ['task_concurrency', workflowId, timeRange],
        queryFn: async (): Promise<ConcurrencyResponse> => {
            // Force demo mode by returning empty data
            return { data: [] };
            
            // Original API call (commented out for demo)
            // const response = await axios.get(
            //     get_task_concurrency_url(workflowId),
            //     {
            //         ...jobmonAxiosConfig,
            //         params: {
            //             time_range: timeRange,
            //             granularity: 'minute'
            //         }
            //     }
            // );
            // return response.data;
        },
        refetchInterval: 30000, // Refresh every 30 seconds
        staleTime: 10000, // Consider data stale after 10 seconds
        retry: 3,
        retryDelay: attemptIndex => Math.min(1000 * 2 ** attemptIndex, 30000)
    });

    // Process data for Plotly
    const { timestamps, traces } = useMemo(() => {
        if (!data?.data || data.data.length === 0) {
            // Generate realistic demo data
            const minutes = TIME_RANGES[timeRange].minutes;
            const points = Math.min(minutes, timeRange === '7d' ? 168 : 60); // More points for 7d view
            const interval = Math.max(1, Math.floor(minutes / points));
            
            const ts = Array.from({ length: points }, (_, i) =>
                dayjs()
                    .subtract((points - 1 - i) * interval, 'minute')
            );

            const formattedTimestamps = ts.map(time => 
                time.format(timeRange === '7d' ? 'MMM DD HH:mm' : 'HH:mm')
            );

            // Generate more realistic demo data with patterns
            const generateRealisticData = (templateName: string, baseLoad: number) => {
                return ts.map((time, i) => {
                    const hour = time.hour();
                    const dayOfWeek = time.day(); // 0 = Sunday, 6 = Saturday
                    
                    // Business hours multiplier (higher load 9-17, lower on weekends)
                    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
                    const isBusinessHours = hour >= 9 && hour <= 17;
                    let multiplier = 1;
                    
                    if (isWeekend) {
                        multiplier = 0.3; // Much lower on weekends
                    } else if (isBusinessHours) {
                        multiplier = 1.5; // Higher during business hours
                    } else {
                        multiplier = 0.6; // Lower during off hours
                    }
                    
                    // Add some wave patterns for different templates
                    const waveOffset = templateName === 'Transform' ? Math.PI / 3 : 
                                     templateName === 'Load' ? Math.PI * 2 / 3 : 0;
                    const wave = Math.sin((i / points) * Math.PI * 4 + waveOffset) * 0.3;
                    
                    // Add some randomness
                    const randomFactor = (Math.random() - 0.5) * 0.4;
                    
                    const value = Math.max(0, Math.floor(
                        baseLoad * multiplier * (1 + wave + randomFactor)
                    ));
                    
                    return value;
                });
            };

            // Use actual task template names or fallback to defaults
            const templateNames = taskTemplateInfo.length > 0 
                ? taskTemplateInfo.slice(0, 5).map(t => t.name) // Limit to 5 for performance
                : ['Extract', 'Transform', 'Load'];

            const baseLoads = [12, 8, 5, 6, 4]; // Different base loads for variety

            const dummyTraces = templateNames.map((templateName, index) => ({
                x: formattedTimestamps,
                y: generateRealisticData(templateName, baseLoads[index] || 5),
                name: templateName,
                stackgroup: 'one',
                mode: 'lines' as const,
                hoverinfo: 'x+y+name' as const,
                fillcolor: TEMPLATE_COLORS[index % TEMPLATE_COLORS.length],
                line: { color: TEMPLATE_COLORS[index % TEMPLATE_COLORS.length] }
            }));

            return { timestamps: formattedTimestamps, traces: dummyTraces };
        }

        // Process real API data
        const templateMap = new Map<string, ConcurrencyDataPoint[]>();
        data.data.forEach(point => {
            if (!templateMap.has(point.task_template_name)) {
                templateMap.set(point.task_template_name, []);
            }
            templateMap.get(point.task_template_name)!.push(point);
        });

        // Get unique timestamps and sort them
        const uniqueTimestamps = Array.from(
            new Set(data.data.map(point => point.timestamp))
        ).sort();

        const formattedTimestamps = uniqueTimestamps.map(ts => 
            dayjs(ts).format(timeRange === '7d' ? 'MMM DD HH:mm' : 'HH:mm')
        );

        const processedTraces = Array.from(templateMap.entries()).map(([templateName, points], index) => {
            // Create a map for quick lookup
            const pointMap = new Map(points.map(p => [p.timestamp, p.concurrent_count]));
            
            // Fill in data for all timestamps (0 if no data)
            const yValues = uniqueTimestamps.map(ts => pointMap.get(ts) || 0);

            return {
                x: formattedTimestamps,
                y: yValues,
                name: templateName,
                stackgroup: 'one',
                mode: 'lines' as const,
                hoverinfo: 'x+y+name' as const,
                fillcolor: TEMPLATE_COLORS[index % TEMPLATE_COLORS.length],
                line: { color: TEMPLATE_COLORS[index % TEMPLATE_COLORS.length] }
            };
        });

        return { timestamps: formattedTimestamps, traces: processedTraces };
    }, [data, timeRange, taskTemplateInfo]);

    const handleTimeRangeChange = (event: SelectChangeEvent<keyof typeof TIME_RANGES>) => {
        setTimeRange(event.target.value as keyof typeof TIME_RANGES);
    };

    const handleExportCSV = async () => {
        setIsExporting(true);
        try {
            const csvData = timestamps.map((time, i) => ({
                timestamp: time,
                ...traces.reduce((acc, trace, _) => ({
                    ...acc,
                    [trace.name]: trace.y[i]
                }), {})
            }));

            const csvContent = [
                Object.keys(csvData[0]).join(','),
                ...csvData.map(row => Object.values(row).join(','))
            ].join('\n');

            const blob = new Blob([csvContent], { type: 'text/csv' });
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `workflow-${workflowId}-concurrency-${timeRange}.csv`;
            link.click();
            window.URL.revokeObjectURL(url);
        } catch (error) {
            console.error('Export failed:', error);
        } finally {
            setIsExporting(false);
        }
    };

    const plotLayout = {
        title: {
            text: `Task Concurrency Timeline - ${TIME_RANGES[timeRange].label}`,
            font: { size: 16 }
        },
        xaxis: {
            title: 'Time',
            tickangle: -45,
            showgrid: true,
            gridcolor: '#f0f0f0',
            type: 'category' as const
        },
        yaxis: {
            title: 'Concurrent Tasks',
            showgrid: true,
            gridcolor: '#f0f0f0',
            rangemode: 'tozero' as const
        },
        hovermode: 'x unified' as const,
        legend: {
            orientation: 'h' as const,
            y: -0.15,
            x: 0.5,
            xanchor: 'center' as const
        },
        plot_bgcolor: 'white',
        paper_bgcolor: 'white',
        margin: { t: 60, l: 60, r: 20, b: 100 },
        showlegend: true
    };

    const plotConfig = {
        responsive: true,
        displayModeBar: true,
        modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
        displaylogo: false
    };

    if (isLoading) {
        return (
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
                <CircularProgress />
                <Typography sx={{ ml: 2 }}>Loading concurrency data...</Typography>
            </Box>
        );
    }

    return (
        <Box sx={{ p: 2 }}>
            {/* Demo Mode Alert */}
            <Alert severity="info" sx={{ mb: 2 }}>
                📊 Demo Mode: Showing realistic simulated task concurrency data with business hour patterns
            </Alert>

            {/* Controls */}
            <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
                <FormControl size="small" sx={{ minWidth: 120 }}>
                    <InputLabel>Time Range</InputLabel>
                    <Select
                        value={timeRange}
                        label="Time Range"
                        onChange={handleTimeRangeChange}
                    >
                        {Object.entries(TIME_RANGES).map(([key, { label }]) => (
                            <MenuItem key={key} value={key}>
                                {label}
                            </MenuItem>
                        ))}
                    </Select>
                </FormControl>
                
                <Button
                    variant="outlined"
                    size="small"
                    onClick={handleExportCSV}
                    disabled={isExporting || traces.length === 0}
                >
                    {isExporting ? 'Exporting...' : 'Export CSV'}
                </Button>

                <Button
                    variant="outlined"
                    size="small"
                    onClick={() => refetch()}
                    disabled={isLoading}
                >
                    Regenerate Data
                </Button>
            </Box>

            {/* Chart */}
            {traces.length > 0 ? (
                <Plot
                    data={traces}
                    layout={plotLayout}
                    config={plotConfig}
                    style={{ width: '100%', height: '500px' }}
                    useResizeHandler
                />
            ) : (
                <Box 
                    display="flex" 
                    justifyContent="center" 
                    alignItems="center" 
                    minHeight="400px"
                    sx={{ border: '1px dashed #ccc', borderRadius: 1 }}
                >
                    <Typography color="text.secondary">
                        No concurrency data available for the selected time range
                    </Typography>
                </Box>
            )}

            {/* Info */}
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                Demo data regenerates every 30 seconds and includes realistic business hour patterns.
                Higher activity during weekdays 9-5, lower on weekends and off-hours.
            </Typography>
        </Box>
    );
};

export default TaskConcurrencyTimeline; 