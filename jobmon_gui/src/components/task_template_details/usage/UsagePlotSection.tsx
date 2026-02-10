// Plot section component for Usage analysis

import React, { useRef, useState } from 'react';
import {
    Box,
    Button,
    ButtonGroup,
    IconButton,
    Paper,
    Skeleton,
    Tooltip,
    Typography,
} from '@mui/material';
import {
    Info as InfoIcon,
    Download as DownloadIcon,
    Tune as TuneIcon,
    ZoomIn as ZoomInIcon,
    PanTool as PanToolIcon,
    Add as AddIcon,
    Remove as RemoveIcon,
    RestartAlt as RestartAltIcon,
    HighlightAlt as SelectIcon,
} from '@mui/icons-material';
import RuntimeMemoryScatterPlot, {
    ScatterPlotHandle,
} from './RuntimeMemoryScatterPlot';
import UsageFilters, { UsageFiltersProps } from './UsageFilters';
import { ScatterDataPoint } from '@jobmon_gui/types/Usage';

type FilterProps = Omit<UsageFiltersProps, 'anchorEl' | 'onClose'>;

interface UsagePlotSectionProps extends FilterProps {
    isLoading: boolean;
    filteredScatterData: ScatterDataPoint[];
    taskTemplateName: string;
    medianRequestedRuntime?: number;
    medianRequestedMemoryGiB?: number;
    onTaskClick: (taskId: number | string) => void;
    onSelected: (selectedPoints: ScatterDataPoint[]) => void;
    onDownloadCSV?: () => void;
    hasData?: boolean;
}

const UsagePlotSection: React.FC<UsagePlotSectionProps> = ({
    isLoading,
    filteredScatterData,
    taskTemplateName,
    medianRequestedRuntime,
    medianRequestedMemoryGiB,
    onTaskClick,
    onSelected,
    onDownloadCSV,
    hasData = false,
    // Filter props forwarded to popover
    availableAttempts,
    availableStatuses,
    availableResourceClusters,
    availableTaskNames,
    selectedAttempts,
    selectedStatuses,
    selectedResourceClusters,
    selectedTaskNames,
    showResourceZones,
    onSelectedAttemptsChange,
    onSelectedStatusesChange,
    onSelectedResourceClustersChange,
    onSelectedTaskNamesChange,
    onShowResourceZonesChange,
    onResetFilters,
}) => {
    const [settingsAnchor, setSettingsAnchor] =
        useState<HTMLElement | null>(null);
    const [dragMode, setDragMode] = useState<
        'zoom' | 'pan' | 'select' | 'lasso'
    >('zoom');
    const plotHandleRef = useRef<ScatterPlotHandle>(null);

    return (
        <Paper
            elevation={0}
            sx={{
                mx: 1,
                mb: 1,
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 2,
            }}
        >
            {/* Chart navigation toolbar */}
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
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
                    <Tooltip title="Box select">
                        <Button
                            variant={
                                dragMode === 'select'
                                    ? 'contained'
                                    : 'outlined'
                            }
                            onClick={() =>
                                setDragMode('select')
                            }
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <SelectIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Zoom in">
                        <Button
                            onClick={() =>
                                plotHandleRef.current?.zoomIn()
                            }
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <AddIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Zoom out">
                        <Button
                            onClick={() =>
                                plotHandleRef.current?.zoomOut()
                            }
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <RemoveIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                    <Tooltip title="Reset zoom">
                        <Button
                            onClick={() =>
                                plotHandleRef.current?.resetZoom()
                            }
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <RestartAltIcon fontSize="small" />
                        </Button>
                    </Tooltip>
                </ButtonGroup>

                <Box
                    sx={{
                        display: 'flex',
                        gap: 0.25,
                    }}
                >
                    {onDownloadCSV && (
                        <Tooltip title="Download CSV">
                            <IconButton
                                size="small"
                                onClick={onDownloadCSV}
                                disabled={!hasData}
                                sx={{ p: 0.5 }}
                            >
                                <DownloadIcon fontSize="small" />
                            </IconButton>
                        </Tooltip>
                    )}
                    <Tooltip title="Chart settings">
                        <IconButton
                            size="small"
                            onClick={e =>
                                setSettingsAnchor(
                                    e.currentTarget
                                )
                            }
                            sx={{ p: 0.5 }}
                        >
                            <TuneIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>
                </Box>
            </Box>

            <Box
                sx={{
                    height: {
                        xs: '350px',
                        sm: '450px',
                        md: '550px',
                    },
                    position: 'relative',
                }}
            >

                {/* Settings popover */}
                <UsageFilters
                    anchorEl={settingsAnchor}
                    onClose={() => setSettingsAnchor(null)}
                    availableAttempts={availableAttempts}
                    availableStatuses={availableStatuses}
                    availableResourceClusters={
                        availableResourceClusters
                    }
                    availableTaskNames={availableTaskNames}
                    selectedAttempts={selectedAttempts}
                    selectedStatuses={selectedStatuses}
                    selectedResourceClusters={
                        selectedResourceClusters
                    }
                    selectedTaskNames={selectedTaskNames}
                    showResourceZones={showResourceZones}
                    onSelectedAttemptsChange={
                        onSelectedAttemptsChange
                    }
                    onSelectedStatusesChange={
                        onSelectedStatusesChange
                    }
                    onSelectedResourceClustersChange={
                        onSelectedResourceClustersChange
                    }
                    onSelectedTaskNamesChange={
                        onSelectedTaskNamesChange
                    }
                    onShowResourceZonesChange={
                        onShowResourceZonesChange
                    }
                    onResetFilters={onResetFilters}
                />

                {isLoading ? (
                    <Box sx={{ p: 3, height: '100%' }}>
                        <Skeleton
                            variant="rectangular"
                            height="100%"
                            sx={{ borderRadius: 1 }}
                        />
                    </Box>
                ) : filteredScatterData.length > 0 ? (
                    <Box
                        sx={{
                            height: '100%',
                            p: { xs: 1, sm: 2 },
                            pt: 0,
                        }}
                    >
                        <RuntimeMemoryScatterPlot
                            ref={plotHandleRef}
                            data={filteredScatterData}
                            onTaskClick={onTaskClick}
                            medianRequestedRuntime={
                                medianRequestedRuntime
                            }
                            medianRequestedMemory={
                                medianRequestedMemoryGiB
                            }
                            taskTemplateName={taskTemplateName}
                            showResourceZones={showResourceZones}
                            onSelected={onSelected}
                            dragMode={dragMode}
                        />
                    </Box>
                ) : (
                    <Box
                        sx={{
                            height: '100%',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            flexDirection: 'column',
                            color: 'text.secondary',
                            p: 4,
                        }}
                    >
                        <InfoIcon
                            sx={{
                                fontSize: 48,
                                mb: 2,
                                opacity: 0.3,
                            }}
                        />
                        <Typography
                            variant="h6"
                            sx={{ mb: 1, fontWeight: 'medium' }}
                        >
                            No Data Available
                        </Typography>
                        <Typography
                            variant="body2"
                            textAlign="center"
                            sx={{ maxWidth: 400 }}
                        >
                            No data matches the current filter
                            criteria. Try adjusting your filters or
                            check if data exists for this task
                            template.
                        </Typography>
                    </Box>
                )}
            </Box>
        </Paper>
    );
};

export default UsagePlotSection;
