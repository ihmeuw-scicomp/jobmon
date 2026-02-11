// Plot section component for Usage analysis

import React, { useRef, useState } from 'react';
import {
    Box,
    Button,
    ButtonGroup,
    Checkbox,
    Chip,
    FormControl,
    FormControlLabel,
    IconButton,
    InputLabel,
    ListItemText,
    MenuItem,
    OutlinedInput,
    Paper,
    Select,
    Skeleton,
    Switch,
    Tooltip,
    Typography,
} from '@mui/material';
import {
    Info as InfoIcon,
    Download as DownloadIcon,
    ZoomIn as ZoomInIcon,
    PanTool as PanToolIcon,
    Add as AddIcon,
    Remove as RemoveIcon,
    RestartAlt as RestartAltIcon,
    HighlightAlt as SelectIcon,
    Gesture as LassoIcon,
} from '@mui/icons-material';
import RuntimeMemoryScatterPlot, {
    ScatterPlotHandle,
} from './RuntimeMemoryScatterPlot';
import { ScatterDataPoint } from '@jobmon_gui/types/Usage';
import { ResourceCluster } from './usageCalculations';

interface UsagePlotSectionProps {
    isLoading: boolean;
    filteredScatterData: ScatterDataPoint[];
    taskTemplateName: string;
    medianRequestedRuntime?: number;
    medianRequestedMemoryGiB?: number;
    showResourceZones: boolean;
    selectedInstanceIds?: Set<number>;
    onTaskClick: (taskId: number | string) => void;
    onSelected: (selectedPoints: ScatterDataPoint[]) => void;
    onShowResourceZonesChange: (show: boolean) => void;
    onDownloadCSV?: () => void;
    hasData?: boolean;
    availableResourceClusters: ResourceCluster[];
    selectedResourceClusters: Set<string>;
    onSelectedResourceClustersChange: (clusters: Set<string>) => void;
    onResetFilters: () => void;
    hasActiveSelection?: boolean;
    onClearSelection?: () => void;
}

const UsagePlotSection: React.FC<UsagePlotSectionProps> = ({
    isLoading,
    filteredScatterData,
    taskTemplateName,
    medianRequestedRuntime,
    medianRequestedMemoryGiB,
    showResourceZones,
    selectedInstanceIds,
    onTaskClick,
    onSelected,
    onShowResourceZonesChange,
    onDownloadCSV,
    hasData = false,
    availableResourceClusters,
    selectedResourceClusters,
    onSelectedResourceClustersChange,
    onResetFilters,
    hasActiveSelection,
    onClearSelection,
}) => {
    const isResourceFiltered =
        selectedResourceClusters.size <
        availableResourceClusters.length;
    const [dragMode, setDragMode] = useState<
        'zoom' | 'pan' | 'select' | 'lasso'
    >('zoom');
    const plotHandleRef = useRef<ScatterPlotHandle>(null);

    return (
        <Paper
            elevation={0}
            sx={{
                mb: 1,
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 2,
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
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
                    <Tooltip title="Lasso select">
                        <Button
                            variant={
                                dragMode === 'lasso'
                                    ? 'contained'
                                    : 'outlined'
                            }
                            onClick={() =>
                                setDragMode('lasso')
                            }
                            sx={{ minWidth: 0, px: 0.75 }}
                        >
                            <LassoIcon fontSize="small" />
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
                        alignItems: 'center',
                        gap: 1,
                        flex: 1,
                        justifyContent: 'center',
                    }}
                >
                    <FormControl size="small" sx={{ minWidth: 150 }}>
                        <InputLabel id="resource-cluster-filter-label">
                            Resource Clusters
                        </InputLabel>
                        <Select
                            labelId="resource-cluster-filter-label"
                            multiple
                            value={Array.from(
                                selectedResourceClusters
                            )}
                            onChange={event => {
                                const {
                                    target: { value },
                                } = event;
                                onSelectedResourceClustersChange(
                                    new Set(
                                        typeof value === 'string'
                                            ? value.split(',')
                                            : (value as string[])
                                    )
                                );
                            }}
                            input={
                                <OutlinedInput label="Resource Clusters" />
                            }
                            renderValue={selected =>
                                selected.length ===
                                availableResourceClusters.length
                                    ? 'All'
                                    : `${selected.length} selected`
                            }
                            MenuProps={{
                                PaperProps: {
                                    style: { maxHeight: 300 },
                                },
                            }}
                        >
                            {availableResourceClusters.map(cluster => (
                                <MenuItem
                                    key={cluster.id}
                                    value={cluster.id}
                                    dense
                                >
                                    <Checkbox
                                        checked={selectedResourceClusters.has(
                                            cluster.id
                                        )}
                                        size="small"
                                    />
                                    <ListItemText
                                        primary={cluster.label}
                                        primaryTypographyProps={{
                                            fontSize: '0.875rem',
                                        }}
                                    />
                                </MenuItem>
                            ))}
                        </Select>
                    </FormControl>
                    {hasActiveSelection && onClearSelection && (
                        <Chip
                            label="Clear selection"
                            size="small"
                            onDelete={onClearSelection}
                            color="primary"
                            variant="outlined"
                            sx={{ height: 28 }}
                        />
                    )}
                    {isResourceFiltered && (
                        <Chip
                            label="Reset filters"
                            size="small"
                            onClick={onResetFilters}
                            variant="outlined"
                            color="default"
                            sx={{ height: 28 }}
                        />
                    )}
                </Box>

                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.5,
                    }}
                >
                    <FormControlLabel
                        control={
                            <Switch
                                checked={showResourceZones}
                                onChange={() =>
                                    onShowResourceZonesChange(
                                        !showResourceZones
                                    )
                                }
                                color="primary"
                                size="small"
                            />
                        }
                        label={
                            <Typography
                                variant="body2"
                                sx={{ fontSize: '0.8rem' }}
                            >
                                Efficiency Zones
                            </Typography>
                        }
                        sx={{ mr: 0.5 }}
                    />
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
                </Box>
            </Box>

            <Box
                sx={{
                    minHeight: {
                        xs: '350px',
                        sm: '450px',
                        md: '400px',
                    },
                    flex: 1,
                    position: 'relative',
                }}
            >
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
                            selectedInstanceIds={selectedInstanceIds}
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
                            criteria. Try adjusting your filters
                            or check if data exists for this task
                            template.
                        </Typography>
                    </Box>
                )}
            </Box>
        </Paper>
    );
};

export default UsagePlotSection;
