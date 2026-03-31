import React from 'react';
import Box from '@mui/material/Box';
import Checkbox from '@mui/material/Checkbox';
import Chip from '@mui/material/Chip';
import FormControl from '@mui/material/FormControl';
import IconButton from '@mui/material/IconButton';
import InputLabel from '@mui/material/InputLabel';
import LinearProgress from '@mui/material/LinearProgress';
import ListItemText from '@mui/material/ListItemText';
import MenuItem from '@mui/material/MenuItem';
import OutlinedInput from '@mui/material/OutlinedInput';
import Paper from '@mui/material/Paper';
import Select from '@mui/material/Select';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import { ResourceEfficiencyMetrics } from '@jobmon_gui/types/Usage';
import { ResourceCluster, getEfficiencyStatus } from './usageCalculations';

interface ResourceSummaryBarProps {
    resourceEfficiency: ResourceEfficiencyMetrics;
    expanded: boolean;
    onToggleExpanded: () => void;
    selectedDataCount: number;
    availableResourceClusters: ResourceCluster[];
    selectedResourceClusters: Set<string>;
    onSelectedResourceClustersChange: (clusters: Set<string>) => void;
    onResetFilters: () => void;
    hasActiveSelection: boolean;
    onClearSelection: () => void;
    hasData: boolean;
}

const ResourceSummaryBar: React.FC<ResourceSummaryBarProps> = ({
    resourceEfficiency,
    expanded,
    onToggleExpanded,
    selectedDataCount,
    availableResourceClusters,
    selectedResourceClusters,
    onSelectedResourceClustersChange,
    onResetFilters,
    hasActiveSelection,
    onClearSelection,
    hasData,
}) => {
    const runtimeStatus = getEfficiencyStatus(
        resourceEfficiency.runtimeUtilization
    );
    const memoryStatus = getEfficiencyStatus(
        resourceEfficiency.memoryUtilization
    );
    const isResourceFiltered =
        selectedResourceClusters.size < availableResourceClusters.length;

    return (
        <Paper
            elevation={0}
            sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 2,
            }}
        >
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    gap: 1.5,
                    px: 1.5,
                    py: 0.75,
                    minHeight: 48,
                }}
            >
                {/* Runtime utilization */}
                {hasData && (
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 0.75,
                            minWidth: 180,
                        }}
                    >
                        <Typography
                            variant="caption"
                            fontWeight="bold"
                            color="text.secondary"
                            sx={{ whiteSpace: 'nowrap' }}
                        >
                            Runtime
                        </Typography>
                        <LinearProgress
                            variant="determinate"
                            value={Math.min(
                                resourceEfficiency.runtimeUtilization,
                                100
                            )}
                            sx={{
                                flex: 1,
                                height: 8,
                                borderRadius: 4,
                                minWidth: 60,
                                bgcolor: 'grey.200',
                                '& .MuiLinearProgress-bar': {
                                    bgcolor: runtimeStatus.color,
                                    borderRadius: 4,
                                },
                            }}
                        />
                        <Typography
                            variant="caption"
                            fontWeight="bold"
                            sx={{
                                whiteSpace: 'nowrap',
                                color: runtimeStatus.color,
                            }}
                        >
                            {Math.round(resourceEfficiency.runtimeUtilization)}%
                        </Typography>
                        <Chip
                            label={runtimeStatus.shortLabel}
                            size="small"
                            sx={{
                                height: 20,
                                fontSize: '0.65rem',
                                fontWeight: 'bold',
                                bgcolor: runtimeStatus.bgColor,
                                color: runtimeStatus.color,
                            }}
                        />
                    </Box>
                )}

                {/* Memory utilization */}
                {hasData && (
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 0.75,
                            minWidth: 180,
                        }}
                    >
                        <Typography
                            variant="caption"
                            fontWeight="bold"
                            color="text.secondary"
                            sx={{ whiteSpace: 'nowrap' }}
                        >
                            Memory
                        </Typography>
                        <LinearProgress
                            variant="determinate"
                            value={Math.min(
                                resourceEfficiency.memoryUtilization,
                                100
                            )}
                            sx={{
                                flex: 1,
                                height: 8,
                                borderRadius: 4,
                                minWidth: 60,
                                bgcolor: 'grey.200',
                                '& .MuiLinearProgress-bar': {
                                    bgcolor: memoryStatus.color,
                                    borderRadius: 4,
                                },
                            }}
                        />
                        <Typography
                            variant="caption"
                            fontWeight="bold"
                            sx={{
                                whiteSpace: 'nowrap',
                                color: memoryStatus.color,
                            }}
                        >
                            {Math.round(resourceEfficiency.memoryUtilization)}%
                        </Typography>
                        <Chip
                            label={memoryStatus.shortLabel}
                            size="small"
                            sx={{
                                height: 20,
                                fontSize: '0.65rem',
                                fontWeight: 'bold',
                                bgcolor: memoryStatus.bgColor,
                                color: memoryStatus.color,
                            }}
                        />
                    </Box>
                )}

                {/* Resource cluster dropdown */}
                {availableResourceClusters.length > 1 && (
                    <FormControl size="small" sx={{ minWidth: 150 }}>
                        <InputLabel id="resource-cluster-filter-label">
                            Resource Clusters
                        </InputLabel>
                        <Select
                            labelId="resource-cluster-filter-label"
                            multiple
                            value={Array.from(selectedResourceClusters)}
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
                            input={<OutlinedInput label="Resource Clusters" />}
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
                )}

                {/* Active filter chips */}
                {hasActiveSelection && (
                    <Chip
                        label={`${selectedDataCount} selected`}
                        size="small"
                        onDelete={onClearSelection}
                        color="primary"
                        variant="outlined"
                        sx={{ height: 24 }}
                    />
                )}
                {isResourceFiltered && (
                    <Chip
                        label="Reset filters"
                        size="small"
                        onClick={onResetFilters}
                        variant="outlined"
                        color="default"
                        sx={{ height: 24 }}
                    />
                )}

                {/* Spacer */}
                <Box sx={{ flex: 1 }} />

                {/* Expand/collapse toggle */}
                <Tooltip
                    title={
                        expanded
                            ? 'Collapse resource profiling'
                            : 'Expand resource profiling'
                    }
                >
                    <IconButton size="small" onClick={onToggleExpanded}>
                        {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    </IconButton>
                </Tooltip>
            </Box>
        </Paper>
    );
};

export default ResourceSummaryBar;
