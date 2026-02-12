// Filters component for Usage analysis

import React from 'react';
import {
    Box,
    Checkbox,
    Chip,
    FormControl,
    FormControlLabel,
    Grid,
    IconButton,
    InputLabel,
    ListItemText,
    MenuItem,
    OutlinedInput,
    Paper,
    Select,
    Switch,
    Tooltip,
    Typography,
} from '@mui/material';
import {
    Clear as ClearIcon,
    Refresh as RefreshIcon,
} from '@mui/icons-material';
import { taskStatusMeta } from '@jobmon_gui/constants/taskStatus';
import { ResourceCluster } from './usageCalculations';

interface UsageFiltersProps {
    availableAttempts: string[];
    availableStatuses: string[];
    availableResourceClusters: ResourceCluster[];
    availableTaskNames: string[];
    selectedAttempts: Set<string>;
    selectedStatuses: Set<string>;
    selectedResourceClusters: Set<string>;
    selectedTaskNames: Set<string>;
    showResourceZones: boolean;
    onSelectedAttemptsChange: (attempts: Set<string>) => void;
    onSelectedStatusesChange: (statuses: Set<string>) => void;
    onSelectedResourceClustersChange: (clusters: Set<string>) => void;
    onSelectedTaskNamesChange: (taskNames: Set<string>) => void;
    onShowResourceZonesChange: (show: boolean) => void;
    onClearFilters: () => void;
    onResetFilters: () => void;
}

const UsageFilters: React.FC<UsageFiltersProps> = ({
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
    onClearFilters,
    onResetFilters,
}) => {
    return (
        <Paper
            elevation={0}
            sx={{
                p: 2,
                mb: 2,
                mx: { xs: 1, sm: 2 },
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 2,
            }}
        >
            <Box
                display="flex"
                alignItems="center"
                justifyContent="space-between"
                mb={2}
            >
                <Typography
                    variant="subtitle1"
                    fontWeight="bold"
                    color="primary.main"
                >
                    Filters & Controls
                </Typography>
                <Box display="flex" gap={1}>
                    <Tooltip title="Clear all filters">
                        <IconButton
                            size="small"
                            onClick={onClearFilters}
                            sx={{
                                bgcolor: 'grey.100',
                                '&:hover': { bgcolor: 'grey.200' },
                            }}
                        >
                            <ClearIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>
                    <Tooltip title="Reset to defaults">
                        <IconButton
                            size="small"
                            onClick={onResetFilters}
                            sx={{
                                bgcolor: 'primary.50',
                                color: 'primary.main',
                                '&:hover': { bgcolor: 'primary.100' },
                            }}
                        >
                            <RefreshIcon fontSize="small" />
                        </IconButton>
                    </Tooltip>
                </Box>
            </Box>

            <Grid container spacing={1.5} alignItems="flex-start">
                {/* Data Filters - expanded to take more space */}
                <Grid item xs={12} md={8}>
                    <Typography
                        variant="caption"
                        color="text.secondary"
                        fontWeight="medium"
                        sx={{
                            mb: 1,
                            display: 'block',
                            height: '1.2em',
                            lineHeight: '1.2em',
                        }}
                    >
                        DATA FILTERS
                    </Typography>
                    <Box display="flex" gap={2} flexWrap="wrap">
                        <FormControl size="small" sx={{ minWidth: 200 }}>
                            <InputLabel id="attempt-filter-label">
                                Attempts
                            </InputLabel>
                            <Select
                                labelId="attempt-filter-label"
                                multiple
                                value={Array.from(selectedAttempts)}
                                onChange={event => {
                                    const {
                                        target: { value },
                                    } = event;
                                    onSelectedAttemptsChange(
                                        new Set(
                                            typeof value === 'string'
                                                ? value.split(',')
                                                : (value as string[])
                                        )
                                    );
                                }}
                                input={<OutlinedInput label="Attempts" />}
                                renderValue={selected => (
                                    <Box
                                        display="flex"
                                        flexWrap="wrap"
                                        gap={0.5}
                                    >
                                        {selected.slice(0, 2).map(value => (
                                            <Chip
                                                key={value}
                                                label={`#${value}`}
                                                size="small"
                                            />
                                        ))}
                                        {selected.length > 2 && (
                                            <Chip
                                                label={`+${selected.length - 2}`}
                                                size="small"
                                                variant="outlined"
                                            />
                                        )}
                                    </Box>
                                )}
                                MenuProps={{
                                    PaperProps: { style: { maxHeight: 200 } },
                                }}
                            >
                                {availableAttempts.map(attempt => (
                                    <MenuItem
                                        key={attempt}
                                        value={attempt}
                                        dense
                                    >
                                        <Checkbox
                                            checked={selectedAttempts.has(
                                                attempt
                                            )}
                                            size="small"
                                        />
                                        <ListItemText
                                            primary={`Attempt ${attempt}`}
                                        />
                                    </MenuItem>
                                ))}
                            </Select>
                        </FormControl>

                        <FormControl size="small" sx={{ minWidth: 200 }}>
                            <InputLabel id="status-filter-label">
                                Status
                            </InputLabel>
                            <Select
                                labelId="status-filter-label"
                                multiple
                                value={Array.from(selectedStatuses)}
                                onChange={event => {
                                    const {
                                        target: { value },
                                    } = event;
                                    onSelectedStatusesChange(
                                        new Set(
                                            typeof value === 'string'
                                                ? value.split(',')
                                                : (value as string[])
                                        )
                                    );
                                }}
                                input={<OutlinedInput label="Status" />}
                                renderValue={selected => (
                                    <Box
                                        display="flex"
                                        flexWrap="wrap"
                                        gap={0.5}
                                    >
                                        {selected.slice(0, 1).map(value => (
                                            <Chip
                                                key={value}
                                                label={
                                                    taskStatusMeta[value]
                                                        ?.label || value
                                                }
                                                size="small"
                                                color={
                                                    value === 'D'
                                                        ? 'success'
                                                        : 'error'
                                                }
                                                variant="outlined"
                                            />
                                        ))}
                                        {selected.length > 1 && (
                                            <Chip
                                                label={`+${selected.length - 1}`}
                                                size="small"
                                                variant="outlined"
                                            />
                                        )}
                                    </Box>
                                )}
                                MenuProps={{
                                    PaperProps: { style: { maxHeight: 200 } },
                                }}
                            >
                                {availableStatuses.map(statusKey => (
                                    <MenuItem
                                        key={statusKey}
                                        value={statusKey}
                                        dense
                                    >
                                        <Checkbox
                                            checked={selectedStatuses.has(
                                                statusKey
                                            )}
                                            size="small"
                                        />
                                        <ListItemText
                                            primary={
                                                taskStatusMeta[statusKey]
                                                    ?.label || statusKey
                                            }
                                        />
                                    </MenuItem>
                                ))}
                            </Select>
                        </FormControl>

                        <FormControl size="small" sx={{ minWidth: 250 }}>
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
                                input={
                                    <OutlinedInput label="Resource Clusters" />
                                }
                                renderValue={selected => (
                                    <Box
                                        display="flex"
                                        flexWrap="wrap"
                                        gap={0.5}
                                    >
                                        {selected.slice(0, 1).map(value => {
                                            const cluster =
                                                availableResourceClusters.find(
                                                    c => c.id === value
                                                );
                                            return (
                                                <Chip
                                                    key={value}
                                                    label={
                                                        cluster
                                                            ? `${cluster.id} (${cluster.taskCount})`
                                                            : value
                                                    }
                                                    size="small"
                                                    color="primary"
                                                    variant="outlined"
                                                />
                                            );
                                        })}
                                        {selected.length > 1 && (
                                            <Chip
                                                label={`+${selected.length - 1}`}
                                                size="small"
                                                variant="outlined"
                                            />
                                        )}
                                    </Box>
                                )}
                                MenuProps={{
                                    PaperProps: { style: { maxHeight: 300 } },
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

                        <FormControl size="small" sx={{ minWidth: 250 }}>
                            <InputLabel id="task-name-filter-label">
                                Task Names
                            </InputLabel>
                            <Select
                                labelId="task-name-filter-label"
                                multiple
                                value={Array.from(selectedTaskNames)}
                                onChange={event => {
                                    const {
                                        target: { value },
                                    } = event;
                                    onSelectedTaskNamesChange(
                                        new Set(
                                            typeof value === 'string'
                                                ? value.split(',')
                                                : (value as string[])
                                        )
                                    );
                                }}
                                input={<OutlinedInput label="Task Names" />}
                                renderValue={selected => (
                                    <Box
                                        display="flex"
                                        flexWrap="wrap"
                                        gap={0.5}
                                    >
                                        {selected.slice(0, 1).map(value => (
                                            <Chip
                                                key={value}
                                                label={
                                                    value.length > 20
                                                        ? `${value.substring(0, 20)}...`
                                                        : value
                                                }
                                                size="small"
                                                color="secondary"
                                                variant="outlined"
                                            />
                                        ))}
                                        {selected.length > 1 && (
                                            <Chip
                                                label={`+${selected.length - 1}`}
                                                size="small"
                                                variant="outlined"
                                            />
                                        )}
                                    </Box>
                                )}
                                MenuProps={{
                                    PaperProps: { style: { maxHeight: 300 } },
                                }}
                            >
                                {availableTaskNames.map(taskName => (
                                    <MenuItem
                                        key={taskName}
                                        value={taskName}
                                        dense
                                    >
                                        <Checkbox
                                            checked={selectedTaskNames.has(taskName)}
                                            size="small"
                                        />
                                        <ListItemText
                                            primary={taskName}
                                            primaryTypographyProps={{
                                                fontSize: '0.875rem',
                                            }}
                                        />
                                    </MenuItem>
                                ))}
                            </Select>
                        </FormControl>
                    </Box>
                </Grid>

                {/* Resource Efficiency Zones - expanded */}
                <Grid item xs={12} md={4}>
                    <Typography
                        variant="caption"
                        color="text.secondary"
                        fontWeight="medium"
                        sx={{
                            mb: 1,
                            display: 'block',
                            height: '1.2em',
                            lineHeight: '1.2em',
                        }}
                    >
                        RESOURCE EFFICIENCY
                    </Typography>
                    <Box display="flex" flexDirection="column" gap={1}>
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
                                    sx={{
                                        fontSize: '0.8rem',
                                        fontWeight: 'medium',
                                    }}
                                >
                                    Show Efficiency Zones
                                </Typography>
                            }
                            sx={{ mr: 0 }}
                        />

                        {showResourceZones && (
                            <Box
                                sx={{
                                    display: 'flex',
                                    flexWrap: 'wrap',
                                    gap: 1,
                                    p: 1,
                                    bgcolor: 'grey.50',
                                    borderRadius: 1,
                                    border: '1px solid',
                                    borderColor: 'grey.200',
                                }}
                            >
                                <Box
                                    display="flex"
                                    alignItems="center"
                                    gap={0.5}
                                >
                                    <Box
                                        sx={{
                                            width: 8,
                                            height: 8,
                                            backgroundColor:
                                                'rgba(76, 175, 80, 0.4)',
                                            borderRadius: 0.5,
                                            border: '1px solid rgba(76, 175, 80, 0.6)',
                                        }}
                                    />
                                    <Typography
                                        variant="caption"
                                        sx={{ fontSize: '0.6rem' }}
                                    >
                                        Optimal
                                    </Typography>
                                </Box>
                                <Box
                                    display="flex"
                                    alignItems="center"
                                    gap={0.5}
                                >
                                    <Box
                                        sx={{
                                            width: 8,
                                            height: 8,
                                            backgroundColor:
                                                'rgba(255, 193, 7, 0.4)',
                                            borderRadius: 0.5,
                                            border: '1px solid rgba(255, 193, 7, 0.6)',
                                        }}
                                    />
                                    <Typography
                                        variant="caption"
                                        sx={{ fontSize: '0.6rem' }}
                                    >
                                        Wasteful
                                    </Typography>
                                </Box>
                                <Box
                                    display="flex"
                                    alignItems="center"
                                    gap={0.5}
                                >
                                    <Box
                                        sx={{
                                            width: 8,
                                            height: 8,
                                            backgroundColor:
                                                'rgba(244, 67, 54, 0.4)',
                                            borderRadius: 0.5,
                                            border: '1px solid rgba(244, 67, 54, 0.6)',
                                        }}
                                    />
                                    <Typography
                                        variant="caption"
                                        sx={{ fontSize: '0.6rem' }}
                                    >
                                        Risk
                                    </Typography>
                                </Box>
                                <Box
                                    display="flex"
                                    alignItems="center"
                                    gap={0.5}
                                >
                                    <Box
                                        sx={{
                                            width: 8,
                                            height: 8,
                                            backgroundColor:
                                                'rgba(156, 39, 176, 0.4)',
                                            borderRadius: 0.5,
                                            border: '1px solid rgba(156, 39, 176, 0.6)',
                                        }}
                                    />
                                    <Typography
                                        variant="caption"
                                        sx={{ fontSize: '0.6rem' }}
                                    >
                                        Mixed
                                    </Typography>
                                </Box>
                            </Box>
                        )}
                    </Box>
                </Grid>
            </Grid>
        </Paper>
    );
};

export default UsageFilters;
