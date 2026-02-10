// Filters popover for Usage analysis scatter plot

import React from 'react';
import {
    Box,
    Checkbox,
    Chip,
    Divider,
    FormControl,
    FormControlLabel,
    InputLabel,
    ListItemText,
    MenuItem,
    OutlinedInput,
    Popover,
    Select,
    Switch,
    Typography,
} from '@mui/material';
import { taskStatusMeta } from '@jobmon_gui/constants/taskStatus';
import { ResourceCluster } from './usageCalculations';

export interface UsageFiltersProps {
    anchorEl: HTMLElement | null;
    onClose: () => void;
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
    onResetFilters: () => void;
}

const UsageFilters: React.FC<UsageFiltersProps> = ({
    anchorEl,
    onClose,
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
    return (
        <Popover
            open={Boolean(anchorEl)}
            anchorEl={anchorEl}
            onClose={onClose}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
            slotProps={{
                paper: {
                    sx: {
                        width: 300,
                        maxHeight: '80vh',
                        overflow: 'auto',
                    },
                },
            }}
        >
            <Box
                sx={{
                    p: 2,
                    display: 'flex',
                    flexDirection: 'column',
                    gap: 1.5,
                }}
            >
                {/* Data Filters */}
                <Box>
                    <Typography
                        variant="overline"
                        color="text.secondary"
                        sx={{ fontSize: '0.65rem' }}
                    >
                        Data Filters
                    </Typography>

                    <Box
                        sx={{
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 1.5,
                            mt: 0.5,
                        }}
                    >
                        <FormControl size="small" fullWidth>
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
                                input={
                                    <OutlinedInput label="Attempts" />
                                }
                                renderValue={selected =>
                                    selected.length ===
                                    availableAttempts.length
                                        ? 'All'
                                        : `${selected.length} selected`
                                }
                                MenuProps={{
                                    PaperProps: {
                                        style: { maxHeight: 200 },
                                    },
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

                        <FormControl size="small" fullWidth>
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
                                input={
                                    <OutlinedInput label="Status" />
                                }
                                renderValue={selected =>
                                    selected.length ===
                                    availableStatuses.length
                                        ? 'All'
                                        : selected
                                              .map(
                                                  s =>
                                                      taskStatusMeta[s]
                                                          ?.label || s
                                              )
                                              .join(', ')
                                }
                                MenuProps={{
                                    PaperProps: {
                                        style: { maxHeight: 200 },
                                    },
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

                        <FormControl size="small" fullWidth>
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
                                {availableResourceClusters.map(
                                    cluster => (
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
                                    )
                                )}
                            </Select>
                        </FormControl>

                        <FormControl size="small" fullWidth>
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
                                input={
                                    <OutlinedInput label="Task Names" />
                                }
                                renderValue={selected =>
                                    selected.length ===
                                    availableTaskNames.length
                                        ? 'All'
                                        : `${selected.length} selected`
                                }
                                MenuProps={{
                                    PaperProps: {
                                        style: { maxHeight: 300 },
                                    },
                                }}
                            >
                                {availableTaskNames.map(taskName => (
                                    <MenuItem
                                        key={taskName}
                                        value={taskName}
                                        dense
                                    >
                                        <Checkbox
                                            checked={selectedTaskNames.has(
                                                taskName
                                            )}
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
                </Box>

                <Divider />

                {/* View Options */}
                <Box>
                    <Typography
                        variant="overline"
                        color="text.secondary"
                        sx={{ fontSize: '0.65rem' }}
                    >
                        View Options
                    </Typography>
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
                        sx={{ ml: 0, mt: 0.5 }}
                    />

                    {showResourceZones && (
                        <Box
                            sx={{
                                display: 'flex',
                                flexWrap: 'wrap',
                                gap: 1,
                                p: 1,
                                mt: 0.5,
                                bgcolor: 'grey.50',
                                borderRadius: 1,
                                border: '1px solid',
                                borderColor: 'grey.200',
                            }}
                        >
                            {[
                                {
                                    color: 'rgba(76, 175, 80, 0.4)',
                                    border: 'rgba(76, 175, 80, 0.6)',
                                    label: 'Optimal',
                                },
                                {
                                    color: 'rgba(255, 193, 7, 0.4)',
                                    border: 'rgba(255, 193, 7, 0.6)',
                                    label: 'Wasteful',
                                },
                                {
                                    color: 'rgba(244, 67, 54, 0.4)',
                                    border: 'rgba(244, 67, 54, 0.6)',
                                    label: 'Risk',
                                },
                                {
                                    color: 'rgba(156, 39, 176, 0.4)',
                                    border: 'rgba(156, 39, 176, 0.6)',
                                    label: 'Mixed',
                                },
                            ].map(zone => (
                                <Box
                                    key={zone.label}
                                    display="flex"
                                    alignItems="center"
                                    gap={0.5}
                                >
                                    <Box
                                        sx={{
                                            width: 8,
                                            height: 8,
                                            backgroundColor: zone.color,
                                            borderRadius: 0.5,
                                            border: `1px solid ${zone.border}`,
                                        }}
                                    />
                                    <Typography
                                        variant="caption"
                                        sx={{ fontSize: '0.6rem' }}
                                    >
                                        {zone.label}
                                    </Typography>
                                </Box>
                            ))}
                        </Box>
                    )}
                </Box>

                <Divider />

                {/* Reset */}
                <Box>
                    <Chip
                        label="Reset filters"
                        size="small"
                        onClick={onResetFilters}
                        variant="outlined"
                        color="primary"
                    />
                </Box>
            </Box>
        </Popover>
    );
};

export default UsageFilters;
