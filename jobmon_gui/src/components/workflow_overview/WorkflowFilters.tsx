import {
    Button,
    Chip,
    Collapse,
    FormControl,
    Grid,
    IconButton,
    InputAdornment,
    InputLabel,
    MenuItem,
    Select,
    Stack,
    TextField,
    Tooltip,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import RemoveIcon from '@mui/icons-material/Remove';
import TextFieldsIcon from '@mui/icons-material/TextFields';
import { DatePicker, LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import {
    useWorkflowSearchSettings,
    WfAttributePair,
    WorkflowSearchSettings,
} from '@jobmon_gui/stores/workflow_settings';
import { useQueryClient } from '@tanstack/react-query';
import { useLocation, useNavigate } from 'react-router-dom';
import {
    settingsToSearchParamsString,
    filterValueToDisplayString,
    getSearchParamsFromLocation,
} from '@jobmon_gui/utils/workflowSearchParams';
import Box from '@mui/material/Box';

/** Status code → human label for chip display */
const STATUS_LABELS: Record<string, string> = {
    A: 'Aborted',
    D: 'Done',
    F: 'Failed',
    G: 'Registering',
    H: 'Halted',
    I: 'Instantiating',
    O: 'Launched',
    Q: 'Queued',
    R: 'Running',
};

export default function WorkflowFilters() {
    const queryClient = useQueryClient();
    const workflowSettings = useWorkflowSearchSettings();
    const location = useLocation();
    const navigate = useNavigate();
    const [showMore, setShowMore] = useState(false);

    useEffect(() => {
        const searchParams = getSearchParamsFromLocation(location.search);
        workflowSettings.loadValuesFromSearchParams(searchParams);
        workflowSettings.triggerDataRefresh();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location.search]);

    const handleClear = () => {
        workflowSettings.clear();
        navigate({ pathname: '/', search: '' }, { replace: false });
        workflowSettings.triggerDataRefresh();
        void queryClient.invalidateQueries({
            queryKey: ['workflow_overview', 'workflows'],
        });
    };

    const handleSubmit = (event: React.FormEvent) => {
        event.preventDefault();
        workflowSettings.applyPendingSettings();

        const searchString = settingsToSearchParamsString(
            workflowSettings.get()
        );
        navigate(
            {
                pathname: '/',
                search: searchString ? `?${searchString}` : '',
            },
            { replace: false }
        );

        workflowSettings.triggerDataRefresh();
        void queryClient.invalidateQueries({
            queryKey: ['workflow_overview', 'workflows'],
        });
    };

    const handleInputChange = <
        K extends keyof WorkflowSearchSettings,
    >(
        key: K,
        value: WorkflowSearchSettings[K]
    ) => {
        workflowSettings.setPendingSetting(key, value);
    };

    const pending = workflowSettings.getPending();
    const applied = workflowSettings.get();
    const attributes: WfAttributePair[] = pending.wf_attributes || [
        { key: '', value: '' },
    ];

    const handleAttrChange = (
        index: number,
        field: 'key' | 'value',
        newValue: string
    ) => {
        const updated = attributes.map((attr, i) =>
            i === index ? { ...attr, [field]: newValue } : attr
        );
        handleInputChange('wf_attributes', updated);
    };

    const handleAddAttr = () => {
        handleInputChange('wf_attributes', [
            ...attributes,
            { key: '', value: '' },
        ]);
    };

    const handleRemoveAttr = (index: number) => {
        const updated = attributes.filter((_, i) => i !== index);
        handleInputChange(
            'wf_attributes',
            updated.length > 0 ? updated : [{ key: '', value: '' }]
        );
    };

    const containsAdornment = (
        field: 'wf_name_contains' | 'wf_args_contains'
    ) => {
        const active = pending[field] || false;
        return (
            <InputAdornment position="end">
                <Tooltip
                    title={
                        active
                            ? 'Substring match (click for exact)'
                            : 'Exact match (click for substring)'
                    }
                >
                    <IconButton
                        size="small"
                        onClick={() => handleInputChange(field, !active)}
                        sx={{
                            color: active
                                ? 'primary.main'
                                : 'action.disabled',
                        }}
                    >
                        <TextFieldsIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
            </InputAdornment>
        );
    };

    // Build chips for active "more" filters so users see them
    // even when the section is collapsed.
    const moreChips: { label: string; onDelete: () => void }[] = [];
    const toolDisplay = filterValueToDisplayString(applied.tool);
    if (toolDisplay) {
        moreChips.push({
            label: `Tool: ${toolDisplay}`,
            onDelete: () => handleInputChange('tool', ''),
        });
    }
    if (applied.wf_args) {
        const prefix = applied.wf_args_contains ? 'Args contains' : 'Args';
        moreChips.push({
            label: `${prefix}: ${applied.wf_args}`,
            onDelete: () => {
                handleInputChange('wf_args', '');
                handleInputChange('wf_args_contains', false);
            },
        });
    }
    if (applied.wf_id) {
        moreChips.push({
            label: `WF ID: ${applied.wf_id}`,
            onDelete: () => handleInputChange('wf_id', ''),
        });
    }
    const appliedAttrs: WfAttributePair[] = applied.wf_attributes || [];
    for (const attr of appliedAttrs) {
        if (attr.key || attr.value) {
            moreChips.push({
                label: `${attr.key || '*'}=${attr.value || '*'}`,
                onDelete: () => {
                    const updated = appliedAttrs.filter(a => a !== attr);
                    handleInputChange(
                        'wf_attributes',
                        updated.length > 0
                            ? updated
                            : [{ key: '', value: '' }]
                    );
                },
            });
        }
    }

    return (
        <Box className="div-level-2" sx={{ mt: 2 }}>
            <form onSubmit={handleSubmit}>
                {/* Primary filters — always visible */}
                <Grid container spacing={2}>
                    <Grid item xs={3}>
                        <TextField
                            label="Username"
                            fullWidth
                            value={filterValueToDisplayString(pending.user)}
                            onChange={e =>
                                handleInputChange('user', e.target.value)
                            }
                        />
                    </Grid>
                    <Grid item xs={3}>
                        <TextField
                            label="Workflow Name"
                            fullWidth
                            placeholder="Supports wildcards (*)"
                            value={pending.wf_name}
                            onChange={e =>
                                handleInputChange('wf_name', e.target.value)
                            }
                            InputProps={{
                                endAdornment: containsAdornment(
                                    'wf_name_contains'
                                ),
                            }}
                        />
                    </Grid>
                    <Grid item xs={2}>
                        <FormControl fullWidth>
                            <InputLabel id="LABEL-workflow-status">
                                Status
                            </InputLabel>
                            <Select
                                labelId="LABEL-workflow-status"
                                label="Status"
                                id={'SELECT-workflow-status'}
                                onChange={e =>
                                    handleInputChange(
                                        'status',
                                        e.target.value
                                    )
                                }
                                value={filterValueToDisplayString(
                                    pending.status
                                )}
                                fullWidth
                            >
                                <MenuItem value="">All</MenuItem>
                                {Object.entries(STATUS_LABELS).map(
                                    ([code, label]) => (
                                        <MenuItem key={code} value={code}>
                                            {label}
                                        </MenuItem>
                                    )
                                )}
                            </Select>
                        </FormControl>
                    </Grid>
                    <Grid item xs={2}>
                        <LocalizationProvider dateAdapter={AdapterDayjs}>
                            <DatePicker
                                label="Date Start"
                                value={
                                    pending.date_submitted
                                        ? dayjs(pending.date_submitted)
                                        : dayjs()
                                }
                                onChange={value =>
                                    handleInputChange(
                                        'date_submitted',
                                        dayjs(value)
                                    )
                                }
                                sx={{ width: '100%' }}
                            />
                        </LocalizationProvider>
                    </Grid>
                    <Grid item xs={2}>
                        <LocalizationProvider dateAdapter={AdapterDayjs}>
                            <DatePicker
                                label="Date End"
                                value={
                                    pending.date_submitted_end
                                        ? dayjs(pending.date_submitted_end)
                                        : dayjs()
                                }
                                onChange={value =>
                                    handleInputChange(
                                        'date_submitted_end',
                                        dayjs(value)
                                    )
                                }
                                sx={{ width: '100%' }}
                            />
                        </LocalizationProvider>
                    </Grid>
                </Grid>

                {/* Active "more" filter chips + toggle */}
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        mt: 1,
                        gap: 1,
                        minHeight: 32,
                    }}
                >
                    <Button
                        size="small"
                        onClick={() => setShowMore(!showMore)}
                        endIcon={
                            showMore ? (
                                <ExpandLessIcon />
                            ) : (
                                <ExpandMoreIcon />
                            )
                        }
                        sx={{ textTransform: 'none', flexShrink: 0 }}
                    >
                        More Filters
                    </Button>
                    {!showMore && moreChips.length > 0 && (
                        <Stack
                            direction="row"
                            spacing={0.5}
                            sx={{ flexWrap: 'wrap', gap: 0.5 }}
                        >
                            {moreChips.map((chip, i) => (
                                <Chip
                                    key={i}
                                    label={chip.label}
                                    size="small"
                                    onDelete={chip.onDelete}
                                    variant="outlined"
                                />
                            ))}
                        </Stack>
                    )}
                </Box>

                {/* Secondary filters — collapsible */}
                <Collapse in={showMore}>
                    <Grid container spacing={2} sx={{ mt: 0 }}>
                        <Grid item xs={4}>
                            <TextField
                                label="Tool"
                                fullWidth
                                value={filterValueToDisplayString(
                                    pending.tool
                                )}
                                onChange={e =>
                                    handleInputChange(
                                        'tool',
                                        e.target.value
                                    )
                                }
                            />
                        </Grid>
                        <Grid item xs={4}>
                            <TextField
                                label="Workflow Args"
                                fullWidth
                                placeholder="Supports wildcards (*)"
                                value={pending.wf_args}
                                onChange={e =>
                                    handleInputChange(
                                        'wf_args',
                                        e.target.value
                                    )
                                }
                                InputProps={{
                                    endAdornment: containsAdornment(
                                        'wf_args_contains'
                                    ),
                                }}
                            />
                        </Grid>
                        <Grid item xs={4}>
                            <TextField
                                label="Workflow ID"
                                fullWidth
                                value={pending.wf_id}
                                onChange={e =>
                                    handleInputChange(
                                        'wf_id',
                                        e.target.value
                                    )
                                }
                            />
                        </Grid>
                        {/* Attribute filter rows — each on its own line */}
                        {attributes.map((attr, index) => (
                            <React.Fragment key={index}>
                                <Grid item xs={4}>
                                    <TextField
                                        label="Attribute Key"
                                        fullWidth
                                        value={attr.key}
                                        onChange={e =>
                                            handleAttrChange(
                                                index,
                                                'key',
                                                e.target.value
                                            )
                                        }
                                    />
                                </Grid>
                                <Grid item xs={4}>
                                    <TextField
                                        label="Attribute Value"
                                        fullWidth
                                        value={attr.value}
                                        onChange={e =>
                                            handleAttrChange(
                                                index,
                                                'value',
                                                e.target.value
                                            )
                                        }
                                    />
                                </Grid>
                                <Grid
                                    item
                                    xs={1}
                                    sx={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: 0.5,
                                    }}
                                >
                                    {attributes.length > 1 && (
                                        <Tooltip title="Remove attribute filter">
                                            <IconButton
                                                size="small"
                                                onClick={() =>
                                                    handleRemoveAttr(index)
                                                }
                                            >
                                                <RemoveIcon fontSize="small" />
                                            </IconButton>
                                        </Tooltip>
                                    )}
                                    {index === attributes.length - 1 && (
                                        <Tooltip title="Add attribute filter">
                                            <IconButton
                                                size="small"
                                                onClick={handleAddAttr}
                                            >
                                                <AddIcon fontSize="small" />
                                            </IconButton>
                                        </Tooltip>
                                    )}
                                </Grid>
                            </React.Fragment>
                        ))}
                    </Grid>
                </Collapse>

                {/* Action buttons */}
                <Grid
                    container
                    spacing={2}
                    justifyContent="center"
                    sx={{ mt: 1 }}
                >
                    <Grid item>
                        <Button variant="contained" type="submit">
                            Submit
                        </Button>
                    </Grid>
                    <Grid item>
                        <Button variant="contained" onClick={handleClear}>
                            Clear All
                        </Button>
                    </Grid>
                </Grid>
            </form>
        </Box>
    );
}
