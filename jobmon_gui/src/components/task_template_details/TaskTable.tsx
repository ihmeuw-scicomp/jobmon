import React, { useState, useEffect, useRef } from 'react';
import { Link, useLocation } from 'react-router-dom';
import '@jobmon_gui/styles/jobmon_gui.css';
import {
    createMRTColumnHelper,
    MaterialReactTable,
    MRT_RowData,
    useMaterialReactTable,
} from 'material-react-table';
import { Box, Button, TextField } from '@mui/material';
import { mkConfig, generateCsv, download } from 'export-to-csv';
import FileDownloadIcon from '@mui/icons-material/FileDownload';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { useQueryClient } from '@tanstack/react-query';
import { type Row } from '@tanstack/react-table';
import { useTaskTableStore } from '@jobmon_gui/stores/TaskTable.ts';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { formatDayjsDate } from '@jobmon_gui/utils/DayTime.ts';
import { getTaskDetailsQueryFn } from '@jobmon_gui/queries/GetTaskDetails.ts';
import {
    TaskInstanceRow,
    TaskTableProps,
} from '@jobmon_gui/types/TaskTable.ts';
import { JobmonModal } from '@jobmon_gui/components/JobmonModal.tsx';
import { ScrollableCodeBlock } from '@jobmon_gui/components/ScrollableTextArea.tsx';
import {
    getStatusLabel,
    getStatusColor,
} from '@jobmon_gui/constants/taskStatus';
import humanizeDuration from 'humanize-duration';

export default function TaskTable({
    data,
    isLoading,
    taskTemplateName,
    workflowId,
    onFilteredInstanceIdsChange,
}: TaskTableProps) {
    dayjs.extend(utc);
    const queryClient = useQueryClient();
    const columnHelper = createMRTColumnHelper<TaskInstanceRow>();
    const location = useLocation();
    const taskTableStore = useTaskTableStore();
    const [selectedCommand, setSelectedCommand] = useState<string>('');

    const handleCommandClick = (command: string) => {
        setSelectedCommand(command);
    };

    const handleCopyCommand = () => {
        navigator.clipboard.writeText(selectedCommand).catch(err => {
            console.error('Failed to copy command:', err);
        });
    };

    const handleCloseModal = () => {
        setSelectedCommand('');
    };

    const columns = [
        columnHelper.accessor('task_id', {
            header: 'Task ID',
            size: 100,
            grow: false,
            Cell: ({ renderedCellValue, row }) => (
                <nav>
                    <Link
                        to={{
                            pathname: `/task_details/${row.original.task_id}`,
                            search: location.search,
                        }}
                        key={`${row.original.task_id}_${row.original.attempt_number}`}
                        onMouseEnter={async () => {
                            queryClient.prefetchQuery({
                                queryKey: [
                                    'task_details',
                                    row.original.task_id,
                                ],
                                queryFn: getTaskDetailsQueryFn,
                            });
                        }}
                    >
                        {renderedCellValue}
                    </Link>
                </nav>
            ),
            filterFn: 'listFilter',
        }),
        columnHelper.accessor('task_name', {
            header: 'Task Name',
            size: 150,
            grow: 1,
        }),
        columnHelper.accessor('attempt_number', {
            header: 'Attempt #',
            size: 90,
            grow: false,
        }),
        columnHelper.accessor('instance_status', {
            header: 'Status',
            size: 140,
            grow: false,
            Cell: ({ row }) => {
                const status = row.original.instance_status;
                const label = getStatusLabel(status);
                const color = getStatusColor(status);
                return (
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 0.75,
                        }}
                    >
                        <Box
                            sx={{
                                width: 10,
                                height: 10,
                                borderRadius: '50%',
                                backgroundColor: color,
                                flexShrink: 0,
                            }}
                        />
                        {label}
                    </Box>
                );
            },
        }),
        columnHelper.accessor('task_command', {
            header: 'Command',
            size: 200,
            grow: 2,
            Cell: ({ cell }) => (
                <Box
                    onClick={() =>
                        handleCommandClick(cell.getValue() as string)
                    }
                    sx={{
                        cursor: 'pointer',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        '&:hover': { textDecoration: 'underline' },
                    }}
                >
                    {cell.getValue() as string}
                </Box>
            ),
        }),
        columnHelper.accessor('task_num_attempts', {
            header: 'Total Attempts',
            size: 110,
            grow: false,
        }),
        columnHelper.accessor('task_max_attempts', {
            header: 'Max Attempts',
            size: 110,
            grow: false,
        }),
        columnHelper.accessor('task_status_date', {
            header: 'Status Date',
            size: 280,
            grow: false,
            enableColumnFilterModes: false,
            filterFn: (row, _columnId, filterValue) => {
                const rowDate = row.getValue<dayjs.Dayjs>(
                    'task_status_date'
                );
                if (!filterValue || !rowDate || !dayjs.isDayjs(rowDate))
                    return true;
                if (
                    Array.isArray(filterValue) &&
                    filterValue.length === 2
                ) {
                    const [from, to] = filterValue;
                    if (
                        from &&
                        dayjs.isDayjs(from) &&
                        rowDate.isBefore(from)
                    )
                        return false;
                    if (to && dayjs.isDayjs(to) && rowDate.isAfter(to))
                        return false;
                    return true;
                }
                if (dayjs.isDayjs(filterValue)) {
                    return rowDate.isAfter(filterValue);
                }
                return true;
            },
            Filter: ({ column }) => {
                const raw = column.getFilterValue();
                let currentFrom: dayjs.Dayjs | undefined;
                let currentTo: dayjs.Dayjs | undefined;
                if (Array.isArray(raw) && raw.length === 2) {
                    currentFrom = dayjs.isDayjs(raw[0])
                        ? raw[0]
                        : undefined;
                    currentTo = dayjs.isDayjs(raw[1])
                        ? raw[1]
                        : undefined;
                } else if (dayjs.isDayjs(raw)) {
                    currentFrom = raw;
                }
                return (
                    <Box
                        sx={{
                            display: 'flex',
                            gap: 0.5,
                            pt: 0.5,
                            alignItems: 'center',
                        }}
                    >
                        <TextField
                            type="datetime-local"
                            size="small"
                            label="From"
                            value={
                                currentFrom
                                    ? currentFrom.format(
                                          'YYYY-MM-DDTHH:mm'
                                      )
                                    : ''
                            }
                            onChange={e => {
                                const from = e.target.value
                                    ? dayjs(e.target.value)
                                    : '';
                                const to = currentTo || '';
                                if (!from && !to) {
                                    column.setFilterValue(undefined);
                                } else {
                                    column.setFilterValue([from, to]);
                                }
                            }}
                            InputLabelProps={{ shrink: true }}
                            sx={{
                                flex: 1,
                                '& input': {
                                    fontSize: '0.75rem',
                                    py: 0.5,
                                },
                            }}
                        />
                        <TextField
                            type="datetime-local"
                            size="small"
                            label="To"
                            value={
                                currentTo
                                    ? currentTo.format(
                                          'YYYY-MM-DDTHH:mm'
                                      )
                                    : ''
                            }
                            onChange={e => {
                                const to = e.target.value
                                    ? dayjs(e.target.value)
                                    : '';
                                const from = currentFrom || '';
                                if (!from && !to) {
                                    column.setFilterValue(undefined);
                                } else {
                                    column.setFilterValue([from, to]);
                                }
                            }}
                            InputLabelProps={{ shrink: true }}
                            sx={{
                                flex: 1,
                                '& input': {
                                    fontSize: '0.75rem',
                                    py: 0.5,
                                },
                            }}
                        />
                    </Box>
                );
            },
            Cell: ({ cell }) => {
                const rawValue = cell.getValue() as dayjs.Dayjs;
                return formatDayjsDate(rawValue);
            },
        }),
        columnHelper.accessor('runtime_seconds', {
            header: 'Runtime',
            size: 130,
            grow: false,
            Cell: ({ cell }) => {
                const val = cell.getValue() as number | null | undefined;
                if (val == null) return 'N/A';
                return humanizeDuration(val * 1000, {
                    largest: 2,
                    round: true,
                });
            },
        }),
        columnHelper.accessor('memory_gib', {
            header: 'Memory (GiB)',
            size: 110,
            grow: false,
            Cell: ({ cell }) => {
                const val = cell.getValue() as number | null | undefined;
                if (val == null) return 'N/A';
                return val.toFixed(2);
            },
        }),
    ];

    const table = useMaterialReactTable({
        data: data,
        columns: columns,
        initialState: {
            density: 'compact',
            showColumnFilters: true,
            pagination: { pageSize: 25, pageIndex: 0 },
        },
        enableColumnFilterModes: true,

        state: {
            isLoading: isLoading,
            pagination: taskTableStore.getPagination(),
            columnFilters: taskTableStore.getFilters(),
            sorting: taskTableStore.getSorting(),
            columnOrder: taskTableStore.getColumnOrder(),
            density: taskTableStore.getDensity(),
            columnVisibility: taskTableStore.getColumnVisibility(),
            showColumnFilters: taskTableStore.getFilterVisibility(),
        },
        enableColumnResizing: true,
        layoutMode: 'grid',
        onPaginationChange: s => {
            taskTableStore.setPagination(s);
        },
        onColumnFiltersChange: s => {
            taskTableStore.setFilters(s);
        },
        onSortingChange: s => {
            taskTableStore.setSorting(s);
        },
        onColumnOrderChange: s => {
            taskTableStore.setColumnOrder(s);
        },
        onDensityChange: s => {
            taskTableStore.setDensity(s);
        },
        onColumnVisibilityChange: s => {
            taskTableStore.setColumnVisibility(s);
        },
        onShowColumnFiltersChange: s => {
            taskTableStore.setFilterVisibility(s);
        },

        filterFns: {
            listFilter: <TData extends MRT_RowData>(
                row: Row<TData>,
                id: string,
                filterValue: number | string
            ) => {
                return filterValue
                    .toString()
                    .toLowerCase()
                    .trim()
                    .split(',')
                    .map(item => item.trim())
                    .includes(
                        row
                            .getValue<number | string>(id)
                            .toString()
                            .toLowerCase()
                            .trim()
                    );
            },
        },
        renderTopToolbarCustomActions: _table => {
            return (
                <Box>
                    <Button
                        onClick={exportToCSV}
                        startIcon={<FileDownloadIcon />}
                    >
                        Export All Data
                    </Button>
                </Box>
            );
        },
    });

    // Report filtered instance IDs back to parent for backward-apply
    const columnFilters = taskTableStore.getFilters();
    const prevFilteredKeyRef = useRef<string>('');

    useEffect(() => {
        if (!onFilteredInstanceIdsChange) return;
        if (isLoading || data.length === 0) return;

        if (columnFilters.length === 0) {
            if (prevFilteredKeyRef.current !== '') {
                prevFilteredKeyRef.current = '';
                onFilteredInstanceIdsChange(null);
            }
            return;
        }

        const filteredRows = table.getFilteredRowModel().rows;
        const ids = new Set(
            filteredRows.map(
                row => row.original.task_instance_id
            )
        );
        const key = [...ids].sort().join(',');
        if (key !== prevFilteredKeyRef.current) {
            prevFilteredKeyRef.current = key;
            onFilteredInstanceIdsChange(ids);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [columnFilters, data, isLoading, onFilteredInstanceIdsChange]);

    const csvConfig = mkConfig({
        fieldSeparator: ',',
        decimalSeparator: '.',
        useKeysAsHeaders: true,
        filename: `Jobmon_Workflow_${workflowId}_TaskInstances`,
    });

    const exportToCSV = () => {
        const csvData = data.map(r => ({
            ...r,
            task_status_date: formatDayjsDate(r.task_status_date),
        }));
        const csv = generateCsv(csvConfig)(csvData);
        download(csvConfig)(csv);
    };

    if (!taskTemplateName) {
        return (
            <Box sx={{ pt: 5 }}>
                Could not retrieve task instances for this template.
            </Box>
        );
    }

    return (
        <Box sx={{ px: 1, width: '100%' }}>
            <MaterialReactTable table={table} />
            <JobmonModal
                title="Command"
                open={!!selectedCommand}
                onClose={handleCloseModal}
                width="80%"
            >
                <ScrollableCodeBlock>
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
                        {selectedCommand}
                    </pre>
                </ScrollableCodeBlock>
                <Box
                    sx={{
                        display: 'flex',
                        justifyContent: 'flex-end',
                        mt: 2,
                    }}
                >
                    <Button
                        variant="contained"
                        startIcon={<ContentCopyIcon />}
                        onClick={handleCopyCommand}
                    >
                        Copy
                    </Button>
                </Box>
            </JobmonModal>
        </Box>
    );
}
