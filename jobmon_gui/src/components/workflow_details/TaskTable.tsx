import React from 'react';
import {Link, useLocation} from 'react-router-dom';
import {AdapterDayjs} from "@mui/x-date-pickers/AdapterDayjs";
import '@jobmon_gui/styles/jobmon_gui.css';
import {FaCircle} from "react-icons/fa";
import {createMRTColumnHelper, MaterialReactTable, MRT_RowData, useMaterialReactTable} from 'material-react-table';
import {Box, Button} from '@mui/material';
import {mkConfig, generateCsv, download} from "export-to-csv";
import FileDownloadIcon from '@mui/icons-material/FileDownload';
import {useQuery, useQueryClient} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import {type Row} from '@tanstack/react-table';
import {useTaskTableStore} from "@jobmon_gui/stores/TaskTable.ts";
import {LocalizationProvider} from "@mui/x-date-pickers";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import {formatDayjsDate} from "@jobmon_gui/utils/DayTime.ts";
import {getTaskDetailsQueryFn} from "@jobmon_gui/queries/GetTaskDetails.ts";
import {getWorkflowTasksQueryFn} from "@jobmon_gui/queries/GetWorkflowTasks.ts";
import {Task, TaskTableProps} from "@jobmon_gui/types/TaskTable.ts";


export default function TaskTable({taskTemplateName, workflowId}: TaskTableProps) {
    dayjs.extend(utc)
    const queryClient = useQueryClient()
    const columnHelper = createMRTColumnHelper<Task>()
    const location = useLocation();
    const taskTableStore = useTaskTableStore()
    const tasks = useQuery({
        queryKey: ["workflow_details", "tasks", workflowId, taskTemplateName],
        queryFn: getWorkflowTasksQueryFn,
        staleTime: 5000,
        enabled: !!taskTemplateName,
        refetchOnMount: false
    })


    const columns = [
        columnHelper.accessor("task_id", {
            header: "Task ID",
            Cell: ({renderedCellValue, row}) => (
                <nav>
                    <Link
                        to={{pathname: `/task_details/${row.original.task_id}`, search: location.search}}
                        key={row.original.task_id}
                        onMouseEnter={async () => {
                            queryClient.prefetchQuery({
                                queryKey: ["task_details", row.original.task_id],
                                queryFn: getTaskDetailsQueryFn
                            })

                        }}
                    >
                        {renderedCellValue}
                    </Link>
                </nav>
            ),
            filterFn: 'listFilter',
        }),
        columnHelper.accessor("task_name", {
            header: "Task Name",
        }),
        columnHelper.accessor("task_status", {
            header: "Status",
            Cell: ({row}) => {
                const status = row.original.task_status;
                const statusData = workflow_status.find(item => item.status === status);
                return (
                    <div>
                        <label className="label-middle"><FaCircle className={statusData.circleClass}/> </label>
                        <label className="label-left">{statusData.label}</label>
                    </div>
                );
            },
        }),
        columnHelper.accessor("task_command", {
            header: "Command",
            enableClickToCopy: true,
            size: 200,
        }),
        columnHelper.accessor("task_num_attempts", {
            header: "Num Attempts",
        }),
        columnHelper.accessor("task_max_attempts", {
            header: "Max Attempts",
        }),
        columnHelper.accessor("task_status_date", {
            header: "Status Date",
            filterVariant: 'datetime-range',
            size: 350,
            Cell: ({renderedCellValue}) => (
                dayjs.isDayjs(renderedCellValue) ? formatDayjsDate(renderedCellValue) : renderedCellValue
            )
        }),
    ];

    const table = useMaterialReactTable({
        data: tasks?.data || [],
        columns: columns,
        initialState: {density: 'comfortable', showColumnFilters: true,},
        enableColumnFilterModes: true,

        state: {
            isLoading: tasks.isLoading,
            pagination: taskTableStore.getPagination(),
            columnFilters: taskTableStore.getFilters(),
            sorting: taskTableStore.getSorting(),
            columnOrder: taskTableStore.getColumnOrder(),
            density: taskTableStore.getDensity(),
            columnVisibility: taskTableStore.getColumnVisibility(),
            showColumnFilters: taskTableStore.getFilterVisibility(),
        },
        enableColumnResizing: true,
        layoutMode: "grid",
        onPaginationChange: (s) => {
            taskTableStore.setPagination(s)
        },
        onColumnFiltersChange: (s) => {
            taskTableStore.setFilters(s)
        },
        onSortingChange: (s) => {
            taskTableStore.setSorting(s)
        },
        onColumnOrderChange: (s) => {
            taskTableStore.setColumnOrder(s)
        },
        onDensityChange: (s) => {
            taskTableStore.setDensity(s)
        },
        onColumnVisibilityChange: (s) => {
            taskTableStore.setColumnVisibility(s)
        },
        onShowColumnFiltersChange: (s) => {
            taskTableStore.setFilterVisibility(s)
        },

        filterFns: {
            listFilter: <TData extends MRT_RowData>(
                row: Row<TData>,
                id: string,
                filterValue: number | string,
            ) => {
                return filterValue.toString().toLowerCase().trim().split(',').map((item) => item.trim()).includes(row.getValue<number | string>(id)
                    .toString()
                    .toLowerCase()
                    .trim())
            }
        },
        renderTopToolbarCustomActions: (table) => {
            return (<Box>
                <Button
                    onClick={exportToCSV}
                    startIcon={<FileDownloadIcon/>}>
                    Export All Data
                </Button>

            </Box>)
        }
    });


    const workflow_status = [
        {status: "PENDING", circleClass: "bar-pp", label: "PENDING"},
        {status: "SCHEDULED", circleClass: "bar-ss", label: "SCHEDULED"},
        {status: "RUNNING", circleClass: "bar-rr", label: "RUNNING"},
        {status: "FATAL", circleClass: "bar-ff", label: "FATAL"},
        {status: "DONE", circleClass: "bar-dd", label: "DONE"}
    ];


    const csvConfig = mkConfig({
        fieldSeparator: ',',
        decimalSeparator: '.',
        useKeysAsHeaders: true,
        filename: `Jobmon_Workflow_${workflowId}_Tasks`
    });

    const exportToCSV = () => {
        // Replace the dayjs objects with strings
        const tasksWithRenderedDates = tasks?.data.map((r) => {
            return {...r, task_status_date: formatDayjsDate(r.task_status_date)}
        })
        const csv = generateCsv(csvConfig)(tasksWithRenderedDates);
        download(csvConfig)(csv);
    };

    if (!taskTemplateName) {
        return (<Typography sx={{pt: 5}}>Select a task template from above to view tasks</Typography>)
    }

    if (tasks.isError) {
        return (<Typography sx={{pt: 5}}>Error loading tasks. Please refresh and try again.</Typography>)
    }

    return (
        <Box p={2} display="flex" justifyContent="center" width="100%">
            <LocalizationProvider dateAdapter={AdapterDayjs}>
                <MaterialReactTable table={table}/>
            </LocalizationProvider>
        </Box>
    );
}