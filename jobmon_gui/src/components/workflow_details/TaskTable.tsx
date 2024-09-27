import React, {useState} from 'react';
import {Link, useLocation} from 'react-router-dom';

import {convertDate, convertDatePST} from '@jobmon_gui/utils/formatters'
import '@jobmon_gui/styles/jobmon_gui.css';
import {FaCircle} from "react-icons/fa";
import {MaterialReactTable, MRT_RowData, useMaterialReactTable} from 'material-react-table';
import {Box, Button, CircularProgress} from '@mui/material';
import {mkConfig, generateCsv, download} from "export-to-csv";
import FileDownloadIcon from '@mui/icons-material/FileDownload';
import {useQuery} from "@tanstack/react-query";
import axios from "axios";
import {task_table_url} from "@jobmon_gui/configs/ApiUrls";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import Typography from "@mui/material/Typography";
import {type Row} from '@tanstack/react-table';
import {useTaskTableColumnsStore} from "@jobmon_gui/stores/task_table";

type TaskTableProps = {
    taskTemplateName: string
    workflowId: number | string
}

type Task = {
    task_command: string
    task_id: number
    task_max_attempts: number
    task_name: string
    task_num_attempts: number
    task_status: string
    task_status_date: string
}
type Tasks = {
    tasks: Task[]
}


export default function TaskTable({taskTemplateName, workflowId}: TaskTableProps) {
    const location = useLocation();
    const columnFilters = useTaskTableColumnsStore()
    const tasks = useQuery({
        queryKey: ["workflow_details", "tasks", workflowId, taskTemplateName],
        queryFn: async () => {
            return axios.get<Tasks>(
                task_table_url + workflowId,
                {
                    ...jobmonAxiosConfig,
                    data: null,
                    params: {tt_name: taskTemplateName}
                }
            ).then((r) => {
                return r.data.tasks
            })
        },
        staleTime: 5000,
        enabled: !!taskTemplateName
    })


    const columns = [
        {
            header: "Task ID",
            accessorKey: "task_id",
            Cell: ({renderedCellValue, row}) => (
                <nav>
                    <Link
                        to={{pathname: `/task_details/${row.original.task_id}`, search: location.search}}
                        key={row.original.task_id}
                    >
                        {renderedCellValue}
                    </Link>
                </nav>
            ),
            filterFn: 'listFilter',
        },
        {
            header: "Task Name",
            accessorKey: "task_name",
        },
        {
            header: "Status",
            accessorKey: "task_status",
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
        },
        {
            header: "Command",
            accessorKey: "task_command",
            enableClickToCopy: true,
            size: 200,
        },
        {
            header: "Num Attempts",
            accessorKey: "task_num_attempts",
        },
        {
            header: "Max Attempts",
            accessorKey: "task_max_attempts",
        },
        {
            header: "Status Date",
            accessorKey: "task_status_date",
            Cell: ({renderedCellValue}) => (
                convertDatePST(renderedCellValue)
            )
        },
    ];


    const [sorting, setSorting] = useState([{
        id: 'task_id',
        desc: false, //sort by age in descending order by default
    }])
    const [pagination, setPagination] = useState({pageIndex: 0, pageSize: 15})

    const setColumnFilters = (updater) => {
        const newColumnFilters = typeof updater === "function" ? updater(columnFilters.get()) : updater;
        columnFilters.set(newColumnFilters)
    }


    const table = useMaterialReactTable({
        data: tasks?.data || [],
        columns: columns,
        initialState: {density: 'comfortable', showColumnFilters: true,},
        enableColumnFilterModes: true,

        state: {

            get columnFilters() {
                return columnFilters.get() //pass controlled state back to the table (overrides internal state)
            },
            get sorting() {
                return sorting
            },
            get pagination() {
                return pagination
            },

        },
        enableColumnResizing: true,
        layoutMode: "grid",
        onColumnFiltersChange: setColumnFilters,
        onSortingChange: setSorting,
        onPaginationChange: setPagination,
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
    });

    const exportToCSV = () => {
        const csv = generateCsv(csvConfig)(tasks?.data);
        download(csvConfig)(csv);
    };

    if (!taskTemplateName) {
        return (<Typography sx={{pt: 5}}>Select a task template from above to view tasks</Typography>)
    }

    if (tasks.isLoading) {
        return (<CircularProgress/>)
    }


    if (tasks.isError) {
        return (<Typography sx={{pt: 5}}>Error loading tasks. Please refresh and try again.</Typography>)
    }

    return (
        <Box p={2} display="flex" justifyContent="center" width="100%">
            <MaterialReactTable table={table}/>
        </Box>
    );
}