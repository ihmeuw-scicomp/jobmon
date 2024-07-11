import React from 'react';
import {Link, useLocation} from 'react-router-dom';

import {convertDate, convertDatePST} from '@jobmon_gui/utils/formatters'
import '@jobmon_gui/styles/jobmon_gui.css';
import {FaCircle} from "react-icons/fa";
import {MaterialReactTable} from 'material-react-table';
import {Box, Button} from '@mui/material';
import {mkConfig, generateCsv, download} from "export-to-csv";
import FileDownloadIcon from '@mui/icons-material/FileDownload';


export default function TaskTable({taskData, loading}) {
    const location = useLocation();

    const workflow_status = [
        {status: "PENDING", circleClass: "bar-pp", label: "PENDING"},
        {status: "SCHEDULED", circleClass: "bar-ss", label: "SCHEDULED"},
        {status: "RUNNING", circleClass: "bar-rr", label: "RUNNING"},
        {status: "FATAL", circleClass: "bar-ff", label: "FATAL"},
        {status: "DONE", circleClass: "bar-dd", label: "DONE"}
    ];

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
                convertDatePST(convertDate(renderedCellValue).toISOString())
            )
        },
    ];

    const csvConfig = mkConfig({
        fieldSeparator: ',',
        decimalSeparator: '.',
        useKeysAsHeaders: true,
    });

    const exportToCSV = () => {
        const csv = generateCsv(csvConfig)(taskData);
        download(csvConfig)(csv);
    };

    return (
        <div>
            {loading &&
                <div>
                    <br/>
                    <div className="loader"/>
                </div>
            }
            <Button
                onClick={exportToCSV}
                startIcon={<FileDownloadIcon/>}
            >
                Export All Data
            </Button>
            {loading === false &&
                <Box p={2} display="flex" justifyContent="center" width="100%">
                    <MaterialReactTable columns={columns} data={taskData}/>
                </Box>
            }
        </div>
    );
}