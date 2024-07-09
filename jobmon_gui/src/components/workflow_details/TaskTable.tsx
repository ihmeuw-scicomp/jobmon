import React from 'react';
import {Link, useLocation} from 'react-router-dom';
import 'react-bootstrap-table2-filter/dist/react-bootstrap-table2-filter.min.css';
import 'react-bootstrap-table2-toolkit/dist/react-bootstrap-table2-toolkit.min.css';

import {convertDate, convertDatePST} from '@jobmon_gui/utils/formatters'
import '@jobmon_gui/styles/jobmon_gui.css';
import {FaCircle} from "react-icons/fa";
import {MaterialReactTable} from 'material-react-table';
import {Box, Typography} from '@mui/material';


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


    return (
        <div>
            {loading &&
                <div>
                    <br/>
                    <div className="loader"/>
                </div>
            }
            {loading === false &&
                <Box p={2}>
                    <Typography variant="h4" gutterBottom>
                        My Table
                    </Typography>
                    <MaterialReactTable columns={columns} data={taskData}/>
                </Box>
            }
        </div>
    );
}