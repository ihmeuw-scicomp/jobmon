import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import BootstrapTable from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import ToolkitProvider, { CSVExport } from 'react-bootstrap-table2-toolkit/dist/react-bootstrap-table2-toolkit';
import filterFactory, { textFilter, numberFilter } from 'react-bootstrap-table2-filter';
import 'react-bootstrap-table2-filter/dist/react-bootstrap-table2-filter.min.css';
import 'react-bootstrap-table2-toolkit/dist/react-bootstrap-table2-toolkit.min.css';

import { convertDate, convertDatePST } from '../functions'
import '../jobmon_gui.css';

export default function TaskTable({ taskData, loading }) {
    const { ExportCSVButton } = CSVExport;
    const [helper, setHelper] = useState("");

    const columns = [
        {
            dataField: "task_id",
            text: "Task ID",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The ID column in TASK table. Unique identifier.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: textFilter(),
            formatter: (cell, row) => <nav>
                <Link
                    to={{ pathname: `/task_details/${cell}` }}
                    key={cell}
                >
                    {cell}
                </Link>
            </nav>
        },
        {
            dataField: "task_name",
            text: "Task Name",
            sort: true,
            style: { overflowWrap: 'break-word' },
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The NAME column in TASK table. The user assigned name of the task.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: textFilter()
        },
        {
            dataField: "task_status",
            text: "Status",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The STATUS column in TASK table. The current status of the task.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: textFilter()
        },
        {
            dataField: "task_command",
            text: "Command",
            sort: true,
            style: { overflowWrap: 'break-word' },
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The COMMAND column in TASK table. The command the jobmon uses to submit the task to the cluster.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: textFilter()
        },
        {
            dataField: "task_num_attempts",
            text: "Num Attempts",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The NUM_ATTEMPTS column in TASK table. The number of attempts the jobmon has tried for this task.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: numberFilter()
        },
        {
            dataField: "task_max_attempts",
            text: "Max Attempts",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The MAX_ATTEMPTS column in TASK table. The max number of attempts the jobmon will retry this task.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: numberFilter()
        },
        {
            dataField: "task_status_date",
            text: "Status Date",
            sort: true,
            sortValue: (cell, row) => convertDate(cell).getTime(),
            formatter: (cell, row, rowIndex) => convertDatePST(cell),
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The STATUS_DATE column in TASK table displayed in PST. The time stamp that the task status was last updated.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            filter: numberFilter()
        },
    ];

    return (
        <div>
            {loading &&
                <div>
                    <br />
                    <div className="loader" />
                </div>
            }
            {loading === false &&
                <ToolkitProvider
                    keyField="task_id"
                    data={taskData}
                    columns={columns}
                    exportCSV={{
                        onlyExportFiltered: true,
                        fileName: 'jobmon_tasks.csv',
                        exportAll: true
                    }}
                >
                    {
                        props => (
                            <div>
                                <ExportCSVButton {...props.csvProps} className="btn btn-dark">Export CSV</ExportCSVButton>
                                <hr />
                                <span className="span-helper"><i>{helper}</i></span>
                                <br/>
                                <BootstrapTable
                                    keyField="task_id"
                                    {...props.baseProps}
                                    bootstrap4
                                    headerClasses="thead-dark"
                                    filter={filterFactory()}
                                    striped
                                    pagination={taskData.length === 0 ? undefined : paginationFactory({ sizePerPage: 10 })}
                                />
                            </div>
                        )
                    }
                </ToolkitProvider>
            }
        </div>
    );
}