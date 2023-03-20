import React, { useEffect, useState } from 'react';
import axios from 'axios';
import BootstrapTable, { ExpandRowProps } from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import ToolkitProvider, { CSVExport } from 'react-bootstrap-table2-toolkit/dist/react-bootstrap-table2-toolkit';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSquare } from '@fortawesome/free-solid-svg-icons';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import Spinner from 'react-bootstrap/Spinner';
import { Link } from "react-router-dom";
import { convertDate, convertDatePST } from '../functions'

// @ts-ignore
import JobmonProgressBar from '../progress_bar.tsx';

const DEBUG = false

// Get task status count for specified workflows
function getAsyncFetchData(setStatusDict, setFinishedWF, statusD, pre_finished_ids, wf_ids: number[]) {
    const url = process.env.REACT_APP_BASE_URL + "/workflow_status_viz";
    const fetchData = async () => {
        let unfinished_wf_ids: number[] = [];
        let finished_wf_ids: number[] = [];
        // have to convert the unknown type to any to operate
        let all_status: any = statusD;

        for (let i = 0; i < wf_ids.length; i++) {
            let w = wf_ids[i];
            if (statusD[w] === undefined || statusD[w]["PENDING"] + statusD[w]["SCHEDULED"] + statusD[w]["RUNNING"] > 0) {
                unfinished_wf_ids.push(w);
            } else {
                finished_wf_ids.push(w);
            }
        }

        //don't query empty list
        if (unfinished_wf_ids.length > 0) {
            const result = await axios.get(url, { params: { workflow_ids: unfinished_wf_ids } });
            // have to convert the unknown type to any to operate
            let temp_data: any = result.data;

            for (let i = 0; i < unfinished_wf_ids.length; i++) {
                let wf_id = unfinished_wf_ids[i];
                if (temp_data[wf_id]["PENDING"] + temp_data[wf_id]["SCHEDULED"] + temp_data[wf_id]["RUNNING"] === 0) {
                    finished_wf_ids.push(wf_id);
                }
                all_status[wf_id] = temp_data[wf_id];
            }
        }
        setStatusDict(all_status);
        setFinishedWF(finished_wf_ids);
    };
    return fetchData
}

interface Data {
    wf_id: number
    wf_tool: string
    wf_name: string
    wf_args: string
    wf_submitted_date: string
    wf_status_date: string
    wf_status: string
    wfr_count: number
}

export default function JobmonWFTable({ allData }) {
    const [expandedRows, setExpandedRows] = useState([]);
    const [statusDict, setStatusDict] = useState({});
    //TODO: get rid of finishedWF
    //without this extra parameter, the progress bar keeps spinning until next row extension
    //the console.log shows the progress bar gets its info though
    //suspect sync issue, but not sure.
    const [finishedWF, setFinishedWF] = useState<number[]>([]);
    const [helper, setHelper] = useState("");

    // Specify table columns
    const columns = [
        {
            dataField: "wf_id",
            text: "Workflow ID",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The ID column in WORKFLOW table. Unique identifier.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            formatter: (cell, row) => <nav>
                <Link
                    to={{ pathname: `/workflow/${cell}/tasks` }}
                    key={cell}
                >
                    {cell}
                </Link>
            </nav>
        },
        {
            dataField: "wf_tool",
            text: "Tool",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The NAME column in TOOL table. The name of the tool used to create the workflow.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            style: { overflowWrap: 'break-word' }
        },
        {
            dataField: "wf_name",
            text: "Workflow Name",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The NAME column in WORKFLOW table. The user assigned name of the workflow.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            style: { overflowWrap: 'break-word' }
        },
        {
            dataField: "wf_args",
            text: "Workflow Args",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The WORKFLOW_ARGS column in WORKFLOW table. The value of workflow_args used to create the workflow.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            style: { overflowWrap: 'break-word' }
        },
        {
            dataField: "wf_submitted_date",
            text: "Date Submitted",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The CREATED_DATE column in WORKFLOW table in PST. The time stamp that the workflow was created.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            sortValue: (cell, row) => convertDate(cell).getTime(),
            formatter: (cell, row, rowIndex) => convertDatePST(cell)
        },
        {
            dataField: "wf_status_date",
            text: "Status Date",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The STATUS_DATE column in WORKFLOW table in PST. The time stamp that the workflow status was last updated.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
            sortValue: (cell, row) => convertDate(cell).getTime(),
            formatter: (cell, row, rowIndex) => convertDatePST(cell)
        },
        {
            dataField: "wf_status",
            text: "Workflow Status",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The STATUS column in WORKFLOW table. The current status of the workflow.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
        },
        {
            dataField: "wfr_count",
            text: "Number of WorkflowRuns",
            sort: true,
            headerEvents: {
                    onMouseEnter: (e, column, columnIndex) => {
                        setHelper("The number of runs of the workflow.");
                    },
                    onMouseLeave: (e, column, columnIndex) => {
                        setHelper("");
                    }
            },
        }
    ];

    // Render the progress bar when row is expanded
    const expandRow: ExpandRowProps<Data, never> = {
        renderer: row => {
            const entry = statusDict[row.wf_id]
            if (entry === undefined) {
                return (
                    <div>
                        <Spinner animation="border" role="status" variant="primary">
                            <span className="visually-hidden"></span>
                        </Spinner>
                    </div>)
            }

            const tasks = entry["tasks"]
            const pending = entry["PENDING"]
            const scheduled = entry["SCHEDULED"]
            const running = entry["RUNNING"]
            const done = entry["DONE"]
            const fatal = entry["FATAL"]
            const maxc = entry["MAXC"]

            return (
                <div>
                    <JobmonProgressBar tasks={tasks} pending={pending} scheduled={scheduled} running={running} done={done} fatal={fatal} maxc={maxc} placement="top" />
                </div>
            )
        },

        expanded: expandedRows,
        onExpand: (row, isExpand, rowIndex, e) => {

            var newExpandedRows: any = isExpand
                ? [...expandedRows, row.wf_id]
                : expandedRows.filter(x => x !== row.wf_id)

            let wf_ids = newExpandedRows;

            getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, wf_ids)();
            setExpandedRows(newExpandedRows)
        },

        showExpandColumn: true,
        expandByColumnOnly: true,
        expandHeaderColumnRenderer: (isAnyExpands) => (<></>),
        expandColumnRenderer: ({ expanded }) => {
            if (expanded) {
                return (
                    <b>-</b>
                );
            }
            return (
                <b>+</b>
            );
        }
    };

    // Update the progress bar every 60 seconds
    useEffect(() => {
        const interval = setInterval(() => {
            let wf_ids = expandedRows;
            getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, wf_ids)();
        }, 60000);
        return () => clearInterval(interval);
    }, [expandedRows, finishedWF, statusDict]);


    // Create and return the React Bootstrap Table
    const { ExportCSVButton } = CSVExport;
    return (
        <div>

            <ToolkitProvider
                keyField="wf_id"
                data={allData}
                columns={columns}
                exportCSV={{
                    onlyExportFiltered: true,
                    fileName: 'jobmon_workflow.csv',
                    exportAll: true
                }}
            >
                {
                    props => (
                        <div>
                            <div style={{ display: "flex" }}>
                                <OverlayTrigger
                                    trigger={["hover", "focus"]}
                                    placement="bottom-start"
                                    overlay={(
                                        <Popover>
                                            Pending: Tasks that are queued in Jobmon.<br></br>
                                            Scheduled: Tasks that are submitted to the cluster and were queued by the cluster scheduler.<br></br>
                                            Running: Tasks that are running on cluster nodes.<br></br>
                                            Fatal: Tasks that did not finish because of an error.<br></br>
                                            Done: Tasks that completed successfully.<br></br>
                                        </Popover>
                                    )}>
                                    <div id="legend" className="legend">
                                        <form>
                                            <label className="label-middle"><FontAwesomeIcon icon={faSquare} className="bar-pp" /> </label>
                                            <label className="label-left">Pending  </label>
                                            <label className="label-middle"><FontAwesomeIcon icon={faSquare} className="bar-ss" /> </label>
                                            <label className="label-left">Scheduled  </label>
                                            <label className="label-middle"><FontAwesomeIcon icon={faSquare} className="bar-rr" /> </label>
                                            <label className="label-left">Running  </label>
                                            <label className="label-middle"><FontAwesomeIcon icon={faSquare} className="bar-ff" /> </label>
                                            <label className="label-left">Fatal  </label>
                                            <label className="label-middle"><FontAwesomeIcon icon={faSquare} className="bar-dd" /> </label>
                                            <label className="label-left" >Done  </label>
                                        </form>
                                    </div>
                                </OverlayTrigger>
                                <ExportCSVButton {...props.csvProps} className="btn btn-dark">Export CSV</ExportCSVButton>
                            </div>
                            <hr />
                            <span className="span-helper"><i>{helper}</i></span>
                            <br/>
                            <BootstrapTable
                                keyField="wf_id"
                                {...props.baseProps}
                                bootstrap4
                                expandRow={expandRow}
                                headerClasses="thead-dark"
                                striped
                                pagination={allData.length === 0 ? undefined : paginationFactory({ sizePerPage: 10 })}
                            />
                        </div>
                    )
                }
            </ToolkitProvider>
        </div>
    );
}
