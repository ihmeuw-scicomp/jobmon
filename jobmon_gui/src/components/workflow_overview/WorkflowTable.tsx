import React, {useEffect, useState} from 'react';
import axios from 'axios';
import BootstrapTable, {ExpandRowProps} from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import {OverlayTrigger} from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import ToolkitProvider, {CSVExport} from 'react-bootstrap-table2-toolkit/dist/react-bootstrap-table2-toolkit';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import Spinner from 'react-bootstrap/Spinner';
import {Link, useLocation} from "react-router-dom";
import {convertDate, convertDatePST} from '@jobmon_gui/utils/formatters';
import {FaCaretDown, FaCaretUp, FaCircle} from "react-icons/fa";
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import {useQuery} from "@tanstack/react-query";
import dayjs from "dayjs";
import {workflow_status_url} from "@jobmon_gui/configs/ApiUrls";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import WorkflowStatus from "@jobmon_gui/components/workflow_overview/WorkflowStatus";
import {useWorkflowSearchSettings} from "@jobmon_gui/stores/workflow_settings";
import {CircularProgress} from "@mui/material";
import Typography from "@mui/material/Typography";


const customCaret = (order, column) => {
    if (!order) return (<span><FaCaretUp style={{marginLeft: "5px"}}/></span>);
    else if (order === 'asc') return (<span><FaCaretUp style={{marginLeft: "5px"}}/></span>);
    else if (order === 'desc') return (<span><FaCaretDown style={{marginLeft: "5px"}}/></span>);
    return null;
}

// Get task status count for specified workflows
function getAsyncFetchData(setStatusDict, setFinishedWF, statusD, pre_finished_ids, wf_ids: number[]) {
    const url = import.meta.env.VITE_APP_BASE_URL + "/workflow_status_viz";
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
            const result = await axios({
                    method: 'get',
                    url: url,
                    data: null,
                    params: {workflow_ids: unfinished_wf_ids},
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                }
            )
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

type WorkflowTableProps = {
    refreshData: boolean
    setRefreshData: (newValue: boolean) => void
}
export default function WorkflowTable({refreshData, setRefreshData}: WorkflowTableProps) {
    const [expandedRows, setExpandedRows] = useState([]);
    const [statusDict, setStatusDict] = useState({});
    //TODO: get rid of finishedWF
    //without this extra parameter, the progress bar keeps spinning until next row extension
    //the console.log shows the progress bar gets its info though
    //suspect sync issue, but not sure.
    const [finishedWF, setFinishedWF] = useState<number[]>([]);
    const [helper, setHelper] = useState("");
    const location = useLocation();
    const workflowSettings = useWorkflowSearchSettings()

    const workflows = useQuery({
        queryKey: ["workflow_overview", "workflows", workflowSettings.get().user, workflowSettings.get().tool, workflowSettings.get().wf_name, workflowSettings.get().wf_args, workflowSettings.get().wf_attribute_key, workflowSettings.get().wf_attribute_value, workflowSettings.get().wf_id, dayjs(workflowSettings.get().date_submitted).format("YYYY-MM-DD"), workflowSettings.get().status],
        queryFn: async () => {
            workflowSettings.clearDataRefresh()
            const params = new URLSearchParams({
                user: workflowSettings.get().user,
                tool: workflowSettings.get().tool,
                wf_name: workflowSettings.get().wf_name,
                wf_args: workflowSettings.get().wf_args,
                wf_attribute_key: workflowSettings.get().wf_attribute_key,
                wf_attribute_value: workflowSettings.get().wf_attribute_value,
                wf_id: workflowSettings.get().wf_id,
                date_submitted: dayjs(workflowSettings.get().date_submitted).format("YYYY-MM-DD"),
                status: workflowSettings.get().status
            });
            return axios.get(workflow_status_url, {...jobmonAxiosConfig, params: params}).then((response) => {
                response.data?.workflows?.forEach((workflow) => {
                    workflow.wf_status = <WorkflowStatus status={workflow.wf_status}/>;
                })

                return response.data?.workflows
            })
        },
        enabled: workflowSettings.getRefreshData()
    })
    // Specify table columns
    const columns = [
        {
            dataField: "wf_id",
            text: "Workflow ID",
            sort: true,
            sortCaret: customCaret,
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
                    to={{pathname: `/workflow/${cell}/tasks`, search: location.search}}
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
            sortCaret: customCaret,

            headerEvents: {
                onMouseEnter: (e, column, columnIndex) => {
                    setHelper("The NAME column in TOOL table. The name of the tool used to create the workflow.");
                },
                onMouseLeave: (e, column, columnIndex) => {
                    setHelper("");
                }
            },
            style: {overflowWrap: 'break-word'}
        },
        {
            dataField: "wf_name",
            text: "Workflow Name",
            sort: true,
            sortCaret: customCaret,

            headerEvents: {
                onMouseEnter: (e, column, columnIndex) => {
                    setHelper("The NAME column in WORKFLOW table. The user assigned name of the workflow.");
                },
                onMouseLeave: (e, column, columnIndex) => {
                    setHelper("");
                }
            },
            style: {overflowWrap: 'break-word'}
        },
        {
            dataField: "wf_args",
            text: "Workflow Args",
            sort: true,
            sortCaret: customCaret,
            headerStyle: {width: "15%"},
            headerEvents: {
                onMouseEnter: (e, column, columnIndex) => {
                    setHelper("The WORKFLOW_ARGS column in WORKFLOW table. The value of workflow_args used to create the workflow.");
                },
                onMouseLeave: (e, column, columnIndex) => {
                    setHelper("");
                }
            },
            style: {overflowWrap: 'break-word'}
        },
        {
            dataField: "wf_submitted_date",
            text: "Date Submitted",
            sort: true,
            sortCaret: customCaret,

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
            sortCaret: customCaret,

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
            sortCaret: customCaret,
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
            sortCaret: customCaret,
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
                    <JobmonProgressBar tasks={tasks} pending={pending} scheduled={scheduled} running={running}
                                       done={done} fatal={fatal} maxc={maxc} placement="top"/>
                </div>
            )
        },
        // @ts-ignore
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
        expandColumnRenderer: ({expanded}) => {
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
    const {ExportCSVButton} = CSVExport;

    if (workflows.isLoading) {
        return (<CircularProgress/>)
    }
    if (workflows.isError) {
        return (<Typography>Error loading workflows. Please refresh and try again</Typography>)
    }

    if(!workflows.data){
        return (<></>)
    }

    if (workflows.data.length < 1) {
        return (<Typography>
            No workflows found for your current search.
            Please update your search parameters and try again
        </Typography>)
    }


    return (
        <div>

            <ToolkitProvider
                keyField="wf_id"
                data={workflows?.data}
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
                            <div>
                                <OverlayTrigger
                                    placement='bottom'
                                    trigger={["hover", "focus"]}
                                    overlay={(
                                        <Popover>
                                            <FaCircle className="bar-pp"/>
                                            <span className='font-weight-bold'>Pending:</span>
                                            Tasks that are queued in Jobmon. <br/>
                                            <FaCircle className="bar-ss"/>
                                            <span className='font-weight-bold'>Scheduled:</span>
                                            Tasks that are submitted to the cluster and were queued by the cluster
                                            scheduler.<br/>
                                            <FaCircle className="bar-rr"/>
                                            <span className='font-weight-bold'>Running:</span>
                                            Tasks that are running on cluster nodes. <br/>
                                            <FaCircle className="bar-ff"/>
                                            <span className='font-weight-bold'>Fatal:</span>
                                            Tasks that did not finish because of an error. <br/>
                                            <FaCircle className="bar-aa"/>
                                            <span className='font-weight-bold'>Aborted:</span>
                                            Workflow encountered an error before a WorkflowRun was created. <br/>
                                            <FaCircle className="bar-dd"/>
                                            <span className='font-weight-bold'>Done:</span>
                                            Tasks that completed successfully.
                                        </Popover>
                                    )}>
                                    <div id="legend" className="legend">
                                        <form className='d-flex justify-content-around w-100 mx-auto py-3'>
                                            <div>
                                                <label className="label-middle"><FaCircle className="bar-pp"/> </label>
                                                <label className="label-left">Pending </label>
                                            </div>
                                            <div>
                                                <label className="label-middle"><FaCircle className="bar-ss"/> </label>
                                                <label className="label-left">Scheduled </label>
                                            </div>
                                            <div>
                                                <label className="label-middle"><FaCircle className="bar-rr"/> </label>
                                                <label className="label-left">Running </label>
                                            </div>
                                            <div>
                                                <label className="label-middle"><FaCircle className="bar-ff"/> </label>
                                                <label className="label-left">Fatal </label>
                                            </div>
                                            <div>
                                                <label className="label-middle"><FaCircle className="bar-aa"/> </label>
                                                <label className="label-left">Aborted </label>
                                            </div>
                                            <div>
                                                <label className="label-middle"><FaCircle className="bar-dd"/> </label>
                                                <label className="label-left">Done </label>
                                            </div>
                                        </form>
                                    </div>
                                </OverlayTrigger>
                            </div>
                            <ExportCSVButton {...props.csvProps} className="btn btn-custom mb-2 ml-2">Export
                                CSV</ExportCSVButton>
                            <span className="span-helper"><i>{helper}</i></span>
                            <br/>
                            <BootstrapTable
                                keyField="wf_id"
                                {...props.baseProps}
                                bootstrap4
                                expandRow={expandRow}
                                headerClasses="thead-dark"
                                striped
                                pagination={workflows?.data?.length === 0 ? undefined : paginationFactory({sizePerPage: 50})}
                            />
                        </div>
                    )
                }
            </ToolkitProvider>
        </div>
    );
}
