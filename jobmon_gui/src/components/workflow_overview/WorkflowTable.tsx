import React, {useEffect, useState} from 'react';
import axios from 'axios';
import BootstrapTable, {ExpandRowProps} from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import {OverlayTrigger} from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
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
import {Box, List, ListItem, ListItemIcon, ListItemText, Typography} from '@mui/material';
import {HiInformationCircle} from "react-icons/hi";
import CustomModal from "@jobmon_gui/components/Modal";
import {IoMdCloseCircle, IoMdCloseCircleOutline} from "react-icons/io";
import {AiFillSchedule, AiFillCheckCircle} from "react-icons/ai";
import {BiRun} from "react-icons/bi";
import {TbHandStop} from "react-icons/tb";
import {HiRocketLaunch} from "react-icons/hi2";


const customCaret = (order, column) => {
    if (!order) return (<span><FaCaretUp style={{marginLeft: "5px"}}/></span>);
    else if (order === 'asc') return (<span><FaCaretUp style={{marginLeft: "5px"}}/></span>);
    else if (order === 'desc') return (<span><FaCaretDown style={{marginLeft: "5px"}}/></span>);
    return null;
}

// Get task status count for specified workflows
function getAsyncFetchData(setStatusDict, setFinishedWF, statusD, pre_finished_ids, wf_ids: number[], setFetchCompleted) {
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
        setFetchCompleted(true);
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

type WorkflowType = {
    DONE: number,
    FATAL: number,
    PENDING: number,
    RUNNING: number,
    SCHEDULED: number,
    wf_args: string,
    wf_id: number,
    wf_name: string,
    wf_status: string,
    wf_status_date: string,
    wf_submitted_date: string,
    wf_tool: string,
    wfr_count: number
}

type WorkflowsQueryResponse = {
    workflows: WorkflowType[]
}

export default function WorkflowTable() {
    const [expandedRows, setExpandedRows] = useState([]);
    const [statusDict, setStatusDict] = useState({});
    //TODO: get rid of finishedWF
    //without this extra parameter, the progress bar keeps spinning until next row extension
    //the console.log shows the progress bar gets its info though
    //suspect sync issue, but not sure.
    const [finishedWF, setFinishedWF] = useState<number[]>([]);
    const [helper, setHelper] = useState("");
    const [showWorkflowInfo, setShowWorkflowInfo] = useState(false)
    const [fetchCompleted, setFetchCompleted] = useState(false);

    const [workflowDetails, setWorkflowDetails] = useState<WorkflowType>({
        DONE: 0,
        FATAL: 0,
        PENDING: 0,
        RUNNING: 0,
        SCHEDULED: 0,
        wf_args: '',
        wf_id: 0,
        wf_name: '',
        wf_status: '',
        wf_status_date: '',
        wf_submitted_date: '',
        wf_tool: '',
        wfr_count: 0
    });
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
            return axios.get<WorkflowsQueryResponse>(workflow_status_url, {
                ...jobmonAxiosConfig,
                params: params
            }).then((response) => {
                const workflowIds = response.data?.workflows.map((workflow) => workflow.wf_id);
                getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, workflowIds, setFetchCompleted)();
                console.log("STATUS DICT", statusDict)
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
            // sortValue: (cell, row) => convertDate(cell).getTime(),
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
            // sortValue: (cell, row) => convertDate(cell).getTime(),
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

            getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, wf_ids, setFetchCompleted)();
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
            getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, wf_ids, setFetchCompleted)();
        }, 60000);
        return () => clearInterval(interval);
    }, [expandedRows, finishedWF, statusDict]);


    if (workflows.isLoading) {
        return (<CircularProgress/>)
    }
    if (workflows.isError) {
        return (<Typography>Error loading workflows. Please refresh and try again</Typography>)
    }

    if (!workflows.data) {
        return (<></>)
    }

    if (workflows.data.length < 1) {
        return (
            <Typography>
                No workflows found for your current search.
                Please update your search parameters and try again
            </Typography>
        )
    }

    const handleInfoClick = (workflowDetails) => {
        setWorkflowDetails(workflowDetails)
        setShowWorkflowInfo(true)
    }

    const statuses = [
        {className: 'bar-pp', label: 'Pending'},
        {className: 'bar-ss', label: 'Scheduled'},
        {className: 'bar-rr', label: 'Running'},
        {className: 'bar-ff', label: 'Fatal'},
        {className: 'bar-aa', label: 'Aborted'},
        {className: 'bar-dd', label: 'Done'},
    ];

    const statusMap = {
        'ABORTED': {icon: <IoMdCloseCircleOutline/>, className: 'icon-aa'},
        'DONE': {icon: <AiFillCheckCircle/>, className: 'icon-dd'},
        'FAILED': {icon: <IoMdCloseCircle/>, className: 'icon-ff'},
        'REGISTERING': {icon: <AiFillSchedule/>, className: 'icon-pp'},
        'HALTED': {icon: <TbHandStop/>, className: 'icon-aa'},
        'INSTANTIATING': {icon: <AiFillSchedule/>, className: 'icon-pp'},
        'LAUNCHED': {icon: <HiRocketLaunch/>, className: 'icon-ss'},
        'QUEUED': {icon: <AiFillSchedule/>, className: 'icon-pp'},
        'RUNNING': {icon: <BiRun/>, className: 'icon-rr'},
    };
    return (
        <div>
            <div>
                <div id="legend" className="legend">
                    <form className='d-flex justify-content-around w-100 mx-auto py-3'>
                        {statuses.map((status, index) => (
                            <div key={index}>
                                <label className="label-middle">
                                    <FaCircle className={status.className}/>
                                </label>
                                <label className="label-left">{status.label}</label>
                            </div>
                        ))}
                    </form>
                </div>
                <Typography variant="h4" component="h1">Workflow List</Typography>
                <List>
                    {workflows.data.map((workflow) => (
                        <ListItem key={workflow.wf_id}>
                            <div style={{display: 'flex', flexDirection: 'column', width: '100%'}}>
                                <Box sx={{display: 'flex', alignItems: 'center'}}>
                    <span className={statusMap[workflow.wf_status].className} style={{marginRight: '8px'}}>
                        {statusMap[workflow.wf_status].icon}
                    </span>
                                    <ListItemText
                                        primary={
                                            <Typography variant="h6">
                                                <Link to={`/workflow/${workflow.wf_id}/tasks${location.search}`}>
                                                    ID: {workflow.wf_id} - Name: {workflow.wf_name}
                                                </Link>
                                            </Typography>
                                        }
                                    />
                                    <ListItemIcon sx={{marginLeft: '12px', fontSize: '32px'}}>
                                        <HiInformationCircle onClick={() => handleInfoClick(workflow)}/>
                                    </ListItemIcon>
                                </Box>
                                {fetchCompleted && (
                                    <JobmonProgressBar
                                        tasks={statusDict[workflow.wf_id]["tasks"]}
                                        pending={statusDict[workflow.wf_id]["PENDING"]}
                                        scheduled={statusDict[workflow.wf_id]["SCHEDULED"]}
                                        running={statusDict[workflow.wf_id]["RUNNING"]}
                                        done={statusDict[workflow.wf_id]["DONE"]}
                                        fatal={statusDict[workflow.wf_id]["FATAL"]}
                                        maxc={statusDict[workflow.wf_id]["MAXC"]}
                                        placement="top"
                                    />
                                )}
                            </div>
                        </ListItem>
                    ))}
                </List>

            </div>
            <CustomModal
                className="workflow_info_modal"
                headerContent={
                    <h5>Workflow Information</h5>
                }
                bodyContent={
                    <p>
                        <b>Workflow Name:</b> {workflowDetails.wf_name}<br/>
                        <b>Workflow ID:</b> {workflowDetails.wf_id}<br/>
                        <b>Workflow Status:</b> {workflowDetails.wf_status}<br/>
                        <b>Tool:</b> {workflowDetails.wf_tool}<br/>
                        <b>Workflow Args:</b> {workflowDetails.wf_args} <br/>
                        <b>Date Submitted:</b> {convertDatePST(workflowDetails.wf_submitted_date)}<br/>
                        <b>Status Date: </b> {convertDatePST(workflowDetails.wf_status_date)}<br/>
                        <b>Number of WorkflowRuns: </b> {workflowDetails.wfr_count}<br/>
                    </p>
                }
                showModal={showWorkflowInfo}
                setShowModal={setShowWorkflowInfo}
            />
        </div>
    );
}
