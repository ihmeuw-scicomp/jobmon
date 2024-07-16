import React, {useEffect, useState} from 'react';
import axios from 'axios';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import {Link, useLocation} from "react-router-dom";
import {convertDatePST} from '@jobmon_gui/utils/formatters';
import {FaCircle} from "react-icons/fa";
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import {useQuery} from "@tanstack/react-query";
import dayjs from "dayjs";
import {workflow_overview_url, workflow_status_url} from "@jobmon_gui/configs/ApiUrls";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
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


// Get task status count for specified workflows
function getAsyncFetchData(setStatusDict, setFinishedWF, statusD, pre_finished_ids, wf_ids: number[], setFetchCompleted) {
    const fetchData = async () => {
        let unfinished_wf_ids: number[] = [];
        let finished_wf_ids: number[] = [];
        // have to convert the unknown type to any to operate
        let all_status: any = statusD;

        for (const w of wf_ids) {
            if (statusD[w] === undefined || statusD[w]["PENDING"] + statusD[w]["SCHEDULED"] + statusD[w]["RUNNING"] > 0) {
                unfinished_wf_ids.push(w);
            } else {
                finished_wf_ids.push(w);
            }
        }

        //don't query empty list
        if (unfinished_wf_ids.length > 0) {
            const result = await axios.get(workflow_overview_url, {
                    ...jobmonAxiosConfig,
                    data: null,
                    params: {workflow_ids: unfinished_wf_ids},
                }
            )
            // have to convert the unknown type to any to operate
            let temp_data: any = result.data;

            for (const wf_id of unfinished_wf_ids) {
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

export default function WorkflowList() {
    const [expandedRows, setExpandedRows] = useState([]);
    const [statusDict, setStatusDict] = useState({});
    //TODO: get rid of finishedWF
    //without this extra parameter, the progress bar keeps spinning until next row extension
    //the console.log shows the progress bar gets its info though
    //suspect sync issue, but not sure.
    const [finishedWF, setFinishedWF] = useState<number[]>([]);
    const [showWorkflowInfo, setShowWorkflowInfo] = useState(false)
    const [fetchCompleted, setFetchCompleted] = useState(false);
    const [workflowIds, setWorkflowIds] = useState([])

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
            return axios.get<WorkflowsQueryResponse>(workflow_overview_url, {
                ...jobmonAxiosConfig,
                params: params
            }).then((response) => {
                const workflowIds = response.data?.workflows.map((workflow) => workflow.wf_id);
                setWorkflowIds(workflowIds)
                getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, workflowIds, setFetchCompleted)();
                return response.data?.workflows
            })
        },
        enabled: workflowSettings.getRefreshData()
    })

    // Update the progress bar every 60 seconds
    useEffect(() => {
        const interval = setInterval(() => {
            getAsyncFetchData(setStatusDict, setFinishedWF, statusDict, finishedWF, workflowIds, setFetchCompleted)();
        }, 60000);
        return () => clearInterval(interval);
    }, [finishedWF, statusDict, workflowIds]);


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
                                    <span className={statusMap[workflow.wf_status].className}
                                          style={{marginRight: '8px'}}>
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
                                <JobmonProgressBar
                                    workflowId={workflow.wf_id}
                                    // placement="top"
                                />

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
