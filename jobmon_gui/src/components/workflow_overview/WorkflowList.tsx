import React, {useState} from 'react';
import axios from 'axios';
import {Link, useLocation} from "react-router-dom";
import {convertDatePST} from '@jobmon_gui/utils/formatters';
import {FaCircle} from "react-icons/fa";
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import {useQuery} from "@tanstack/react-query";
import dayjs from "dayjs";
import {workflow_overview_url} from "@jobmon_gui/configs/ApiUrls";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import {useWorkflowSearchSettings} from "@jobmon_gui/stores/workflow_settings";
import {CircularProgress, Grid} from "@mui/material";
import {Box, List, ListItem, ListItemIcon, ListItemText, Typography} from '@mui/material';
import InfoIcon from '@mui/icons-material/Info';
import DirectionsRunIcon from '@mui/icons-material/DirectionsRun';
import ErrorIcon from '@mui/icons-material/Error';
import PanToolIcon from '@mui/icons-material/PanTool';
import CancelIcon from '@mui/icons-material/Cancel';
import DoneIcon from '@mui/icons-material/Done';
import CalendarMonthIcon from '@mui/icons-material/CalendarMonth';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import IconButton from '@mui/material/IconButton';
import {JobmonModal} from "@jobmon_gui/components/JobmonModal";
import {ScrollableCodeBlock} from "@jobmon_gui/components/ScrollableTextArea";

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
    wf_submitted_date_end: string,
    wf_tool: string,
    wfr_count: number
}

type WorkflowsQueryResponse = {
    workflows: WorkflowType[]
}

export default function WorkflowList() {
    const [showWorkflowInfo, setShowWorkflowInfo] = useState(false)
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
        wf_submitted_date_end: '',
        wf_tool: '',
        wfr_count: 0
    });
    const location = useLocation();
    const workflowSettings = useWorkflowSearchSettings()

    const workflows = useQuery({
        queryKey: [
            "workflow_overview", "workflows", workflowSettings.get().user,
            workflowSettings.get().tool, workflowSettings.get().wf_name,
            workflowSettings.get().wf_args,
            workflowSettings.get().wf_attribute_key,
            workflowSettings.get().wf_attribute_value,
            workflowSettings.get().wf_id,
            dayjs(workflowSettings.get().date_submitted).format("YYYY-MM-DD"),
            dayjs(workflowSettings.get().date_submitted_end).format("YYYY-MM-DD"),
            workflowSettings.get().status
        ],
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
                date_submitted_end: dayjs(workflowSettings.get().date_submitted_end).add(1, 'day').format("YYYY-MM-DD"),
                status: workflowSettings.get().status
            });
            return axios.get<WorkflowsQueryResponse>(workflow_overview_url, {
                ...jobmonAxiosConfig,
                params: params
            }).then((response) => {
                return response.data?.workflows
            })
        },
        enabled: workflowSettings.getRefreshData()
    })


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
        'ABORTED': {icon: <CancelIcon/>, className: 'icon-aa'},
        'DONE': {icon: <DoneIcon/>, className: 'icon-dd'},
        'FAILED': {icon: <ErrorIcon/>, className: 'icon-ff'},
        'REGISTERING': {icon: <CalendarMonthIcon/>, className: 'icon-pp'},
        'HALTED': {icon: <PanToolIcon/>, className: 'icon-aa'},
        'INSTANTIATING': {icon: <CalendarMonthIcon/>, className: 'icon-pp'},
        'LAUNCHED': {icon: <RocketLaunchIcon/>, className: 'icon-ss'},
        'QUEUED': {icon: <CalendarMonthIcon/>, className: 'icon-pp'},
        'RUNNING': {icon: <DirectionsRunIcon/>, className: 'icon-rr'},
    };

    const modalTitleStyles = {
        fontWeight: "bold"
    }
    const modalValuesStyles = {
        fontFamily: 'Roboto Mono Variable',
    }


    return (
        <Box>
            <Box>
                <Box id="legend" className="legend">
                    <form className='d-flex justify-content-around w-100 mx-auto py-3'>
                        {statuses.map((status, index) => (
                            <Box key={index}>
                                <label className="label-middle">
                                    <FaCircle className={status.className}/>
                                </label>
                                <label className="label-left">{status.label}</label>
                            </Box>
                        ))}
                    </form>
                </Box>
                <Typography variant="h4" component="h1">Workflow List</Typography>
                <List>
                    {workflows.data.map((workflow) => (
                        <ListItem key={workflow.wf_id}>
                            <Box style={{display: 'flex', flexDirection: 'column', width: '100%'}}>
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
                                        <IconButton>
                                            <InfoIcon onClick={() => handleInfoClick(workflow)}/>
                                        </IconButton>

                                    </ListItemIcon>
                                </Box>
                                <JobmonProgressBar
                                    workflowId={workflow.wf_id}
                                />

                            </Box>
                        </ListItem>
                    ))}
                </List>

            </Box>
            <JobmonModal
                title="Workflow Details"
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Workflow Name:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography sx={modalValuesStyles}>{workflowDetails.wf_name}</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Workflow ID:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography sx={modalValuesStyles}>{workflowDetails.wf_id}</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Workflow Status:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography sx={modalValuesStyles}>{workflowDetails.wf_status}</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Tool:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography sx={modalValuesStyles}>{workflowDetails.wf_tool}</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Date Submitted:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography
                                sx={modalValuesStyles}>{convertDatePST(workflowDetails.wf_submitted_date)}</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Status Date:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography
                                sx={modalValuesStyles}>{convertDatePST(workflowDetails.wf_status_date)}</Typography>
                        </Grid>
                        <Grid item xs={4}>
                            <Typography sx={modalTitleStyles}>Number of Workflow Runs:</Typography>
                        </Grid>
                        <Grid item xs={8}>
                            <Typography sx={modalValuesStyles}>{workflowDetails.wfr_count}</Typography>
                        </Grid>

                        <Grid item xs={12}>
                            <Typography sx={modalTitleStyles}>Workflow Args:</Typography>
                        </Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>
                                {workflowDetails.wf_args}
                            </ScrollableCodeBlock>
                        </Grid>
                    </Grid>
                }
                open={showWorkflowInfo}
                onClose={() => setShowWorkflowInfo(false)}
                width={"80%"}
            />
        </Box>
    );
}
