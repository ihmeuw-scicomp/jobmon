import "@jobmon_gui/styles/jobmon_gui.css";
import {BiRun} from "react-icons/bi";
import {IoMdCloseCircle, IoMdCloseCircleOutline} from "react-icons/io";
import {AiFillSchedule, AiFillCheckCircle} from "react-icons/ai";
import {TbHandStop} from "react-icons/tb";
import {HiRocketLaunch} from "react-icons/hi2";
import React, {useContext, useState} from "react";
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import {CircularProgress, Grid} from "@mui/material";
import {Box} from "@mui/system";
import {useMutation, useQuery} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import humanizeDuration from 'humanize-duration';
import {formatJobmonDate} from "@jobmon_gui/utils/DayTime.ts";

type WorkflowHeaderProps = {
    wf_id: number | string
}
import IconButton from "@mui/material/IconButton";
import {HtmlTooltip} from "@jobmon_gui/components/HtmlToolTip";
import InfoIcon from '@mui/icons-material/Info';
import AuthContext from "@jobmon_gui/contexts/AuthContext.tsx";
import StopWorkflowButton from "@jobmon_gui/components/StopWorkflow.tsx";
import {getWorkflowDetailsQueryFn} from "@jobmon_gui/queries/GetWorkflowDetails.ts";

export default function WorkflowHeader({
                                           wf_id,
                                       }: WorkflowHeaderProps) {
    const {user} = useContext(AuthContext)
    const user_name = user?.preferred_username ? user?.preferred_username.split("@")[0] : "unknown"
    const wfDetails = useQuery({
        queryKey: ["workflow_details", "details", wf_id],
        queryFn: getWorkflowDetailsQueryFn,
        staleTime: 60000, // 60 seconds
    })

    const stopWorkflow = useMutation({})

    const [showWFInfo, setShowWFInfo] = useState(false)

    const statusIcons = {
        A: {icon: <IoMdCloseCircleOutline/>, className: 'icon-aa'},
        D: {icon: <AiFillCheckCircle/>, className: 'icon-dd'},
        F: {icon: <IoMdCloseCircle/>, className: 'icon-ff'},
        G: {icon: <AiFillSchedule/>, className: 'icon-pp'},
        H: {icon: <TbHandStop/>, className: 'icon-aa'},
        I: {icon: <AiFillSchedule/>, className: 'icon-pp'},
        O: {icon: <HiRocketLaunch/>, className: 'icon-ss'},
        Q: {icon: <AiFillSchedule/>, className: 'icon-pp'},
        R: {icon: <BiRun/>, className: 'icon-rr'},
    };


    const gridHeaderStyles = {fontWeight: "bold"}

    if (wfDetails.isLoading) {
        return <CircularProgress/>
    }
    if (wfDetails.isError) {
        return <Typography>Error loading workflow details. Please refresh and try again.</Typography>
    }
    const wf_status = wfDetails?.data?.wf_status
    const wf_status_desc = wfDetails?.data?.wf_status_desc
    const wf_tool = wfDetails?.data?.tool_name
    const wf_name = wfDetails?.data?.wf_name
    const wf_args = wfDetails?.data?.wf_args
    const wf_submitted_date = formatJobmonDate(wfDetails?.data?.wf_created_date)
    const wfr_heartbeat_date = formatJobmonDate(wfDetails?.data?.wfr_heartbeat_date)
    const wf_elapsed_time = humanizeDuration(new Date(wfDetails?.data?.wfr_heartbeat_date).getTime() - new Date(wfDetails?.data?.wf_created_date).getTime())
    const jobmon_version = wfDetails?.data?.wfr_jobmon_version
    const wfr_user = wfDetails?.data?.wfr_user

    const {icon, className} = statusIcons[wf_status] || {};

    return (
        <Box className="App-header">
            <Box sx={{display: 'flex', alignItems: 'center'}}>
                <span>
                    {icon && <span className={className}>{icon}</span>}
                    {wf_id} - {wf_name}
                </span>
                <span style={{transform: 'translateY(-5px)', paddingLeft: '10px'}}>
                    <HtmlTooltip
                        title="Workflow Information"
                        arrow={true}
                        placement={"bottom"}
                        sx={{pl: 1}}
                    >
                        <IconButton
                            color="inherit"
                            onClick={() => setShowWFInfo(true)}
                        >
                            <InfoIcon fontSize={"large"}/>
                        </IconButton>
                    </HtmlTooltip>
                    <StopWorkflowButton wf_id={wf_id}/>
                </span>
            </Box>
            <Box>
                <JobmonModal
                    title={"Workflow Information"}
                    children={
                        <Grid container spacing={2}>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Status:</Grid>
                            <Grid item xs={9}>{wf_status}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Status Description:</Grid>
                            <Grid item xs={9}>{wf_status_desc}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Tool:</Grid>
                            <Grid item xs={9}>{wf_tool}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Name:</Grid>
                            <Grid item xs={9}>{wf_name}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Args:</Grid>
                            <Grid item xs={9}>{wf_args}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Submitted Date:</Grid>
                            <Grid item xs={9}>{wf_submitted_date}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>WorkflowRun Heartbeat Date:</Grid>
                            <Grid item xs={9}>{wfr_heartbeat_date}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow User:</Grid>
                            <Grid item xs={9}>{wfr_user}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Elapsed Time:</Grid>
                            <Grid item xs={9}>{wf_elapsed_time}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Jobmon Version:</Grid>
                            <Grid item xs={9}>{jobmon_version}</Grid>
                        </Grid>
                    }
                    open={showWFInfo}
                    onClose={() => setShowWFInfo(false)}
                    width="80%"/>
            </Box>
        </Box>

    )

}