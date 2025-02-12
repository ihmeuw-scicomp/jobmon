import React, {useState} from 'react';
import {useParams, useNavigate, useLocation} from 'react-router-dom';
import TaskInstanceTable from '@jobmon_gui/components/task_details/TaskInstanceTable';
import TaskDAG from '@jobmon_gui/components/task_details/TaskDAG.tsx';
import {HiInformationCircle} from "react-icons/hi";
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import {CircularProgress, Grid} from "@mui/material";
import {useQuery, useQueryClient} from "@tanstack/react-query";
import {getTaskDetailsQueryFn} from "@jobmon_gui/queries/GetTaskDetails.ts";
import Typography from "@mui/material/Typography";
import {HtmlTooltip} from "@jobmon_gui/components/HtmlToolTip";
import IconButton from "@mui/material/IconButton";
import {getWorkflowDetailsQueryFn} from "@jobmon_gui/queries/GetWorkflowDetails.ts";
import {getWorkflowTTStatusQueryFn} from "@jobmon_gui/queries/GetWorkflowTTStatus.ts";
import {AppBreadcrumbs, BreadcrumbItem} from '@jobmon_gui/components/common/AppBreadcrumbs';


export default function TaskDetails() {
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    let params = useParams();
    const taskId = params.taskId;

    const task_details = useQuery({
        queryKey: ["task_details", taskId],
        queryFn: getTaskDetailsQueryFn
    });

    const [showTaskFSM, setShowTaskFSM] = useState(false);
    const location = useLocation();

    const handleHomeClick = () => {
        const searchParams = new URLSearchParams(location.search);
        const search = searchParams.toString();
        navigate({
            pathname: '/',
            search: search ? `?${search}` : ''
        });
    };

    const handleWorkflowMouseEnter = async () => {
        queryClient.prefetchQuery({
            queryKey: ["workflow_details", "details", task_details?.data?.workflow_id],
            queryFn: getWorkflowDetailsQueryFn,
        },)
        queryClient.prefetchQuery({
            queryKey: ["workflow_details", "tt_status", task_details?.data?.workflow_id],
            queryFn: getWorkflowTTStatusQueryFn
        },)
    };

    const breadcrumbItems: BreadcrumbItem[] = [
        {label: 'Home', onClick: handleHomeClick},
        {
            label: `Workflow ID ${task_details?.data?.workflow_id}`,
            to: `/workflow/${task_details?.data?.workflow_id}`,
            onMouseEnter: handleWorkflowMouseEnter,
        },
        {label: `Task ID ${taskId}`, active: true},
    ];


    if (task_details.isError) {
        return <Typography>Unable to load task details. Please refresh and try again.</Typography>
    }
    if (task_details.isLoading || !task_details.data) {
        return <CircularProgress/>
    }
    return (
        <div>
            <AppBreadcrumbs items={breadcrumbItems}/>
            <div>
                <header className="div-level-2 header-1 ">
                    <p className='color-dark'>
                        Task Name: {task_details?.data?.task_name}&nbsp;
                    </p>
                    <p className="color-dark">
                        Task Dependencies&nbsp;
                        <span>
                        <HtmlTooltip
                            title="Task Finite State Machine"
                            arrow={true}
                            placement={"right"}
                        >
                                <IconButton
                                    color="inherit"
                                    sx={{
                                        padding: 0,
                                        fontSize: 'inherit',
                                    }}
                                >
                                    <HiInformationCircle
                                        style={{cursor: 'pointer'}}
                                        onClick={() => setShowTaskFSM(true)}
                                    />
                                </IconButton>
                            </HtmlTooltip>
                        </span>
                    </p>
                </header>
            </div>
            <div className='row pt-2 mx-0 px-0'>
                <TaskDAG taskId={taskId} taskName={task_details?.data?.task_name}
                         taskStatus={task_details?.data?.task_status}/>
            </div>
            <div id="wftable" className="div-level-2">
                <TaskInstanceTable taskId={taskId}/>
            </div>
            <JobmonModal
                title={
                    "Task Finite State Machine"
                }
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={3}><b>Registered (G):</b></Grid>
                        <Grid item xs={9}> Task is bound to the database.</Grid>
                        <Grid item xs={3}><b>Queued for Instantiation (Q):</b></Grid>
                        <Grid item xs={9}> Task's dependencies have successfully completed, task can be run when the
                            scheduler is ready.</Grid>
                        <Grid item xs={3}><b>Instantiated (I):</b></Grid>
                        <Grid item xs={9}> A task instance is preparing to be launched/submitted.</Grid>
                        <Grid item xs={3}><b>Launched (L):</b></Grid>
                        <Grid item xs={9}> Task instance submitted to the cluster normally.</Grid>
                        <Grid item xs={3}><b>Running (R):</b></Grid>
                        <Grid item xs={9}> Task is running on the specified distributor.</Grid>
                        <Grid item xs={3}><b>Error Recoverable (E):</b></Grid>
                        <Grid item xs={9}> Task has errored out but has more attempts so it will be retried.</Grid>
                        <Grid item xs={3}><b>Adjusting Resources (A):</b></Grid>
                        <Grid item xs={9}> Task errored with a resource error, the resources will be adjusted before
                            retrying.</Grid>
                        <Grid item xs={3}><b>Error Fatal (F):</b></Grid>
                        <Grid item xs={9}> Task errored out and has used all of the attempts, therefore has failed for
                            this WorkflowRun. <br/>It can be resumed in a new WFR.</Grid>
                        <Grid item xs={3}><b>Done (D):</b></Grid>
                        <Grid item xs={9}> Task ran successfully to completion; it has a TaskInstance that successfully
                            completed.</Grid>
                    </Grid>
                }
                open={showTaskFSM}
                onClose={() => setShowTaskFSM(false)}
                width="80%"
            />
        </div>
    );

}