import React, {useState} from 'react';
import {Link, useParams, useNavigate, useLocation} from 'react-router-dom';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import TaskInstanceTable from '@jobmon_gui/components/task_details/TaskInstanceTable';
import NodeLists from '@jobmon_gui/components/task_details/NodeLists';
import TaskFSM from '@jobmon_gui/components/task_details/TaskFSM';
import {HiInformationCircle} from "react-icons/hi";
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import {CircularProgress, Grid} from "@mui/material";
import {useQuery} from "@tanstack/react-query";
import {getTaskInstanceDetailsQueryFn} from "@jobmon_gui/queries/GetTaskInstanceDetails.ts";
import {getTaskDetailsQueryFn} from "@jobmon_gui/queries/GetTaskDetails.ts";
import Typography from "@mui/material/Typography";
import {ScrollableCodeBlock} from "@jobmon_gui/components/ScrollableTextArea.tsx";
import {formatJobmonDate} from "@jobmon_gui/utils/DayTime.ts";


export default function TaskDetails() {
    const navigate = useNavigate()
    let params = useParams();
    const taskId = params.taskId

    const task_details = useQuery({
        queryKey: ["task_details", taskId],
        queryFn: getTaskDetailsQueryFn
    })

    const [showTaskInfo, setShowTaskInfo] = useState(false)
    const [showTaskFSM, setShowTaskFSM] = useState(false)
    const location = useLocation();

    const handleHomeClick = () => {
        const searchParams = new URLSearchParams(location.search);
        const search = searchParams.toString();
        navigate({
            pathname: '/',
            search: search ? `?${search}` : ''
        });
    };


    if (task_details.isError) {
        return <Typography>Unable to load task details. Please refresh and try again.</Typography>
    }
    if (task_details.isLoading || !task_details.data) {
        return <CircularProgress/>
    }
    return (
        <div>
            <Breadcrumb>
                <Breadcrumb.Item>
                    <button className="breadcrumb-button" onClick={handleHomeClick}>Home</button>
                </Breadcrumb.Item>
                <Breadcrumb.Item><Link to={{pathname: `/workflow/${task_details?.data?.workflow_id}/tasks`}}>Workflow
                    ID {task_details?.data?.workflow_id}</Link></Breadcrumb.Item>
                <Breadcrumb.Item active>Task ID {taskId}</Breadcrumb.Item>
            </Breadcrumb>
            <div>
                <header className="div-level-2 header-1 ">
                    <p className='color-dark'>
                        Task Name: {task_details?.data?.task_name}&nbsp;
                        <span>
                            <HiInformationCircle onClick={() => setShowTaskInfo(true)}/>
                        </span>
                    </p>
                    <p className="color-dark">
                        Task Finite State Machine&nbsp;
                        <span>
                            <HiInformationCircle onClick={() => setShowTaskFSM(true)}/>
                        </span>
                    </p>
                </header>
            </div>
            <div className='row pt-2 mx-0 px-0'>
                <div className="col-3">
                    <NodeLists taskId={taskId}/>
                </div>
                <div className="col-9">
                    <TaskFSM taskStatusCode={task_details?.data?.task_status}/>
                </div>
            </div>
            <div id="wftable" className="div-level-2">
                <TaskInstanceTable taskId={taskId}/>
            </div>
            <JobmonModal
                title={
                    "Task Information"
                }
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={3}><b>Task ID:</b></Grid>
                        <Grid item xs={9}>{taskId}</Grid>
                        <Grid item xs={3}><b>Task Command:</b></Grid>
                        <Grid item
                              xs={9}><ScrollableCodeBlock>{task_details?.data?.task_command}</ScrollableCodeBlock></Grid>
                        <Grid item xs={3}><b>Task Status Date:</b></Grid>
                        <Grid item
                              xs={9}>{formatJobmonDate(task_details?.data?.task_status_date)}</Grid>
                    </Grid>
                }
                open={showTaskInfo}
                onClose={() => setShowTaskInfo(false)}
                width="80%"
            />
            <JobmonModal
                title={
                    "Task Finite State Machine"
                }
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={3}><b>Registering:</b></Grid>
                        <Grid item xs={9}> Task is bound to the database.</Grid>
                        <Grid item xs={3}><b>Queued:</b></Grid>
                        <Grid item xs={9}> Task's dependencies have successfully completed, task can be run when the
                            scheduler is ready.</Grid>
                        <Grid item xs={3}><b>Instantiating:</b></Grid>
                        <Grid item xs={9}> A task instance is preparing to be launched/submitted.</Grid>
                        <Grid item xs={3}><b>Launched:</b></Grid>
                        <Grid item xs={9}> Task instance submitted to the cluster normally.</Grid>
                        <Grid item xs={3}><b>Running:</b></Grid>
                        <Grid item xs={9}> Task is running on the specified distributor.</Grid>
                        <Grid item xs={3}><b>Error Recoverable:</b></Grid>
                        <Grid item xs={9}> Task has errored out but has more attempts so it will be retried.</Grid>
                        <Grid item xs={3}><b>Adjusting Resources:</b></Grid>
                        <Grid item xs={9}> Task errored with a resource error, the resources will be adjusted before
                            retrying.</Grid>
                        <Grid item xs={3}><b>Error Fatal:</b></Grid>
                        <Grid item xs={9}> Task errored out and has used all of the attempts, therefore has failed for
                            this WorkflowRun. <br/>It can be resumed in a new WFR.</Grid>
                        <Grid item xs={3}><b>Done:</b></Grid>
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