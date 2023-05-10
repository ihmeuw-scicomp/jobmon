import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Link, useParams, useNavigate } from 'react-router-dom';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import TaskInstanceTable from './task_instance_table';
import NodeLists from './node_list';
import TaskFSM from './task_fsm';
import { FaLightbulb } from "react-icons/fa";
import { OverlayTrigger, Popover } from 'react-bootstrap';
import { convertDatePST } from '../functions'

function getTaskDetails(setTaskStatus, setWorkflowId, setTaskName, setTaskCommand, setTaskStatusDate, taskId) {
    // Returns task status and workflow ID
    const url = process.env.REACT_APP_BASE_URL + "/task/get_task_details_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        const data = result.data.task_details[0]
        setTaskStatus(data.task_status)
        setWorkflowId(data.workflow_id)
        setTaskName(data.task_name)
        setTaskCommand(data.task_command)
        setTaskStatusDate(convertDatePST(data.task_status_date))
        
    };
    return fetchData
}

function getTIDetails(setTIDetails, taskId) {
    // Data for the TaskInstance table
    const url = process.env.REACT_APP_BASE_URL + "/task/get_ti_details_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        setTIDetails(result.data.taskinstances)
    };
    return fetchData
}

function getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId) {
    // Data for upstream and downstream task lists
    const url = process.env.REACT_APP_BASE_URL + "/task_dependencies/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        let data = result.data;
        setUpstreamTasks(data["up"])
        setDownstreamTasks(data["down"])
    };
    return fetchData
}

function TaskDetails() {
    let params = useParams();
    let taskId = params.taskId
    const [ti_details, setTIDetails] = useState([])
    const [upstream_tasks, setUpstreamTasks] = useState([])
    const [downtream_tasks, setDownstreamTasks] = useState([])
    const [task_status, setTaskStatus] = useState("")
    const [workflow_id, setWorkflowId] = useState("")
    const [task_name, setTaskName] = useState("")
    const [task_command, setTaskCommand] = useState("")
    const [task_status_date, setTaskStatusDate] = useState("")


    //***********************hooks******************************
    useEffect(() => {
        getTIDetails(setTIDetails, taskId)();
        getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId)();
        getTaskDetails(setTaskStatus, setWorkflowId, setTaskName, setTaskCommand, setTaskStatusDate, taskId)();
    }, [taskId]);

    // Update task status and table every 60 seconds if task is not in terminal state
        useEffect(() => {
        const interval = setInterval(() => {
            if (task_status !== "D" && task_status !== "F" ) {
                getTaskDetails(setTaskStatus, setWorkflowId, setTaskName, setTaskCommand, setTaskStatusDate, taskId)();
                getTIDetails(setTIDetails, taskId)();
            }
        }, 60000);
        return () => clearInterval(interval);
    }, [taskId, task_status]);

    const navigate = useNavigate()
    return (
        <div>
            <Breadcrumb>
                <Breadcrumb.Item><button className="breadcrumb-button" onClick={() => navigate(-2)}>Home</button></Breadcrumb.Item>
                 <Breadcrumb.Item><Link to={{ pathname: `/workflow/${workflow_id}/tasks` }}>Workflow ID {workflow_id}</Link></Breadcrumb.Item>
                <Breadcrumb.Item active>Task ID {taskId}</Breadcrumb.Item>
            </Breadcrumb>
            <div>
                <header className="div-level-2 header-1 ">
                    <p className='color-dark'>
                        Task Name: {task_name}&nbsp;
                        <OverlayTrigger
                            placement="right"
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    <p><b>Task ID:</b> {taskId}</p>
                                    <p><b>Task Command:</b> {task_command}</p>
                                    <p><b>Task Status Date:</b> {task_status_date} </p>
                                </Popover>
                            )}
                        >
                            <span><FaLightbulb /></span>
                        </OverlayTrigger>
                    </p>
                    <p className="color-dark">
                        Task Finite State Machine&nbsp;
                        <OverlayTrigger
                            placement="right"
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    <p><b>Registering:</b> Task is bound to the database.</p>
                                    <p><b>Queued:</b> Task's dependencies have successfully completed, task can be run when the scheduler is ready.</p>
                                    <p><b>Instantiating:</b> A task instance is preparing to be launched/submitted.</p>
                                    <p><b>Launched:</b> Task instance submitted to the cluster normally.</p>
                                    <p><b>Running:</b> Task is running on the specified distributor.</p>
                                    <p><b>Error Recoverable:</b> Task has errored out but has more attempts so it will be retried.</p>
                                    <p><b>Adjusting Resources:</b> Task errored with a resource error, the resources will be adjusted before retrying.</p>
                                    <p><b>Error Fatal:</b> Task errored out and has used all of the attempts, therefore has failed for this WorkflowRun. It can be resumed in a new WFR.</p>
                                    <p><b>Done:</b> Task ran successfully to completion; it has a TaskInstance that successfully completed.</p>
                                </Popover>
                            )}
                        >
                            <span><FaLightbulb /></span>
                        </OverlayTrigger>
                    </p>
                </header>
            </div>
            <div className='row pt-2 mx-0 px-0'>
                <div className="col-3">
                    <NodeLists upstreamTasks={upstream_tasks} downstreamTasks={downtream_tasks} />
                </div>
                <div className="col-9">
                    <TaskFSM taskStatus={task_status} />
                </div>
            </div>
            <div id="wftable" className="div-level-2" >
                <TaskInstanceTable taskInstanceData={ti_details} />
            </div>
        </div>

    );

}

export default TaskDetails;