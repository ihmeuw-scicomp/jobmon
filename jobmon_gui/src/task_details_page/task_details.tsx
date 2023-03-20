import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Link, useParams } from 'react-router-dom';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import TaskInstanceTable from './task_instance_table';
import NodeLists from './node_list';
import TaskFSM from './task_fsm';

function getTaskDetails(setTaskStatus, setWorkflowId, taskId) {
    // Returns task status and workflow ID
    const url = process.env.REACT_APP_BASE_URL + "/task/get_task_details_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        const data = result.data.task_details[0]
        setTaskStatus(data.task_status)
        setWorkflowId(data.workflow_id)
        
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
    const [workflow_id, setWorkflowId] = useState([])


    //***********************hooks******************************
    useEffect(() => {
        getTIDetails(setTIDetails, taskId)();
        getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId)();
        getTaskDetails(setTaskStatus, setWorkflowId, taskId)();
    }, [taskId]);

    // Update task status and table every 60 seconds if task is not in terminal state
        useEffect(() => {
        const interval = setInterval(() => {
            if (task_status !== "D" && task_status !== "F" ) {
                getTaskDetails(setTaskStatus, setWorkflowId, taskId)();
                getTIDetails(setTIDetails, taskId)();
            }
        }, 60000);
        return () => clearInterval(interval);
    }, [taskId, task_status]);

    return (
        <div>
            <Breadcrumb>
                <Breadcrumb.Item><Link to="/">Home</Link></Breadcrumb.Item>
                <Breadcrumb.Item><Link to={{ pathname: `/workflow/${workflow_id}/tasks` }}>Workflow ID {workflow_id}</Link></Breadcrumb.Item>
                <Breadcrumb.Item active>Task ID {taskId}</Breadcrumb.Item>
            </Breadcrumb>
            <div>
                <header className="App-header">
                    <p>Task ID: {taskId}</p>
                </header>
            </div>
            <div className="div-level-2">
                <TaskFSM taskStatus={task_status} />
            </div>
            <div className="div-level-2">
                <NodeLists upstreamTasks={upstream_tasks} downstreamTasks={downtream_tasks} />
            </div>
            <div id="wftable" className="div-level-2" >
                <TaskInstanceTable taskInstanceData={ti_details} />
            </div>
        </div>

    );

}

export default TaskDetails;