import React, {useState, useEffect} from 'react';
import axios from 'axios';
import {Link, useParams, useNavigate, useLocation} from 'react-router-dom';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import TaskInstanceTable from '@jobmon_gui/components/task_details/TaskInstanceTable';
import NodeLists from '@jobmon_gui/components/task_details/NodeLists';
import TaskFSM from '@jobmon_gui/components/task_details/TaskFSM';
import {convertDatePST} from '@jobmon_gui/utils/formatters'
import {HiInformationCircle} from "react-icons/hi";
import CustomModal from '@jobmon_gui/components/Modal';

function getTaskDetails(setTaskStatus, setWorkflowId, setTaskName, setTaskCommand, setTaskStatusDate, taskId) {
    // Returns task status and workflow ID
    const url = import.meta.env.VITE_APP_BASE_URL + "/task/get_task_details_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios({
                method: 'get',
                url: url,
                data: null,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            }
        )
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
    const url = import.meta.env.VITE_APP_BASE_URL + "/task/get_ti_details_viz/" + taskId;
    const fetchData = async () => {
        const result: any = await axios({
                method: 'get',
                url: url,
                data: null,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            }
        )
        setTIDetails(result.data.taskinstances)
    };
    return fetchData
}

function getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId) {
    // Data for upstream and downstream task lists
    const url = import.meta.env.VITE_APP_BASE_URL + "/task_dependencies/" + taskId;
    const fetchData = async () => {
        const result: any = await axios({
                method: 'get',
                url: url,
                data: null,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            }
        )
        let data = result.data;
        setUpstreamTasks(data["up"])
        setDownstreamTasks(data["down"])
    };
    return fetchData
}

export default function TaskDetails() {
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
    const [showTaskInfo, setShowTaskInfo] = useState(false)
    const [showTaskFSM, setShowTaskFSM] = useState(false)


    //***********************hooks******************************
    useEffect(() => {
        getTIDetails(setTIDetails, taskId)();
        getTaskDependencies(setUpstreamTasks, setDownstreamTasks, taskId)();
        getTaskDetails(setTaskStatus, setWorkflowId, setTaskName, setTaskCommand, setTaskStatusDate, taskId)();
    }, [taskId]);

    // Update task status and table every 60 seconds if task is not in terminal state
    useEffect(() => {
        const interval = setInterval(() => {
            if (task_status !== "D" && task_status !== "F") {
                getTaskDetails(setTaskStatus, setWorkflowId, setTaskName, setTaskCommand, setTaskStatusDate, taskId)();
                getTIDetails(setTIDetails, taskId)();
            }
        }, 60000);
        return () => clearInterval(interval);
    }, [taskId, task_status]);

    const location = useLocation();

    const handleHomeClick = () => {
        const searchParams = new URLSearchParams(location.search);
        const search = searchParams.toString();
        navigate({
            pathname: '/',
            search: search ? `?${search}` : ''
        });
    };

    const navigate = useNavigate()
    return (
        <div>
            <Breadcrumb>
                <Breadcrumb.Item>
                    <button className="breadcrumb-button" onClick={handleHomeClick}>Home</button>
                </Breadcrumb.Item>
                <Breadcrumb.Item><Link to={{pathname: `/workflow/${workflow_id}/tasks`}}>Workflow
                    ID {workflow_id}</Link></Breadcrumb.Item>
                <Breadcrumb.Item active>Task ID {taskId}</Breadcrumb.Item>
            </Breadcrumb>
            <div>
                <header className="div-level-2 header-1 ">
                    <p className='color-dark'>
                        Task Name: {task_name}&nbsp;
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
                    <NodeLists upstreamTasks={upstream_tasks} downstreamTasks={downtream_tasks}/>
                </div>
                <div className="col-9">
                    <TaskFSM taskStatus={task_status}/>
                </div>
            </div>
            <div id="wftable" className="div-level-2">
                <TaskInstanceTable taskInstanceData={ti_details}/>
            </div>
            <CustomModal
                className="task_info_modal"
                headerContent={
                    <h5> Task Information</h5>
                }
                bodyContent={
                    <p>
                        <b>Task ID:</b> {taskId}<br/>
                        <b>Task Command:</b> {task_command}<br/>
                        <b>Task Status Date:</b> {task_status_date}<br/>
                    </p>
                }
                showModal={showTaskInfo}
                setShowModal={setShowTaskInfo}
            />
            <CustomModal
                className="task_fsm_modal"
                headerContent={
                    <h5> Task Finite State Machine</h5>
                }
                bodyContent={
                    <p>
                        <b>Registering:</b> Task is bound to the database.<br/>
                        <b>Queued:</b> Task's dependencies have successfully completed, task can be run when the
                        scheduler is ready.<br/>
                        <b>Instantiating:</b> A task instance is preparing to be launched/submitted.<br/>
                        <b>Launched:</b> Task instance submitted to the cluster normally.<br/>
                        <b>Running:</b> Task is running on the specified distributor.<br/>
                        <b>Error Recoverable:</b> Task has errored out but has more attempts so it will be retried.<br/>
                        <b>Adjusting Resources:</b> Task errored with a resource error, the resources will be adjusted
                        before retrying.<br/>
                        <b>Error Fatal:</b> Task errored out and has used all of the attempts, therefore has failed for
                        this WorkflowRun. <br/>
                        It can be resumed in a new WFR.<br/>
                        <b>Done:</b> Task ran successfully to completion; it has a TaskInstance that successfully
                        completed.<br/>
                    </p>
                }
                showModal={showTaskFSM}
                setShowModal={setShowTaskFSM}
            />
        </div>
    );

}