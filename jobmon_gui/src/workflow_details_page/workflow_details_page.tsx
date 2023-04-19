import React, { useEffect, useState } from 'react';
import '../jobmon_gui.css';
import { useParams, Link, Outlet } from 'react-router-dom';
import { useForm } from "react-hook-form";
import axios from 'axios';
import DropdownButton from 'react-bootstrap/DropdownButton';
import Dropdown from 'react-bootstrap/Dropdown';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faCircle, faLightbulb } from '@fortawesome/free-solid-svg-icons';


// @ts-ignore
import JobmonProgressBar from '../progress_bar.tsx';
import Tasks from './tasks';
import Usage from './usage';
import Errors from './errors';
import WFHeader from "./wf_header"
import { init_apm, convertDatePST, safe_rum_add_label, safe_rum_transaction } from '../functions';

function getAsyncWFdetail(setWFDict, wf_id: string) {
    const url = process.env.REACT_APP_BASE_URL + "/workflow_status_viz";
    const wf_ids = [wf_id];
    const fetchData = async () => {
        const result: any = await axios.get(url, { params: { workflow_ids: wf_ids } });
        setWFDict(result.data[wf_id]);
    };
    return fetchData
}

function getWorkflowAttributes(wf_id: string, setWFTool, setWFName, setWFArgs, setWFSubmitted, setWFStatusDate, setWFStatus, setWFStatusDesc) {
    const url = process.env.REACT_APP_BASE_URL + "/workflow_details_viz/" + wf_id;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        const data = result.data[0]
        setWFTool(data["tool_name"]);
        setWFName(data["wf_name"]);
        setWFArgs(data["wf_args"]);
        setWFStatus(data["wf_status"]);
        setWFStatusDesc("Workflow Status: " + data["wf_status"] + " -- " + data["wf_status_desc"])
        setWFSubmitted(convertDatePST(data["wf_created_date"]));
        setWFStatusDate(convertDatePST(data["wf_status_date"]));
    };
    return fetchData
}

function getAsyncTTdetail(setTTDict, wf_id: string, setTTLoaded) {
    const url = process.env.REACT_APP_BASE_URL + "/workflow_tt_status_viz/" + wf_id;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        let return_array: any = [];
        for (let t in result.data) {
            return_array.push(result.data[t]);
        }
        setTTDict(return_array);
        setTTLoaded(true);
    };
    return fetchData
}

function getAsyncErrorLogs(setErrorLogs, wf_id: string, setErrorLoading, tt_id?: string) {
    setErrorLoading(true);
    const url = process.env.REACT_APP_BASE_URL + "/tt_error_log_viz/" + wf_id + "/" + tt_id;
    const fetchData = async () => {
        const result: any = await axios.get(url);
        setErrorLogs(result.data);
        setErrorLoading(false);
    };
    return fetchData
}

function WorkflowDetails({ subpage }) {
    const apm = init_apm("wf_detail_page");
    let rum_t: any = safe_rum_transaction(apm);
    let params = useParams();
    let workflowId = params.workflowId;
    const [task_template_name, setTaskTemplateName] = useState('');
    const [tt_id, setTTID] = useState('');
    const [task_template_version_id, setTaskTemplateVersionId] = useState('');
    const [usage_info, setUsageInfo] = useState([]);
    const [tasks, setTasks] = useState([]);
    const [wfDict, setWFDict] = useState({
        'tasks': 0, 'PENDING': 0, 'SCHEDULED': 0, 'RUNNING': 0, 'DONE': 0, 'FATAL': 0,
        'num_attempts_avg': 0, 'num_attempts_min': 0, 'num_attempts_max': 0, 'MAXC': 0
    });
    const [ttDict, setTTDict] = useState([]);
    const [errorLogs, setErrorLogs] = useState([]);
    const [error_loading, setErrorLoading] = useState(false);
    const [task_loading, setTaskLoading] = useState(false);
    const [wf_status, setWFStatus] = useState([]);
    const [wf_status_desc, setWFStatusDesc] = useState([]);
    const [wf_tool, setWFTool] = useState([]);
    const [wf_name, setWFName] = useState([]);
    const [wf_args, setWFArgs] = useState([]);
    const [wf_submitted_date, setWFSubmitted] = useState([]);
    const [wf_status_date, setWFStatusDate] = useState([]);
    // only show the loading circle the first time
    const [tt_loaded, setTTLoaded] = useState(false);

    //***********************hooks******************************
    useEffect(() => {
        if (typeof params.workflowId !== 'undefined') {
            getWorkflowAttributes(params.workflowId, setWFTool, setWFName, setWFArgs, setWFSubmitted, setWFStatusDate, setWFStatusDesc)();
        }
    }, [params.workflowId]);
    useEffect(() => {
        if (typeof params.workflowId !== 'undefined') {
            getAsyncWFdetail(setWFDict, params.workflowId)();
            getAsyncTTdetail(setTTDict, params.workflowId, setTTLoaded)();
            safe_rum_add_label(rum_t, "wf_id", params.workflowId);
        }
    }, [params.workflowId]);
    // Update the progress bar every 60 seconds
    useEffect(() => {
        const interval = setInterval(() => {
            if (wfDict['PENDING'] + wfDict['SCHEDULED'] + wfDict['RUNNING'] !== 0) {
                if (typeof params.workflowId !== 'undefined') {
                    //only query server when wf is unfinised
                    getAsyncWFdetail(setWFDict, params.workflowId)();
                    getAsyncTTdetail(setTTDict, params.workflowId, setTTLoaded)();
                }
            }
        }, 60000);
        return () => clearInterval(interval);
    }, [wfDict, params.workflowId]);

    // Get information to populate the Tasks table
    useEffect(() => {
        if (task_template_name === null || task_template_name === "") {
            return
        }
        setTaskLoading(true);
        let task_table_url = process.env.REACT_APP_BASE_URL + "/task_table_viz/" + workflowId;
        const fetchData = async () => {
            const result: any = await axios.get(
                task_table_url,
                { params: { tt_name: task_template_name } }
            );
            let tasks = result.data.tasks;
            setTasks(tasks);
            setTaskLoading(false);
        };
        fetchData();
    }, [task_template_name, workflowId]);
    useEffect(() => {
        if (typeof params.workflowId !== 'undefined' && tt_id !== 'undefined' && tt_id !== '') {
            getAsyncErrorLogs(setErrorLogs, params.workflowId, setErrorLoading, tt_id)();
        }
    }, [tt_id, params.workflowId]);

    // Get resource usage information
    useEffect(() => {
        if (!task_template_version_id) {
            return
        }
        let usage_url = process.env.REACT_APP_BASE_URL + "/task_template_resource_usage";

        const fetchData = async () => {
            const result: any = await axios({
                method: 'post',
                url: usage_url,
                data: {
                    task_template_version_id: task_template_version_id,
                    workflows: [workflowId],
                    viz: true
                },
            })
            let usage = result.data;
            setUsageInfo(usage);
        };
        fetchData();
    }, [task_template_version_id, workflowId]);

    //*******************event handling****************************
    // TaskTemplate name form
    const { register, handleSubmit } = useForm();
    const onSubmit = handleSubmit((d) => {
        setTaskTemplateName(d["task_template_name"]);
    });
    //TaskTemplate link click function
    function clickTaskTemplate(name, tt_id, tt_version_id) {
        setTaskTemplateName(name);
        setTTID(tt_id);
        setTaskTemplateVersionId(tt_version_id);
    }

    //********************html page*************************************
    return (
        <div>
            <Breadcrumb>
                <Breadcrumb.Item><Link to="/">Home</Link></Breadcrumb.Item>
                <Breadcrumb.Item active>Workflow ID {workflowId} </Breadcrumb.Item>
            </Breadcrumb>
            <div className='d-flex justify-content-start pt-3'>
                <WFHeader
                      wf_id={workflowId}
                      wf_status={wf_status}
                      wf_status_desc={wf_status_desc}
                 />
                <div>
                    <DropdownButton variant="dark" menuVariant="dark" title="Details" className="mt-2">
                        <Dropdown.Item variant="dark">
                            <p><b>Workflow Tool:</b> {wf_tool}</p>
                            <p><b>Workflow Name:</b> {wf_name}</p>
                            <p><b>Workflow Args:</b> {wf_args}</p>
                            <p><b>Workflow Submitted Date:</b> {wf_submitted_date}</p>
                            <p><b>Workflow Status Date:</b> {wf_status_date}</p>
                        </Dropdown.Item>
                    </DropdownButton>
                </div>
            </div>
            <div id="wf_progress" className="div-level-2">
                <JobmonProgressBar
                    tasks={wfDict.tasks}
                    pending={wfDict.PENDING}
                    scheduled={wfDict.SCHEDULED}
                    running={wfDict.RUNNING}
                    done={wfDict.DONE}
                    fatal={wfDict.FATAL}
                    num_attempts_avg={wfDict.num_attempts_avg}
                    num_attempts_min={wfDict.num_attempts_min}
                    num_attempts_max={wfDict.num_attempts_max}
                    maxc={wfDict.MAXC}
                    placement="bottom"
                />
            </div>

            <div id="tt_title" className="div-level-2">
                <header className="header-1 d-flex align-items-center">
                    <p className='mr-5'>
                        Task Templates&nbsp;
                        <OverlayTrigger
                            placement="right"
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    The list of task templates with status bar, ordered by the submitted time of the first task associated with the task template.
                                </Popover>
                            )}
                        >
                            <span><FontAwesomeIcon icon={faLightbulb} /></span>
                        </OverlayTrigger>
                    </p>
                    {tt_id === "" &&
                        <div className="div-hint">
                            <i> Please select a Task Template to see its Tasks, Resource Usage, and Errors. </i>
                        </div>
                    }

                </header>
            </div>

            <div id="tt_progress" className="div-scroll">
                {tt_loaded &&
                    <ul>
                        {
                            ttDict.map(d => (
                                <li
                                    className={`tt-container ${tt_id === d["id"] ? "selected" : ""}`}
                                    id={d["id"]}
                                    onClick={() => clickTaskTemplate(d["name"], d["id"], d["task_template_version_id"])}
                                >
                                    <div className="div_floatleft">
                                        <span className="tt-name">{d["name"]}</span>
                                    </div>
                                    <div className="div_floatright">
                                        <JobmonProgressBar
                                            id={d["id"]}
                                            tasks={d["tasks"]}
                                            pending={d["PENDING"]}
                                            scheduled={d["SCHEDULED"]}
                                            running={d["RUNNING"]}
                                            done={d["DONE"]}
                                            fatal={d["FATAL"]}
                                            num_attempts_avg={d["num_attempts_avg"]}
                                            num_attempts_min={d["num_attempts_min"]}
                                            num_attempts_max={d["num_attempts_max"]}
                                            maxc={d["MAXC"]}
                                            placement="left"
                                            style="none"
                                        />
                                    </div>
                                    <br />
                                    <hr className="hr-dot" />
                                </li>
                            ))
                        }
                    </ul>
                }
                {tt_loaded === false &&
                    <div className="loader" />
                }

            </div>
            <div id="tt_search" className="div-level-2">
                <hr className="hr-2" />
                <div className="div-full">
                    <ul className="nav nav-pills">
                        <li className="nav-item">
                            <Link
                                className={`nav-link ${subpage === "tasks" ? "active" : ""}`}
                                aria-current="page"
                                to={`/workflow/${workflowId}/tasks`}
                                replace={true}>
                                Tasks
                            </Link>
                        </li>
                        <li className="nav-item">
                            <Link
                                className={`nav-link ${subpage === "usage" ? "active" : ""}`}
                                to={`/workflow/${workflowId}/usage`}
                                replace={true}>
                                Resource Usage
                            </Link>
                        </li>
                        <li className="nav-item">
                            <Link
                                className={`nav-link ${subpage === "errors" ? "active" : ""}`}
                                to={`/workflow/${workflowId}/errors`}
                                replace={true}>
                                Errors
                            </Link>
                        </li>
                    </ul>
                    <Outlet />
                </div>

                {(subpage === "tasks") && <Tasks tasks={tasks} onSubmit={onSubmit} register={register} loading={task_loading} apm={apm} />}
                {(subpage === "usage") && <Usage taskTemplateName={task_template_name} taskTemplateVersionId={task_template_version_id} usageInfo={usage_info} apm={apm} />}
                {(subpage === "errors") && <Errors errorLogs={errorLogs} tt_name={task_template_name} loading={error_loading} apm={apm} />}

            </div>

        </div>

    );

}

export default WorkflowDetails;