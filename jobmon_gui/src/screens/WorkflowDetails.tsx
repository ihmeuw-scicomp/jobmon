import React, {useEffect, useState} from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import {useParams, Link, Outlet, useNavigate, useLocation} from 'react-router-dom';
import {useForm} from "react-hook-form";
import axios from 'axios';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import {OverlayTrigger} from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import {FaLightbulb} from "react-icons/fa";
import humanizeDuration from 'humanize-duration';


// @ts-ignore
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar.tsx';
import Tasks from '@jobmon_gui/components/workflow_details/Tasks';
import Usage from '@jobmon_gui/components/workflow_details/Usage';
import Errors from '@jobmon_gui/components/workflow_details/Errors';
import WorkflowHeader from "@jobmon_gui/components/workflow_details/WorkflowHeader"
import {convertDatePST} from '@jobmon_gui/utils/formatters';
import {init_apm, safe_rum_add_label, safe_rum_transaction} from '@jobmon_gui/utils/rum';

function getAsyncWFdetail(setWFDict, wf_id: string) {
    const url = import.meta.env.VITE_APP_BASE_URL + "/workflow_status_viz";
    const wf_ids = [wf_id];
    const fetchData = async () => {
        const result: any = await axios({
                method: 'get',
                url: url,
                data: null,
                params: {workflow_ids: wf_ids},
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            }
        )
        setWFDict(result.data[wf_id]);
    };
    return fetchData
}

function getWorkflowAttributes(
    wf_id: string,
    setWFTool,
    setWFName,
    setWFArgs,
    setWFSubmitted,
    setWFStatusDate,
    setWFStatus,
    setWFStatusDesc,
    setWFElapsedTime,
    setJobmonVersion
) {
    const url = import.meta.env.VITE_APP_BASE_URL + "/workflow_details_viz/" + wf_id;
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
        const data = result.data[0]
        setWFTool(data["tool_name"]);
        setWFName(data["wf_name"]);
        setWFArgs(data["wf_args"]);
        setWFStatus(data["wf_status"]);
        setWFStatusDesc(data["wf_status"] + " -- " + data["wf_status_desc"])
        setWFSubmitted(convertDatePST(data["wf_created_date"]));
        setWFStatusDate(convertDatePST(data["wf_status_date"]));
        setWFElapsedTime(humanizeDuration(new Date().getTime() - new Date(data["wf_status_date"]).getTime()))
        setJobmonVersion(data["wfr_jobmon_version"]);
    };
    return fetchData
}

function getAsyncTTdetail(setTTDict, wf_id: string, setTTLoaded) {
    const url = import.meta.env.VITE_APP_BASE_URL + "/workflow_tt_status_viz/" + wf_id;
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
    const url = import.meta.env.VITE_APP_BASE_URL + "/tt_error_log_viz/" + wf_id + "/" + tt_id;
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
        setErrorLogs(result.data);
        setErrorLoading(false);
    };
    return fetchData
}

function WorkflowDetails({subpage}) {
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
    const [task_loading, setTaskLoading] = useState(false);
    const [wf_status, setWFStatus] = useState([]);
    const [wf_status_desc, setWFStatusDesc] = useState([]);
    const [wf_tool, setWFTool] = useState([]);
    const [wf_name, setWFName] = useState([]);
    const [wf_args, setWFArgs] = useState([]);
    const [wf_submitted_date, setWFSubmitted] = useState([]);
    const [wf_status_date, setWFStatusDate] = useState([]);
    const [wf_elapsed_time, setWFElapsedTime] = useState([])
    const [jobmon_version, setJobmonVersion] = useState([])
    // only show the loading circle the first time
    const [tt_loaded, setTTLoaded] = useState(false);

    //***********************hooks******************************
    useEffect(() => {
        if (typeof params.workflowId !== 'undefined') {
            getWorkflowAttributes(params.workflowId, setWFTool, setWFName, setWFArgs, setWFSubmitted, setWFStatusDate, setWFStatus, setWFStatusDesc, setWFElapsedTime, setJobmonVersion)();
        }
    }, [params.workflowId]);

    useEffect(() => {
        if (typeof params.workflowId !== 'undefined') {
            getAsyncWFdetail(setWFDict, params.workflowId)();
            getAsyncTTdetail(setTTDict, params.workflowId, setTTLoaded)();
            safe_rum_add_label(rum_t, "wf_id", params.workflowId);
        }
    }, [params.workflowId, rum_t]);

    // Update the progress bar every 60 seconds
    useEffect(() => {
        const interval = setInterval(() => {
            if (wfDict['PENDING'] + wfDict['SCHEDULED'] + wfDict['RUNNING'] !== 0) {
                if (typeof params.workflowId !== 'undefined') {
                    // only query server when wf is unfinished
                    getAsyncWFdetail(setWFDict, params.workflowId)();
                    getAsyncTTdetail(setTTDict, params.workflowId, setTTLoaded)();
                    getWorkflowAttributes(params.workflowId, setWFTool, setWFName, setWFArgs, setWFSubmitted, setWFStatusDate, setWFStatus, setWFStatusDesc, setWFElapsedTime, setJobmonVersion)();
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
        let task_table_url = import.meta.env.VITE_APP_BASE_URL + "/task_table_viz/" + workflowId;
        const fetchData = async () => {
            const result: any = await axios({
                    method: 'get',
                    url: task_table_url,
                    data: null,
                    params: {tt_name: task_template_name},
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                }
            )
            let tasks = result.data.tasks;
            setTasks(tasks);
            setTaskLoading(false);
        };
        fetchData();
    }, [task_template_name, workflowId]);


    useEffect(() => {
        if (!task_template_version_id) {
            return
        }
        let usage_url = import.meta.env.VITE_APP_BASE_URL + "/task_template_resource_usage";

        const fetchData = async () => {
            const result: any = await axios({
                method: 'post',
                url: usage_url,
                data: {
                    task_template_version_id: task_template_version_id,
                    workflows: [workflowId],
                    viz: true
                },
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
            })
            let usage = result.data;
            setUsageInfo(usage);
        };
        fetchData();
    }, [task_template_version_id, workflowId]);

    //*******************event handling****************************
    // TaskTemplate name form
    const {register, handleSubmit} = useForm();
    const onSubmit = handleSubmit((d) => {
        setTaskTemplateName(d["task_template_name"]);
    });

    //TaskTemplate link click function
    function clickTaskTemplate(name, tt_id, tt_version_id) {
        setTaskTemplateName(name);
        setTTID(tt_id);
        setTaskTemplateVersionId(tt_version_id);
    }

    const navigate = useNavigate();
    const location = useLocation();

    const handleHomeClick = () => {
        const searchParams = new URLSearchParams(location.search);
        const search = searchParams.toString();
        navigate({
            pathname: '/',
            search: search ? `?${search}` : ''
        });
    };
    //********************html page*************************************
    return (
        <div>
            <Breadcrumb>
                <Breadcrumb.Item>
                    <button className="breadcrumb-button"
                            onClick={handleHomeClick}>Home
                    </button>
                </Breadcrumb.Item>
                <Breadcrumb.Item active>Workflow ID {workflowId} </Breadcrumb.Item>
            </Breadcrumb>
            <div className='d-flex justify-content-start pt-3'>
                <WorkflowHeader
                    wf_id={workflowId}
                    wf_status={wf_status}
                    wf_status_desc={wf_status_desc}
                    wf_tool={wf_tool}
                    wf_name={wf_name}
                    wf_args={wf_args}
                    wf_submitted_date={wf_submitted_date}
                    wf_status_date={wf_status_date}
                    wf_elapsed_time={wf_elapsed_time}
                    jobmon_version={jobmon_version}
                />
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
                                    The list of task templates with status bar, ordered by the submitted time of the
                                    first task associated with the task template.
                                </Popover>
                            )}
                        >
                            <span><FaLightbulb/></span>
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
                                            style={{}}
                                        />
                                    </div>
                                    <br/>
                                    <hr className="hr-dot"/>
                                </li>
                            ))
                        }
                    </ul>
                }
                {!tt_loaded &&
                    <div className="loader" />
                }

            </div>
            <div id="tt_search" className="div-level-2">
                <hr className="hr-2"/>
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
                    <Outlet/>
                </div>

                {(subpage === "tasks") && <Tasks tasks={tasks} onSubmit={onSubmit} register={register} loading={task_loading} apm={apm} />}
                {(subpage === "usage") && <Usage taskTemplateName={task_template_name} taskTemplateVersionId={task_template_version_id} usageInfo={usage_info} apm={apm} />}
                {(subpage === "errors") && <Errors taskTemplateName={task_template_name} taskTemplateId={tt_id} workflowId={params.workflowId} apm={apm} />}

            </div>

        </div>

    );

}

export default WorkflowDetails;