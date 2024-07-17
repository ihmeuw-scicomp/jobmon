import React, {useState} from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import {useParams, useNavigate, useLocation} from 'react-router-dom';
import axios from 'axios';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import {FaLightbulb} from "react-icons/fa";
import humanizeDuration from 'humanize-duration';
import Tab from '@mui/material/Tab';

import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import Usage from '@jobmon_gui/components/workflow_details/Usage';
import WorkflowHeader from "@jobmon_gui/components/workflow_details/WorkflowHeader"
import Box from "@mui/material/Box";
import {CircularProgress, Tabs, Tooltip} from "@mui/material";
import TaskTable from "@jobmon_gui/components/workflow_details/TaskTable";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import {useQuery} from "@tanstack/react-query";
import {workflow_details_url, workflow_tt_status_url} from "@jobmon_gui/configs/ApiUrls";
import Typography from "@mui/material/Typography";
import {TTStatusResponse} from "@jobmon_gui/types/TaskTemplateStatus";
import HtmlTooltip from "@jobmon_gui/components/HtmlToolTip";
import ClusteredErrors from "@jobmon_gui/components/workflow_details/ClusteredErrors";

type WorkflowDetailsProps = {
    subpage: number
}

type WorkflowDetails = {
    tool_name: string
    wf_args: string
    wf_created_date: string
    wf_name: string
    wf_status: string
    wf_status_date: string
    wf_status_desc: string
    wfr_jobmon_version: string
}
type WorkflowDetailsResponse = WorkflowDetails[]

const task_template_tooltip_text = (<Box>
    <Typography sx={{fontWeight: "bold"}}>Task Templates</Typography>
    <Typography variant={"body2"}>
        The list of task templates with status bar, ordered by the
        submitted time of the first task associated with the task template.
    </Typography>
</Box>)


function WorkflowDetails({subpage}: WorkflowDetailsProps) {
    let params = useParams();
    let workflowId = params.workflowId;
    const [task_template_name, setTaskTemplateName] = useState('');
    const [tt_id, setTTID] = useState('');
    const [task_template_version_id, setTaskTemplateVersionId] = useState('');


    const wfDetails = useQuery({
        queryKey: ["workflow_details", "details", params.workflowId],
        queryFn: async () => {

            return axios.get<WorkflowDetailsResponse>(workflow_details_url + params.workflowId, {
                    ...jobmonAxiosConfig,
                    data: null,
                }
            ).then((r) => {
                return r.data[0]
            })
        },
        staleTime: 60000, // 60 seconds
    })

    const wfTTStatus = useQuery({
        queryKey: ["workflow_details", "tt_status", workflowId],
        queryFn: async () => {

            return axios.get<TTStatusResponse>(workflow_tt_status_url + params.workflowId, {
                    ...jobmonAxiosConfig,
                    data: null,
                }
            ).then((r) => {
                return r.data
            })
        },
    })


    //*******************event handling****************************

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

    interface TabPanelProps {
        children?: React.ReactNode;
        index: number;
        value: number;
    }

    function TabPanel(props: TabPanelProps) {
        const {children, value, index, ...other} = props;

        return (
            <div
                role="tabpanel"
                hidden={value !== index}
                id={`tabpanel-${index}`}
                aria-labelledby={`tabpanel-${index}`}
                {...other}
            >
                {children}
            </div>
        );
    }

    console.log(`subpage: ${subpage}`)

    if (wfTTStatus.isLoading) {
        return (<CircularProgress/>)
    }
    if (wfTTStatus.isError) {
        return (<Typography>Error loading workflow task template details. Please refresh and try again.</Typography>)
    }


    return (
        <Box>
            <Breadcrumb>
                <Breadcrumb.Item>
                    <button className="breadcrumb-button"
                            onClick={handleHomeClick}>Home
                    </button>
                </Breadcrumb.Item>
                <Breadcrumb.Item active>Workflow ID {workflowId} </Breadcrumb.Item>
            </Breadcrumb>
            <Box sx={{justifyContent: 'start', pt: 3}}>
                <WorkflowHeader
                    wf_id={workflowId}
                    wf_status={wfDetails?.data?.wf_status}
                    wf_status_desc={wfDetails?.data?.wf_status_desc}
                    wf_tool={wfDetails?.data?.tool_name}
                    wf_name={wfDetails?.data?.wf_name}
                    wf_args={wfDetails?.data?.wf_args}
                    wf_submitted_date={wfDetails?.data?.wf_created_date}
                    wf_status_date={wfDetails?.data?.wf_status_date}
                    wf_elapsed_time={humanizeDuration(new Date().getTime() - new Date(wfDetails?.data?.wf_status_date).getTime())}
                    jobmon_version={wfDetails?.data?.wfr_jobmon_version}
                />
            </Box>

            <Box id="wf_progress" className="div-level-2">
                <JobmonProgressBar workflowId={workflowId}
                                   placement="bottom"/>
            </Box>

            <Box id="tt_title" className="div-level-2">
                <Typography sx={{
                    textAlign: "left",
                    fontSize: "calc(16px + 1vmin)",
                    color: "var(--color-title)",
                    width: "90%"
                }}>Task Templates&nbsp;
                    <HtmlTooltip title={task_template_tooltip_text}
                                 arrow={true}
                                 placement={"right"}>
                        <span>
                            <FaLightbulb/>
                        </span>
                    </HtmlTooltip>
                </Typography>

            </Box>

            <Box id="tt_progress" className="div-scroll">
                <ul>
                    {
                        Object.keys(wfTTStatus?.data).map(key => (
                            <li
                                className={`tt-container ${tt_id == wfTTStatus?.data[key]["id"].toString() ? "selected" : ""}`}
                                id={wfTTStatus?.data[key]["id"].toString()}
                                onClick={() => clickTaskTemplate(wfTTStatus?.data[key]["name"], wfTTStatus?.data[key]["id"], wfTTStatus?.data[key]["task_template_version_id"])}
                            >
                                <div className="div_floatleft">
                                    <span className="tt-name">{wfTTStatus?.data[key]["name"]}</span>
                                </div>
                                <div className="div_floatright">
                                    <JobmonProgressBar
                                        workflowId={workflowId}
                                        ttId={key}
                                        placement="left"
                                    />
                                </div>
                                <br/>
                                <hr className="hr-dot"/>
                            </li>
                        ))
                    }
                </ul>

            </Box>
            <Box sx={{borderBottom: 1, borderColor: 'divider'}}>
                <Tabs onChange={(event, value) => {
                    if (value == 0) {
                        navigate(`/workflow/${workflowId}/tasks`)
                    }
                    if (value == 1) {
                        navigate(`/workflow/${workflowId}/usage`)
                    }

                }} aria-label="Tab selection" value={subpage}>
                    <Tab label="Errors and Tasks" value={0}/>
                    <Tab label="Resource Usage" value={1}/>
                </Tabs>
            </Box>
            <TabPanel value={subpage} index={0}>
                <Box>
                    <Typography variant={"h5"} sx={{pt:3}}>Clustered Errors</Typography>
                    <ClusteredErrors taskTemplateId={tt_id} workflowId={workflowId}/>
                </Box>
                <Box>
                    <Typography variant={"h5"} sx={{pt:3}}>Tasks</Typography>
                    <TaskTable taskTemplateName={task_template_name} workflowId={workflowId}/>
                </Box>
            </TabPanel>
            <TabPanel value={subpage} index={1}>
                <Usage taskTemplateName={task_template_name}
                       taskTemplateVersionId={task_template_version_id}
                       workflowId={workflowId}
                />
            </TabPanel>
        </Box>

    );

}

export default WorkflowDetails;