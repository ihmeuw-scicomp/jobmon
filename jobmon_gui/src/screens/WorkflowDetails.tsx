import React, {useState} from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import {useParams, useNavigate, useLocation} from 'react-router-dom';
import axios from 'axios';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import Tab from '@mui/material/Tab';
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import Usage from '@jobmon_gui/components/workflow_details/Usage';
import WorkflowHeader from "@jobmon_gui/components/workflow_details/WorkflowHeader"
import Box from "@mui/material/Box";
import {CircularProgress, Tabs} from "@mui/material";
import TaskTable from "@jobmon_gui/components/workflow_details/TaskTable";
import {useQuery, useQueryClient} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import HtmlTooltip from "@jobmon_gui/components/HtmlToolTip";
import ClusteredErrors from "@jobmon_gui/components/workflow_details/ClusteredErrors";
import {useTaskTableStore} from "@jobmon_gui/stores/TaskTable.ts";
import {useClusteredErrorsTableStore} from "@jobmon_gui/stores/ClusteredErrorsTable.ts";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import Divider from "@mui/material/Divider";
import {getWorkflowTTStatusQueryFn} from "@jobmon_gui/queries/GetWorkflowTTStatus.ts";
import {getWorkflowUsageQueryFn} from "@jobmon_gui/queries/GetWorkflowUsage.ts";
import {getClusteredErrorsFn} from "@jobmon_gui/queries/GetClusteredErrors.ts";
import {getWorkflowTasksQueryFn} from "@jobmon_gui/queries/GetWorkflowTasks.ts";


const task_template_tooltip_text = (<Box>
    <Typography sx={{fontWeight: "bold"}}>Task Templates</Typography>
    <Typography variant={"body2"}>
        The list of task templates with status bar, ordered by the
        submitted time of the first task associated with the task template.
    </Typography>
</Box>)


function WorkflowDetails() {
    let params = useParams();
    let workflowId = params.workflowId;
    const queryClient = useQueryClient()


    const [activeTab, setActiveTab] = useState(0);
    const [task_template_name, setTaskTemplateName] = useState('');
    const [tt_id, setTTID] = useState('');
    const [task_template_version_id, setTaskTemplateVersionId] = useState('');

    const wfTTStatus = useQuery({
        queryKey: ["workflow_details", "tt_status", workflowId],
        queryFn: getWorkflowTTStatusQueryFn
    })

    //TaskTemplate link click function
    const clickTaskTemplate = async (name, tt_id, tt_version_id) => {
        setTTID(tt_id);
        setTaskTemplateName(name);
        setTaskTemplateVersionId(tt_version_id);
        useTaskTableStore.setState({...useTaskTableStore.getState(), filters: []})
        useClusteredErrorsTableStore.setState({...useClusteredErrorsTableStore.getState(), filters: []})

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

    if (wfTTStatus.isLoading) {
        return (<CircularProgress/>)
    }
    if (wfTTStatus.isError) {
        return (<Typography>Error loading workflow task template details. Please refresh and try again.</Typography>)
    }

    const taskTemplateInfo = wfTTStatus?.data
        ? Object.values(wfTTStatus.data).map((taskTemplate: any) => ({
            tt_version_id: taskTemplate.task_template_version_id,
            name: taskTemplate.name
        }))
        : [];

    return (
        <Box>
            <Breadcrumb>
                <Breadcrumb.Item>
                    <button className="breadcrumb-button"
                            onClick={handleHomeClick}>Home
                    </button>
                </Breadcrumb.Item>
                <Breadcrumb.Item active>Workflow ID {workflowId}</Breadcrumb.Item>
            </Breadcrumb>
            <Box sx={{justifyContent: 'start', pt: 3}}>
                <WorkflowHeader
                    wf_id={workflowId} task_template_info={taskTemplateInfo}
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
                }}><HtmlTooltip title={task_template_tooltip_text}
                                arrow={true}
                                placement={"right"}>
                    <span>Task Templates&nbsp;</span>
                </HtmlTooltip>
                </Typography>

            </Box>

            <Box id="tt_progress">
                <List>
                    {
                        Object.keys(wfTTStatus?.data).map(key => (
                            <ListItem
                                key={key}
                                className={`tt-container ${tt_id == wfTTStatus?.data[key]["id"].toString() ? "selected" : ""}`}
                                id={wfTTStatus?.data[key]["id"].toString()}
                                onClick={() => {
                                    clickTaskTemplate(wfTTStatus?.data[key]["name"], wfTTStatus?.data[key]["id"], wfTTStatus?.data[key]["task_template_version_id"])
                                }}
                                onMouseEnter={async () => {
                                    void queryClient.prefetchQuery({
                                        queryKey: ["workflow_details", "usage", wfTTStatus?.data[key]["task_template_version_id"], workflowId],
                                        queryFn: getWorkflowUsageQueryFn,
                                    })
                                    void queryClient.prefetchQuery({
                                        queryKey: ["workflow_details", "clustered_errors", workflowId, wfTTStatus?.data[key]["task_template_version_id"]],
                                        queryFn: getClusteredErrorsFn,
                                    })
                                    void queryClient.prefetchQuery({
                                        queryKey: ["workflow_details", "tasks", workflowId, wfTTStatus?.data[key]["name"]],
                                        queryFn: getWorkflowTasksQueryFn,
                                    })
                                }}
                            >
                                <Box className="div_floatleft">
                                    <Typography className="tt-name">{wfTTStatus?.data[key]["name"]}</Typography>
                                </Box>
                                <Box className="div_floatright">
                                    <JobmonProgressBar
                                        workflowId={workflowId}
                                        ttId={key}
                                        placement="left"
                                    />
                                </Box>
                                <br/>
                                <Divider className="hr-dot"/>
                            </ListItem>
                        ))
                    }
                </List>

            </Box>
            <Box sx={{borderBottom: 1, borderColor: 'divider'}}>
                <Tabs
                    value={activeTab}
                    onChange={(event, newValue) => setActiveTab(newValue)}
                    aria-label="Tab selection"
                >
                    <Tab label="Errors and Tasks" value={0}/>
                    <Tab label="Resource Usage" value={1}/>
                </Tabs>
            </Box>
            <TabPanel value={activeTab} index={0}>
                <Box>
                    <Typography variant={"h5"} sx={{pt: 3}}>Clustered Errors</Typography>
                    <ClusteredErrors taskTemplateId={tt_id} workflowId={workflowId}/>
                </Box>
                <Box>
                    <Typography variant={"h5"} sx={{pt: 3}}>Tasks</Typography>
                    <TaskTable taskTemplateName={task_template_name} workflowId={workflowId}/>
                </Box>
            </TabPanel>
            <TabPanel value={activeTab} index={1}>
                <Usage taskTemplateName={task_template_name}
                       taskTemplateVersionId={task_template_version_id}
                       workflowId={workflowId}
                />
            </TabPanel>
        </Box>
    );
}

export default WorkflowDetails;
