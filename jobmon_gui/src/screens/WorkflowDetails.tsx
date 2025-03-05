import React, {useState} from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import {useParams, useNavigate, useLocation, Link} from 'react-router-dom';
import Tab from '@mui/material/Tab';
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import WorkflowHeader from "@jobmon_gui/components/workflow_details/WorkflowHeader"
import Box from "@mui/material/Box";
import {CircularProgress, Tabs} from "@mui/material";
import {useQuery, useQueryClient} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import {useTaskTableStore} from "@jobmon_gui/stores/TaskTable.ts";
import {useClusteredErrorsTableStore} from "@jobmon_gui/stores/ClusteredErrorsTable.ts";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import {getWorkflowTTStatusQueryFn} from "@jobmon_gui/queries/GetWorkflowTTStatus.ts";
import {getWorkflowUsageQueryFn} from "@jobmon_gui/queries/GetWorkflowUsage.ts";
import {getClusteredErrorsFn} from "@jobmon_gui/queries/GetClusteredErrors.ts";
import {getWorkflowTasksQueryFn} from "@jobmon_gui/queries/GetWorkflowTasks.ts";
import {AppBreadcrumbs, BreadcrumbItem} from "@jobmon_gui/components/common/AppBreadcrumbs";
import TabPanel from "@jobmon_gui/components/common/TabPanel";
import WorkflowDAG from "@jobmon_gui/components/workflow_details/WorkflowDAG.tsx";


function WorkflowDetails() {
    const {workflowId} = useParams();
    const queryClient = useQueryClient();

    const [tt_active_tab, setTTActiveTab] = useState(0);

    const wfTTStatus = useQuery({
        queryKey: ["workflow_details", "tt_status", workflowId],
        queryFn: getWorkflowTTStatusQueryFn,
        refetchOnMount: true
    })

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

    const breadcrumbItems: BreadcrumbItem[] = [
        {label: "Home", to: "/", onClick: handleHomeClick},
        {label: `Workflow ID ${workflowId}`, active: true},
    ];

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
            <AppBreadcrumbs items={breadcrumbItems}/>

            <Box sx={{justifyContent: 'start', pt: 3}}>
                <WorkflowHeader
                    wf_id={workflowId} task_template_info={taskTemplateInfo}
                />
            </Box>

            <Box id="wf_progress" className="div-level-2">
                <JobmonProgressBar
                    workflowId={workflowId}
                    placement="bottom"
                />
            </Box>

            <Box sx={{borderBottom: 1, borderColor: 'divider'}}>
                <Tabs
                    value={tt_active_tab}
                    onChange={(event, newValue) => setTTActiveTab(newValue)}
                    aria-label="Tab selection"
                >
                    <Tab label="Task Templates" value={0}/>
                    <Tab label="DAG Viz" value={1}/>
                </Tabs>
            </Box>

            <TabPanel value={tt_active_tab} index={0}>
                <Box id="tt_progress">
                    <List
                        sx={{
                            padding: 0,
                            '& .MuiListItem-root:nth-of-type(odd)': {
                                backgroundColor: '#f9f9f9',
                            },
                        }}
                    >
                        {Object.keys(wfTTStatus?.data).map((key, index) => {
                            const taskTemplate = wfTTStatus.data[key];
                            const ttId = taskTemplate.id.toString();

                            return (
                                <ListItem
                                    key={ttId}
                                    sx={{
                                        borderBottom: '1px solid #ccc'
                                    }}
                                >
                                    <ListItemButton
                                        component={Link}
                                        to={`/workflow/${workflowId}/task_template/${ttId}`}
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
                                        onClick={() => {
                                            useTaskTableStore.setState({
                                                ...useTaskTableStore.getState(),
                                                filters: [],
                                            });
                                            useClusteredErrorsTableStore.setState({
                                                ...useClusteredErrorsTableStore.getState(),
                                                filters: [],
                                            });
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
                                    </ListItemButton>
                                </ListItem>
                            );
                        })}
                    </List>
                </Box>
            </TabPanel>
            <TabPanel index={tt_active_tab} value={1}>
                <WorkflowDAG workflowId={workflowId}/>
            </TabPanel>
        </Box>
    );
}

export default WorkflowDetails;
