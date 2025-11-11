import React, { useState } from 'react';
import '@jobmon_gui/styles/jobmon_gui.css';
import { useParams, useNavigate, useLocation, Link } from 'react-router-dom';
import Tab from '@mui/material/Tab';
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import WorkflowHeader from '@jobmon_gui/components/workflow_details/WorkflowHeader';
import Box from '@mui/material/Box';
import { CircularProgress, Tabs } from '@mui/material';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import Typography from '@mui/material/Typography';
import CloseIcon from '@mui/icons-material/Close';
import { useTaskTableStore } from '@jobmon_gui/stores/TaskTable.ts';
import { useClusteredErrorsTableStore } from '@jobmon_gui/stores/ClusteredErrorsTable.ts';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import { getWorkflowTTStatusQueryFn } from '@jobmon_gui/queries/GetWorkflowTTStatus.ts';
import { getWorkflowUsageQueryFn } from '@jobmon_gui/queries/GetWorkflowUsage.ts';
import { getClusteredErrorsFn } from '@jobmon_gui/queries/GetClusteredErrors.ts';
import { getWorkflowTasksQueryFn } from '@jobmon_gui/queries/GetWorkflowTasks.ts';
import {
    AppBreadcrumbs,
    BreadcrumbItem,
} from '@jobmon_gui/components/common/AppBreadcrumbs';
import TabPanel from '@jobmon_gui/components/common/TabPanel';
import WorkflowDAG from '@jobmon_gui/components/workflow_details/WorkflowDAG.tsx';
import { getWorkflowFiltersForNavigation } from '@jobmon_gui/utils/workflowFilterPersistence';

// Color constant matching the fatal status color from CSS
const FATAL_COLOR = '#d55e00';

// Styles for the fatal indicator icon
const fatalIconStyles = {
    color: FATAL_COLOR,
    fontSize: '1.2rem',
    flexShrink: 0,
};

// Styles for the task template name container
const taskTemplateNameContainerStyles = {
    display: 'flex',
    alignItems: 'center',
    gap: 1,
};

function WorkflowDetails() {
    const { workflowId } = useParams();
    const queryClient = useQueryClient();

    const [tt_active_tab, setTTActiveTab] = useState(0);

    const wfTTStatus = useQuery({
        queryKey: ['workflow_details', 'tt_status', workflowId],
        queryFn: getWorkflowTTStatusQueryFn,
        refetchOnMount: true,
        refetchOnWindowFocus: true,
    });

    const navigate = useNavigate();
    const location = useLocation();

    const handleHomeClick = () => {
        const search = getWorkflowFiltersForNavigation(location.search);
        navigate({
            pathname: '/',
            search: search || '',
        });
    };

    const breadcrumbItems: BreadcrumbItem[] = [
        { label: 'Home', to: '/', onClick: handleHomeClick },
        { label: `Workflow ID ${workflowId}`, active: true },
    ];

    if (wfTTStatus.isLoading) {
        return <CircularProgress />;
    }
    if (wfTTStatus.isError) {
        return (
            <Typography>
                Error loading workflow task template details. Please refresh and
                try again.
            </Typography>
        );
    }

    const taskTemplateInfo = wfTTStatus?.data
        ? Object.values(wfTTStatus.data).map(
              (taskTemplate: {
                  task_template_version_id: string | number;
                  name: string;
                  id: string | number;
              }) => ({
                  tt_version_id: taskTemplate.task_template_version_id,
                  name: taskTemplate.name,
              })
          )
        : [];

    // A callback that invalidates the progress bar query.
    const handlePopupClose = () => {
        queryClient.invalidateQueries({
            queryKey: ['workflow_details', 'progress_bar', workflowId],
        });
        queryClient.invalidateQueries({
            queryKey: ['workflow_details', 'tt_status', workflowId],
        });
    };

    return (
        <Box>
            <AppBreadcrumbs items={breadcrumbItems} />

            <Box sx={{ justifyContent: 'start', pt: 3 }}>
                <WorkflowHeader
                    wf_id={workflowId}
                    task_template_info={taskTemplateInfo}
                    onTechnicalPanelClose={handlePopupClose}
                />
            </Box>

            <Box id="wf_progress" className="div-level-2">
                <JobmonProgressBar workflowId={workflowId} placement="bottom" />
            </Box>

            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                <Tabs
                    value={tt_active_tab}
                    onChange={(event, newValue) => setTTActiveTab(newValue)}
                    aria-label="Tab selection"
                >
                    <Tab label="Task Templates" value={0} />
                    <Tab label="DAG Viz" value={1} />
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
                        {Object.keys(wfTTStatus?.data).map(key => {
                            const taskTemplate = wfTTStatus.data[key];
                            const ttId = taskTemplate.id.toString();

                            return (
                                <ListItem
                                    key={ttId}
                                    sx={{
                                        borderBottom: '1px solid #ccc',
                                    }}
                                >
                                    <ListItemButton
                                        component={Link}
                                        to={`/workflow/${workflowId}/task_template/${ttId}`}
                                        onMouseEnter={async () => {
                                            void queryClient.prefetchQuery({
                                                queryKey: [
                                                    'workflow_details',
                                                    'usage',
                                                    taskTemplate.task_template_version_id,
                                                    workflowId,
                                                ],
                                                queryFn: getWorkflowUsageQueryFn,
                                            });
                                            void queryClient.prefetchQuery({
                                                queryKey: [
                                                    'workflow_details',
                                                    'clustered_errors',
                                                    workflowId,
                                                    taskTemplate.task_template_version_id,
                                                ],
                                                queryFn: getClusteredErrorsFn,
                                            });
                                            void queryClient.prefetchQuery({
                                                queryKey: [
                                                    'workflow_details',
                                                    'tasks',
                                                    workflowId,
                                                    taskTemplate.name,
                                                ],
                                                queryFn: getWorkflowTasksQueryFn,
                                            });
                                        }}
                                        onClick={() => {
                                            useTaskTableStore.setState({
                                                ...useTaskTableStore.getState(),
                                                filters: [],
                                            });
                                            useClusteredErrorsTableStore.setState(
                                                {
                                                    ...useClusteredErrorsTableStore.getState(),
                                                    filters: [],
                                                }
                                            );
                                        }}
                                    >
                                        <Box 
                                            className="div_floatleft" 
                                            sx={taskTemplateNameContainerStyles}
                                        >
                                            <Typography className="tt-name">
                                                {taskTemplate.name}
                                            </Typography>
                                            {taskTemplate.FATAL > 0 && (
                                                <CloseIcon sx={fatalIconStyles} />
                                            )}
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
                <WorkflowDAG workflowId={workflowId} />
            </TabPanel>
        </Box>
    );
}

export default WorkflowDetails;
