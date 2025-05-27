import { useState } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Tab from '@mui/material/Tab';
import Tabs from '@mui/material/Tabs';
import Typography from '@mui/material/Typography';

import {
    AppBreadcrumbs,
    BreadcrumbItem,
} from '@jobmon_gui/components/common/AppBreadcrumbs';
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import TabPanel from '@jobmon_gui/components/common/TabPanel';
import ClusteredErrors from '@jobmon_gui/components/task_template_details/ClusteredErrors';
import TaskTable from '@jobmon_gui/components/task_template_details/TaskTable';
import TaskTemplateHeader from '@jobmon_gui/components/task_template_details/TaskTemplateHeader';
import Usage from '@jobmon_gui/components/task_template_details/usage/Usage';
import { getTaskTemplateDetails } from '@jobmon_gui/queries/GetTaskTemplateDetails.ts';
import { getWorkflowDetailsQueryFn } from '@jobmon_gui/queries/GetWorkflowDetails.ts';
import { getWorkflowTTStatusQueryFn } from '@jobmon_gui/queries/GetWorkflowTTStatus.ts';

export default function TaskTemplateDetails() {
    const { workflowId, taskTemplateId } = useParams();
    const queryClient = useQueryClient();

    const [activeTab, setActiveTab] = useState(0);

    const TaskTemplateDetails = getTaskTemplateDetails(
        workflowId,
        taskTemplateId
    );

    const navigate = useNavigate();
    const location = useLocation();

    const handleHomeClick = () => {
        const searchParams = new URLSearchParams(location.search);
        const search = searchParams.toString();
        navigate({
            pathname: '/',
            search: search ? `?${search}` : '',
        });
    };

    const handleWorkflowMouseEnter = async () => {
        queryClient.prefetchQuery({
            queryKey: ['workflow_details', 'details', workflowId],
            queryFn: getWorkflowDetailsQueryFn,
        });
        queryClient.prefetchQuery({
            queryKey: ['workflow_details', 'tt_status', workflowId],
            queryFn: getWorkflowTTStatusQueryFn,
        });
    };

    const breadcrumbItems: BreadcrumbItem[] = [
        { label: 'Home', to: '/', onClick: handleHomeClick },
        {
            label: `Workflow ID ${workflowId}`,
            to: `/workflow/${workflowId}`,
            onMouseEnter: handleWorkflowMouseEnter,
        },
        { label: `Task Template ID ${taskTemplateId}`, active: true },
    ];

    if (TaskTemplateDetails.isLoading) {
        return <CircularProgress />;
    }
    if (TaskTemplateDetails.isError || !TaskTemplateDetails.data) {
        return <Typography>Error loading template.</Typography>;
    }

    return (
        <Box>
            <AppBreadcrumbs items={breadcrumbItems} />

            <Box sx={{ justifyContent: 'start', pt: 3 }}>
                <TaskTemplateHeader
                    taskTemplateId={TaskTemplateDetails.data.task_template_id}
                    taskTemplateName={
                        TaskTemplateDetails.data.task_template_name
                    }
                />
            </Box>

            <Box id="tt_progress" className="div-level-2">
                <JobmonProgressBar
                    workflowId={workflowId}
                    ttId={TaskTemplateDetails.data.task_template_id}
                    placement="bottom"
                />
            </Box>

            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                <Tabs
                    value={activeTab}
                    onChange={(event, newValue) => setActiveTab(newValue)}
                    aria-label="Tab selection"
                >
                    <Tab label="Tasks" value={0} />
                    <Tab label="Clustered Errors" value={1} />
                    <Tab label="Resource Usage" value={2} />
                </Tabs>
            </Box>
            <TabPanel value={activeTab} index={0}>
                <TaskTable
                    taskTemplateName={
                        TaskTemplateDetails.data.task_template_name
                    }
                    workflowId={workflowId}
                />
            </TabPanel>
            <TabPanel value={activeTab} index={1}>
                <ClusteredErrors
                    taskTemplateId={TaskTemplateDetails.data.task_template_id}
                    workflowId={workflowId}
                />
            </TabPanel>
            <TabPanel value={activeTab} index={2}>
                <Usage
                    taskTemplateName={
                        TaskTemplateDetails.data.task_template_name
                    }
                    taskTemplateVersionId={
                        TaskTemplateDetails.data.task_template_version_id
                    }
                    workflowId={workflowId}
                />
            </TabPanel>
        </Box>
    );
}
