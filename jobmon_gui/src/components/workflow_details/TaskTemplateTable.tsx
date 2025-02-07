import {useState} from 'react';
import {useQuery, useQueryClient} from "@tanstack/react-query";
import JobmonProgressBar from '@jobmon_gui/components/JobmonProgressBar';
import {CircularProgress} from "@mui/material";
import Box from "@mui/material/Box";
import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import Typography from "@mui/material/Typography";

import {useClusteredErrorsTableStore} from "@jobmon_gui/stores/ClusteredErrorsTable.ts";
import {useTaskTableStore} from "@jobmon_gui/stores/TaskTable.ts";
import {getClusteredErrorsFn} from "@jobmon_gui/queries/GetClusteredErrors.ts";
import {getWorkflowTasksQueryFn} from "@jobmon_gui/queries/GetWorkflowTasks.ts";
import {getWorkflowTTStatusQueryFn} from "@jobmon_gui/queries/GetWorkflowTTStatus.ts";
import {getWorkflowUsageQueryFn} from "@jobmon_gui/queries/GetWorkflowUsage.ts";


type TaskTemplateTableProps = {
    workflowId: number | string,
}

export default function TaskTemplateTable({workflowId}: TaskTemplateTableProps) {
    const queryClient = useQueryClient();

    const [task_template_name, setTaskTemplateName] = useState('');
    const [tt_id, setTTID] = useState('');
    const [task_template_version_id, setTaskTemplateVersionId] = useState('');

    const wfTTStatus = useQuery({
        queryKey: ["workflow_details", "tt_status", workflowId],
        queryFn: getWorkflowTTStatusQueryFn
    });

    //TaskTemplate link click function
    const clickTaskTemplate = async (name, tt_id, tt_version_id) => {
        setTTID(tt_id);
        setTaskTemplateName(name);
        setTaskTemplateVersionId(tt_version_id);
        useTaskTableStore.setState({...useTaskTableStore.getState(), filters: []})
        useClusteredErrorsTableStore.setState({...useClusteredErrorsTableStore.getState(), filters: []})
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
    );
}
