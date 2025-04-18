import React, {useMemo, useState} from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import {useQuery} from '@tanstack/react-query';
import {
    CircularProgress,
    Grid,
    Collapse,
    Stack,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    FormControlLabel,
    Checkbox,
    IconButton,
    Button, Tooltip
} from '@mui/material';
import Typography from '@mui/material/Typography';
import ReactFlow, {
    Background,
    Controls,
    Edge,
    Node,
} from 'reactflow';
import axios from "axios";
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import 'reactflow/dist/style.css';
import {Link, useNavigate} from 'react-router-dom';
import dagre from 'dagre';
import {getTaskDependenciesQuernFn} from '@jobmon_gui/queries/GetTaskDependancies.ts';
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import {ScrollableCodeBlock} from "@jobmon_gui/components/ScrollableTextArea.tsx";
import {formatJobmonDate} from "@jobmon_gui/utils/DayTime.ts";
import {getTaskDetailsQueryFn} from "@jobmon_gui/queries/GetTaskDetails.ts";
import {update_task_status_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";


type NodeListsProps = {
    taskId: string | number;
    taskName: string;
    taskStatus: string;
};

export default function TaskDAG({taskId, taskName, taskStatus}: NodeListsProps) {
    const [showTaskInfo, setShowTaskInfo] = useState(false)
    const [selectedTaskId, setSelectedTaskId] = useState(taskId);
    const [updateTaskCollepsOpen, setUpdateTaskCollepsOpen] = useState(true);
    const [newStatus, setNewStatus] = React.useState('G');
    const [recursive, setRecursive] = React.useState(true);
    const [taskUpdateMsg, setTaskUpdateMsg] = React.useState('')

    const taskDependencies = useQuery({
        queryKey: ['task_dependencies', taskId],
        queryFn: getTaskDependenciesQuernFn,
        refetchInterval: 60_000,
    });

    const task_details = useQuery({
        queryKey: selectedTaskId ? ['task_details', selectedTaskId] : undefined,
        queryFn: getTaskDetailsQueryFn,
        enabled: !!selectedTaskId,
    });

    const statusColorMap: Record<string, string> = {
        G: '#FFFFFF', // Registering
        Q: '#0072b2', // Queued
        I: '#0072b2', // Instantiating
        O: '#0072b2', // Launched
        R: '#0072b2', // Running
        D: '#009e73', // Done
        E: '#e69f00', // Error Recoverable
        F: '#d55e00', // Error Fatal
        A: '#e69f00', // Adjusting Resources
    };

    const truncate = (str: string) => {
        const maxLength = 15
        return str.length > maxLength ? `${str.slice(0, maxLength)}...` : str;
    }

    const {nodes, edges} = useMemo(() => {
        if (!taskDependencies.data) {
            return {nodes: [], edges: []};
        }

        const dagreGraph = new dagre.graphlib.Graph();
        dagreGraph.setDefaultEdgeLabel(() => ({}));

        dagreGraph.setGraph({
            rankdir: 'TB',
            align: 'UL',
        });

        const upstreamTasks = taskDependencies.data.up.flat(1).slice(0, 15);
        const downstreamTasks = taskDependencies.data.down.flat(1).slice(0, 15);

        const upstreamNodes: Node[] = upstreamTasks.map((task: any) => ({
            id: `up-${task.id}`,
            data: {label: truncate(task.name)},
            position: {x: 0, y: 0},
            style: {backgroundColor: statusColorMap[task.status] || 'white'},
        }));

        const downstreamNodes: Node[] = downstreamTasks.map((task: any) => ({
            id: `down-${task.id}`,
            data: {label: truncate(task.name)},
            position: {x: 0, y: 0},
            style: {backgroundColor: statusColorMap[task.status] || 'white'},
        }));

        const currentNode: Node = {
            id: `task-${taskId}`,
            data: {label: truncate(taskName)},
            position: {x: 0, y: 0},
            style: {
                backgroundColor: statusColorMap[taskStatus] || 'white',
                border: '2px solid #000000',
            },
        };

        const edges: Edge[] = [
            ...upstreamTasks.map((task: any) => ({
                id: `edge-up-${task.id}`,
                source: `up-${task.id}`,
                target: `task-${taskId}`,
            })),
            ...downstreamTasks.map((task: any) => ({
                id: `edge-down-${task.id}`,
                source: `task-${taskId}`,
                target: `down-${task.id}`,
            })),
        ];

        [...upstreamNodes, currentNode, ...downstreamNodes].forEach((node) => {
            dagreGraph.setNode(node.id, {width: 100, height: 50});
        });

        edges.forEach((edge) => {
            dagreGraph.setEdge(edge.source, edge.target);
        });

        dagre.layout(dagreGraph);

        const positionedNodes = [...upstreamNodes, currentNode, ...downstreamNodes].map((node) => {
            const {x, y} = dagreGraph.node(node.id);
            return {...node, position: {x, y}};
        });

        return {nodes: positionedNodes, edges};
    }, [taskDependencies.data, taskId, taskName]);

    const handleNodeClick = (event: React.MouseEvent, node: Node) => {
        const taskId = node.id.replace(/^(up-|down-|task-)/, '');
        setSelectedTaskId(taskId);
        setShowTaskInfo(true);
        // Extend the task update collapse by default
        setUpdateTaskCollepsOpen(true)
        // Clear the task update message
        setTaskUpdateMsg('')
    };

    if (taskDependencies.isError) {
        return <Typography>Error loading upstream and downstream tasks. Please reload and try again.</Typography>;
    }

    if (taskDependencies.isLoading || !taskDependencies.data) {
        return <CircularProgress/>;
    }

    const handleTaskStatusUpdate = async () => {
        const taskIds = [selectedTaskId]
        const workflowId = task_details?.data?.workflow_id
        setTaskUpdateMsg('Updating...')
        try {
          const response = await axios.put(
            update_task_status_url,
            {
              task_ids: taskIds,
              new_status: newStatus,
              workflow_status: null,
              workflow_id: workflowId,
              recursive: recursive,
            },
            jobmonAxiosConfig
          );
          setTaskUpdateMsg('Task statuses updated successfully');
          // Refresh task details
          task_details.refetch();
          taskDependencies.refetch();
        } catch (error) {
          setTaskUpdateMsg('Error updating task statuses: ' + error.response.data.message);
        }
     }

    return (
        <div style={{width: '100%', height: '500px'}}>
            {nodes.length === 0 ? (
                <CircularProgress/>
            ) : (
                <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodeClick={handleNodeClick}
                    fitView
                >
                    <Background/>
                    <Controls/>
                </ReactFlow>
            )}
            <JobmonModal
                title={
                    "Task Information"
                }
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={3}><b>Task ID:</b></Grid>
                        <Grid item xs={9}>
                            <Link
                                to={{pathname: `/task_details/${selectedTaskId}`}}
                                key={selectedTaskId}
                                onClick={() => setShowTaskInfo(false)}
                            >
                                {selectedTaskId}
                            </Link>
                        </Grid>
                        <Grid item xs={3}><b>Task Status:</b></Grid>
                        <Grid
                              item
                              xs={9}
                            >
                              {task_details?.data?.task_status}
                              <IconButton onClick={() => setUpdateTaskCollepsOpen(prev => !prev)}>
                                  {updateTaskCollepsOpen ? <KeyboardArrowUpIcon/> : <KeyboardArrowDownIcon/>}
                              </IconButton>
                            <Collapse in={updateTaskCollepsOpen} timeout="auto" unmountOnExit>
                               <Stack
                                  direction="row"
                                  spacing={2}
                                  alignItems="left"
                                  sx={{ border: 1, p: 2 }}
                                >
                                   <b>Update Task Status: </b>
                                   <FormControl variant="outlined" size="small">
                                    <InputLabel id="new-status-label">New Status</InputLabel>
                                    <Select
                                      labelId="new-status-label"
                                      id="new-status-select"
                                      value={newStatus}
                                      label="New Status"
                                      onChange={(e) => setNewStatus(e.target.value as string)}
                                      style={{ minWidth: 80 }}
                                    >
                                      <MenuItem value="D">D</MenuItem>
                                      <MenuItem value="G">G</MenuItem>
                                    </Select>
                                  </FormControl>
                                  <Tooltip title="Recurivly modifies the task status if checked. Only updates the selected tasks if unchecked.">
                                      <FormControlLabel
                                        control={
                                          <Checkbox
                                            checked={recursive}
                                            onChange={(e) => setRecursive(e.target.checked)}
                                            color="primary"
                                          />
                                        }
                                        label="Recursive"
                                      />
                                   </Tooltip>
                                   <Button variant="contained" color="primary" onClick={handleTaskStatusUpdate}>
                                    Update
                                  </Button>
                                   {taskUpdateMsg !== '' &&
                                       <Typography color="red">
                                           <i><br/>{taskUpdateMsg}</i>
                                       </Typography>
                                   }
                               </Stack>
                            </Collapse>
                        </Grid>

                        <Grid item xs={3}><b>Task Command:</b></Grid>
                        <Grid item
                              xs={9}><ScrollableCodeBlock>{task_details?.data?.task_command}</ScrollableCodeBlock></Grid>
                        <Grid item xs={3}><b>Task Status Date:</b></Grid>
                        <Grid item
                              xs={9}>{formatJobmonDate(task_details?.data?.task_status_date)}</Grid>
                    </Grid>
                }
                open={showTaskInfo}
                onClose={() => setShowTaskInfo(false)}
                width="80%"
            />
        </div>
    );
}
