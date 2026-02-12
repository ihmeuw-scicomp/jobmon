import React, { useMemo, useState, memo } from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { useQuery } from '@tanstack/react-query';
import {
    CircularProgress,
    FormControl,
    Select,
    MenuItem,
    FormControlLabel,
    Checkbox,
    IconButton,
    Button,
    Tooltip,
    Chip,
    Divider,
} from '@mui/material';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import CloseIcon from '@mui/icons-material/Close';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ReactFlow, {
    Background,
    Controls,
    Edge,
    Handle,
    MarkerType,
    Node,
    Position,
} from 'reactflow';
import axios from 'axios';
import 'reactflow/dist/style.css';
import { Link } from 'react-router-dom';
import dagre from 'dagre';
import { getTaskDependenciesQuernFn } from '@jobmon_gui/queries/GetTaskDependancies.ts';
import { ScrollableCodeBlock } from '@jobmon_gui/components/ScrollableTextArea.tsx';
import { formatJobmonDate } from '@jobmon_gui/utils/DayTime.ts';
import { getTaskDetailsQueryFn } from '@jobmon_gui/queries/GetTaskDetails.ts';
import { update_task_status_url } from '@jobmon_gui/configs/ApiUrls.ts';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import {
    getStatusColor,
    getStatusLabel,
    getStatusTextColor,
    taskStatusMeta,
} from '@jobmon_gui/constants/taskStatus';

type NodeListsProps = {
    taskId: string | number;
    taskName: string;
    taskStatus: string;
};

// Custom node component with tooltip showing full name and status
const DAGNode = memo(
    ({
        data,
    }: {
        data: {
            label: string;
            fullName: string;
            statusLabel: string;
            statusColor: string;
            isCurrent: boolean;
        };
    }) => {
        return (
            <Tooltip
                title={
                    <span>
                        {data.fullName}
                        <br />
                        Status: {data.statusLabel}
                    </span>
                }
                arrow
                placement="top"
            >
                <div
                    style={{
                        padding: '6px 10px',
                        borderRadius: 4,
                        backgroundColor: data.statusColor,
                        border: data.isCurrent
                            ? '2px solid #000'
                            : '1px solid #ccc',
                        fontSize: 11,
                        textAlign: 'center',
                        cursor: 'pointer',
                        color:
                            data.statusColor === '#0072b2' ||
                            data.statusColor === '#009e73'
                                ? '#fff'
                                : '#000',
                    }}
                >
                    <Handle
                        type="target"
                        position={Position.Top}
                        style={{ visibility: 'hidden' }}
                    />
                    {data.label}
                    <Handle
                        type="source"
                        position={Position.Bottom}
                        style={{ visibility: 'hidden' }}
                    />
                </div>
            </Tooltip>
        );
    }
);

const nodeTypes = { dagNode: DAGNode };

const MAX_NODES = 15;
const PANEL_WIDTH_PX = 280;

export default function TaskDAG({
    taskId,
    taskName,
    taskStatus,
}: NodeListsProps) {
    const [showTaskInfo, setShowTaskInfo] = useState(false);
    const [selectedTaskId, setSelectedTaskId] = useState(taskId);
    const [newStatus, setNewStatus] = React.useState('G');
    const [recursive, setRecursive] = React.useState(true);
    const [taskUpdateMsg, setTaskUpdateMsg] = React.useState('');

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

    const truncate = (str: string) => {
        const maxLength = 15;
        return str.length > maxLength ? `${str.slice(0, maxLength)}...` : str;
    };

    // Compute truncation info
    const totalUpstream = taskDependencies.data
        ? taskDependencies.data.up.flat(1).length
        : 0;
    const totalDownstream = taskDependencies.data
        ? taskDependencies.data.down.flat(1).length
        : 0;
    const upstreamTruncated = totalUpstream > MAX_NODES;
    const downstreamTruncated = totalDownstream > MAX_NODES;

    const { nodes, edges } = useMemo(() => {
        if (!taskDependencies.data) {
            return { nodes: [], edges: [] };
        }

        const dagreGraph = new dagre.graphlib.Graph();
        dagreGraph.setDefaultEdgeLabel(() => ({}));

        dagreGraph.setGraph({
            rankdir: 'TB',
            align: 'UL',
        });

        const upstreamTasks = taskDependencies.data.up
            .flat(1)
            .slice(0, MAX_NODES);
        const downstreamTasks = taskDependencies.data.down
            .flat(1)
            .slice(0, MAX_NODES);

        const makeNode = (
            task: { id: string | number; name: string; status: string },
            prefix: string,
            isCurrent: boolean
        ): Node => ({
            id: `${prefix}-${task.id}`,
            type: 'dagNode',
            data: {
                label: truncate(task.name),
                fullName: task.name,
                statusLabel: getStatusLabel(task.status),
                statusColor: getStatusColor(task.status),
                isCurrent,
            },
            position: { x: 0, y: 0 },
        });

        const upstreamNodes: Node[] = upstreamTasks.map(
            (task: { id: string | number; name: string; status: string }) =>
                makeNode(task, 'up', false)
        );

        const downstreamNodes: Node[] = downstreamTasks.map(
            (task: { id: string | number; name: string; status: string }) =>
                makeNode(task, 'down', false)
        );

        const currentNode: Node = makeNode(
            { id: taskId, name: taskName, status: taskStatus },
            'task',
            true
        );

        const markerEnd = {
            type: MarkerType.ArrowClosed,
            width: 16,
            height: 16,
        };

        const upstreamEdges: Edge[] = upstreamTasks.map(
            (task: { id: string | number; name: string; status: string }) => ({
                id: `edge-up-${task.id}`,
                source: `up-${task.id}`,
                target: `task-${taskId}`,
                markerEnd,
            })
        );

        const downstreamEdges: Edge[] = downstreamTasks.map(
            (task: { id: string | number; name: string; status: string }) => ({
                id: `edge-down-${task.id}`,
                source: `task-${taskId}`,
                target: `down-${task.id}`,
                markerEnd,
            })
        );

        const edges: Edge[] = [...upstreamEdges, ...downstreamEdges];

        [...upstreamNodes, currentNode, ...downstreamNodes].forEach(node => {
            dagreGraph.setNode(node.id, { width: 100, height: 50 });
        });

        edges.forEach(edge => {
            dagreGraph.setEdge(edge.source, edge.target);
        });

        dagre.layout(dagreGraph);

        const positionedNodes = [
            ...upstreamNodes,
            currentNode,
            ...downstreamNodes,
        ].map(node => {
            const { x, y } = dagreGraph.node(node.id);
            return { ...node, position: { x, y } };
        });

        return { nodes: positionedNodes, edges };
    }, [taskDependencies.data, taskId, taskName, taskStatus]);

    // Collect statuses present in the current graph for the legend
    const presentStatuses = useMemo(() => {
        if (!taskDependencies.data) return [];
        const allTasks = [
            ...taskDependencies.data.up.flat(1),
            ...taskDependencies.data.down.flat(1),
            { id: taskId, name: taskName, status: taskStatus },
        ];
        const seen = new Set<string>();
        const result: { code: string; label: string; color: string }[] = [];
        for (const t of allTasks) {
            const code = t.status?.toUpperCase();
            if (code && !seen.has(code) && taskStatusMeta[code]) {
                seen.add(code);
                result.push({
                    code,
                    label: taskStatusMeta[code].label,
                    color: taskStatusMeta[code].color,
                });
            }
        }
        return result;
    }, [taskDependencies.data, taskId, taskName, taskStatus]);

    const handleNodeClick = (_event: React.MouseEvent, node: Node) => {
        const clickedId = node.id.replace(/^(up-|down-|task-)/, '');
        setSelectedTaskId(clickedId);
        setShowTaskInfo(true);
        setTaskUpdateMsg('');
    };

    if (taskDependencies.isError) {
        return (
            <Typography>
                Error loading upstream and downstream tasks. Please reload and
                try again.
            </Typography>
        );
    }

    if (taskDependencies.isLoading || !taskDependencies.data) {
        return <CircularProgress />;
    }

    const handleTaskStatusUpdate = async () => {
        const taskIds = [selectedTaskId];
        const workflowId = task_details?.data?.workflow_id;
        setTaskUpdateMsg('Updating...');
        try {
            await axios.put(
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
            task_details.refetch();
            taskDependencies.refetch();
        } catch (error: unknown) {
            let msg = 'Unknown error';
            if (
                typeof error === 'object' &&
                error !== null &&
                'response' in error
            ) {
                const resp = (error as {
                    response?: { data?: { message?: string } };
                }).response;
                msg = resp?.data?.message || msg;
            } else if (error instanceof Error) {
                msg = error.message;
            }
            setTaskUpdateMsg('Error updating task statuses: ' + msg);
        }
    };

    const detailStatus = task_details?.data?.task_status || '';
    const detailColor = getStatusColor(detailStatus);
    const detailTextColor = getStatusTextColor(detailStatus);
    const detailLabel = getStatusLabel(detailStatus);

    return (
        <div style={{ width: '100%' }}>
            <Box sx={{ display: 'flex', height: 500 }}>
                {/* DAG graph */}
                <Box sx={{ flex: 1, minWidth: 0 }}>
                    {nodes.length === 0 ? (
                        <CircularProgress />
                    ) : (
                        <ReactFlow
                            nodes={nodes}
                            edges={edges}
                            nodeTypes={nodeTypes}
                            onNodeClick={handleNodeClick}
                            fitView
                        >
                            <Background />
                            <Controls />
                        </ReactFlow>
                    )}
                </Box>

                {/* Side panel */}
                {showTaskInfo && (
                    <Box
                        sx={{
                            flex: `0 0 ${PANEL_WIDTH_PX}px`,
                            maxWidth: PANEL_WIDTH_PX,
                            minWidth: 0,
                            borderLeft: '1px solid',
                            borderColor: 'divider',
                            overflowY: 'auto',
                            p: 1.5,
                        }}
                    >
                        {/* Header: name + status + close */}
                        <Box
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 0.5,
                                mb: 1,
                            }}
                        >
                            <Typography
                                variant="subtitle2"
                                sx={{
                                    fontWeight: 600,
                                    flex: 1,
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap',
                                }}
                            >
                                {task_details?.data?.task_name ||
                                    `Task ${selectedTaskId}`}
                            </Typography>
                            <Chip
                                label={detailLabel}
                                size="small"
                                sx={{
                                    backgroundColor: detailColor,
                                    color: detailTextColor,
                                    fontWeight: 600,
                                    fontSize: 10,
                                    height: 20,
                                }}
                            />
                            <Tooltip title="Close panel">
                                <IconButton
                                    size="small"
                                    onClick={() =>
                                        setShowTaskInfo(false)
                                    }
                                    sx={{ ml: -0.5 }}
                                >
                                    <CloseIcon
                                        sx={{ fontSize: 16 }}
                                    />
                                </IconButton>
                            </Tooltip>
                        </Box>

                        {/* Task ID link */}
                        <Box
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 0.5,
                                mb: 1,
                            }}
                        >
                            <Typography
                                variant="caption"
                                color="text.secondary"
                            >
                                Task ID:
                            </Typography>
                            <Link
                                to={`/task_details/${selectedTaskId}`}
                                onClick={() =>
                                    setShowTaskInfo(false)
                                }
                                style={{
                                    display: 'inline-flex',
                                    alignItems: 'center',
                                    gap: 2,
                                    fontSize: 12,
                                }}
                            >
                                {selectedTaskId}
                                <OpenInNewIcon
                                    sx={{ fontSize: 12 }}
                                />
                            </Link>
                        </Box>

                        {/* Status date */}
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            sx={{ mb: 1 }}
                        >
                            Updated:{' '}
                            {formatJobmonDate(
                                task_details?.data
                                    ?.task_status_date
                            )}
                        </Typography>

                        {/* Command */}
                        {task_details?.data?.task_command && (
                            <>
                                <Typography
                                    variant="caption"
                                    color="text.secondary"
                                    sx={{
                                        fontWeight: 600,
                                        textTransform:
                                            'uppercase',
                                        letterSpacing: 0.5,
                                    }}
                                >
                                    Command
                                </Typography>
                                <ScrollableCodeBlock
                                    maxheight="80px"
                                    sx={{ fontSize: 11 }}
                                >
                                    {
                                        task_details.data
                                            .task_command
                                    }
                                </ScrollableCodeBlock>
                            </>
                        )}

                        <Divider sx={{ my: 1 }} />

                        {/* Set Status */}
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            sx={{
                                fontWeight: 600,
                                textTransform: 'uppercase',
                                letterSpacing: 0.5,
                                display: 'block',
                                mb: 0.5,
                            }}
                        >
                            Set Status
                        </Typography>
                        <Box
                            sx={{
                                display: 'flex',
                                flexDirection: 'column',
                                gap: 0.5,
                            }}
                        >
                            <FormControl
                                size="small"
                                fullWidth
                            >
                                <Select
                                    value={newStatus}
                                    onChange={e =>
                                        setNewStatus(
                                            e.target
                                                .value as string
                                        )
                                    }
                                    size="small"
                                >
                                    <MenuItem value="D">
                                        Done
                                    </MenuItem>
                                    <MenuItem value="G">
                                        Registered
                                    </MenuItem>
                                </Select>
                            </FormControl>
                            <Tooltip title="Recursively update downstream tasks to the same status">
                                <FormControlLabel
                                    control={
                                        <Checkbox
                                            checked={recursive}
                                            onChange={e =>
                                                setRecursive(
                                                    e.target
                                                        .checked
                                                )
                                            }
                                            size="small"
                                        />
                                    }
                                    label={
                                        <Typography variant="caption">
                                            Recursive
                                        </Typography>
                                    }
                                    sx={{ mx: 0 }}
                                />
                            </Tooltip>
                            <Button
                                variant="contained"
                                size="small"
                                onClick={
                                    handleTaskStatusUpdate
                                }
                                fullWidth
                            >
                                Update
                            </Button>
                        </Box>
                        {taskUpdateMsg && (
                            <Typography
                                variant="caption"
                                sx={{
                                    mt: 0.5,
                                    display: 'block',
                                    color: taskUpdateMsg.includes(
                                        'Error'
                                    )
                                        ? 'error.main'
                                        : 'success.main',
                                }}
                            >
                                {taskUpdateMsg}
                            </Typography>
                        )}
                    </Box>
                )}
            </Box>

            {/* Truncation indicators */}
            {(upstreamTruncated || downstreamTruncated) && (
                <Box sx={{ mt: 0.5 }}>
                    {upstreamTruncated && (
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                        >
                            Showing {MAX_NODES} of {totalUpstream} upstream
                            tasks
                        </Typography>
                    )}
                    {downstreamTruncated && (
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                        >
                            Showing {MAX_NODES} of {totalDownstream} downstream
                            tasks
                        </Typography>
                    )}
                </Box>
            )}

            {/* Color legend — only statuses present in the graph */}
            {presentStatuses.length > 0 && (
                <Box
                    sx={{
                        display: 'flex',
                        flexWrap: 'wrap',
                        gap: 2,
                        mt: 1,
                        px: 1,
                    }}
                >
                    {presentStatuses.map(s => (
                        <Box
                            key={s.code}
                            sx={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: 0.5,
                            }}
                        >
                            <Box
                                sx={{
                                    width: 10,
                                    height: 10,
                                    borderRadius: '50%',
                                    backgroundColor: s.color,
                                    flexShrink: 0,
                                }}
                            />
                            <Typography variant="caption">
                                {s.label}
                            </Typography>
                        </Box>
                    ))}
                </Box>
            )}
        </div>
    );
}
