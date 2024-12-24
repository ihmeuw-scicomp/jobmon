import React, {useMemo} from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import {useQuery} from '@tanstack/react-query';
import {CircularProgress} from '@mui/material';
import Typography from '@mui/material/Typography';
import ReactFlow, {
    Background,
    Controls,
    Edge,
    Node,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {useNavigate} from 'react-router-dom';
import dagre from 'dagre';
import {getTaskDependenciesQuernFn} from '@jobmon_gui/queries/GetTaskDependancies.ts';

type NodeListsProps = {
    taskId: string | number;
    taskName: string;
    taskStatus: string;
};

export default function TaskDAG({taskId, taskName, taskStatus}: NodeListsProps) {
    const navigate = useNavigate();
    const taskDependencies = useQuery({
        queryKey: ['task_dependencies', taskId],
        queryFn: getTaskDependenciesQuernFn,
        refetchInterval: 60_000,
    });

    const statusColorMap: Record<string, string> = {
        G: '#0072b2', // Registering
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
        navigate(`/task_details/${taskId}`);
    };

    if (taskDependencies.isError) {
        return <Typography>Error loading upstream and downstream tasks. Please reload and try again.</Typography>;
    }

    if (taskDependencies.isLoading || !taskDependencies.data) {
        return <CircularProgress/>;
    }
    
    return (
        <div style={{width: '100%', height: '500px'}}>
            {nodes.length === 0 || edges.length === 0 ? (
                <div>Loading...</div> // You can replace this with a spinner or any custom loading component
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
        </div>
    );
}
