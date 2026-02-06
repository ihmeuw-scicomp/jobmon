import React, { useState, useMemo, useEffect, useRef, useCallback } from 'react';
import ReactFlow, {
    MiniMap,
    Controls,
    Background,
    Handle,
    Position,
    Node,
    Edge,
} from 'reactflow';
import dagre from 'dagre';
import axios from 'axios';
import { useNavigate } from 'react-router-dom';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import {
    get_task_template_dag,
    workflow_tt_status_url,
} from '@jobmon_gui/configs/ApiUrls.ts';
import TaskTemplatePopover from '@jobmon_gui/components/TaskTemplatePopover.tsx';
import { useQuery } from '@tanstack/react-query';
import { TTStatus, TTStatusResponse } from '@jobmon_gui/types/TaskTemplateStatus.ts';
import { CircularProgress } from '@mui/material';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface TaskTemplateDAGResponse {
    tt_dag: {
        name: string;
        downstream_task_template_id?: string;
    }[];
}

interface DagNodeData {
    label: string;
    width: number;
    height: number;
}

interface PopoverPosition {
    x: number;
    y: number;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NODE_DIMENSIONS = {
    minWidth: 120,
    minHeight: 36,
    charWidthEstimate: 8,
    horizontalPadding: 24,
    taskCountWidthPerTask: 3,
    taskCountWidthCap: 120,
    taskCountHeightPerTask: 2,
    taskCountHeightCap: 80,
} as const;

const DAGRE_LAYOUT = {
    rankdir: 'TB' as const,
    ranksep: 60,
    nodesep: 50,
    marginx: 20,
    marginy: 20,
};

const POPOVER_OFFSET = 16;
const POPOVER_Z_INDEX = 1000;

const DAG_NODE_STYLE: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '8px 12px',
    boxSizing: 'border-box',
    background: '#fff',
    border: '1px solid #b1b1b7',
    borderRadius: 3,
    wordBreak: 'break-word',
    textAlign: 'center',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getNodeDimensions(label: string, taskCount: number): { width: number; height: number } {
    const {
        minWidth,
        minHeight,
        charWidthEstimate,
        horizontalPadding,
        taskCountWidthPerTask,
        taskCountWidthCap,
        taskCountHeightPerTask,
        taskCountHeightCap,
    } = NODE_DIMENSIONS;

    const baseWidth = Math.max(
        minWidth,
        (label?.length ?? 0) * charWidthEstimate + horizontalPadding
    );
    const widthBonus = Math.min(taskCount * taskCountWidthPerTask, taskCountWidthCap);
    const heightBonus = Math.min(taskCount * taskCountHeightPerTask, taskCountHeightCap);

    return {
        width: baseWidth + widthBonus,
        height: minHeight + heightBonus,
    };
}

function buildNodesAndEdges(ttDag: TaskTemplateDAGResponse['tt_dag']): {
    nodes: Node<DagNodeData>[];
    edges: Edge[];
} {
    const nodes: Node<DagNodeData>[] = [];
    const edges: Edge[] = [];
    const seenIds = new Set<string>();

    ttDag.forEach((task, index) => {
        const sourceId = task.name;
        const targetId = task.downstream_task_template_id;

        if (!seenIds.has(sourceId)) {
            nodes.push({
                id: sourceId,
                type: 'dagNode',
                data: { label: sourceId },
                position: { x: 0, y: 0 },
            });
            seenIds.add(sourceId);
        }
        if (targetId && !seenIds.has(targetId)) {
            nodes.push({
                id: targetId,
                type: 'dagNode',
                data: { label: targetId },
                position: { x: 0, y: 0 },
            });
            seenIds.add(targetId);
        }
        if (targetId) {
            edges.push({ id: `e${index}`, source: sourceId, target: targetId });
        }
    });

    return { nodes, edges };
}

function applyDagreLayout(
    nodes: Node<DagNodeData>[],
    edges: Edge[]
): Node<DagNodeData>[] {
    const graph = new dagre.graphlib.Graph();
    graph.setGraph(DAGRE_LAYOUT);
    graph.setDefaultEdgeLabel(() => ({}));

    nodes.forEach(node => {
        const w = node.data?.width ?? NODE_DIMENSIONS.minWidth;
        const h = node.data?.height ?? NODE_DIMENSIONS.minHeight;
        graph.setNode(node.id, { width: w, height: h });
    });
    edges.forEach(edge => {
        graph.setEdge(edge.source, edge.target, { minlen: 1, weight: 1 });
    });

    dagre.layout(graph);

    return nodes.map(node => {
        const { x, y } = graph.node(node.id);
        const w = node.data?.width ?? NODE_DIMENSIONS.minWidth;
        const h = node.data?.height ?? NODE_DIMENSIONS.minHeight;
        return {
            ...node,
            position: { x: x - w / 2, y: y - h / 2 },
        };
    });
}

// ---------------------------------------------------------------------------
// Custom node component
// ---------------------------------------------------------------------------

function DagNode({ data }: { data: DagNodeData }) {
    const label = data.label ?? '';
    const width = data.width ?? NODE_DIMENSIONS.minWidth;
    const height = data.height ?? NODE_DIMENSIONS.minHeight;

    return (
        <>
            <Handle type="target" position={Position.Top} />
            <div style={{ width, minHeight: height, ...DAG_NODE_STYLE }}>
                {label}
            </div>
            <Handle type="source" position={Position.Bottom} />
        </>
    );
}

const nodeTypes = { dagNode: DagNode };

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

interface WorkflowDAGProps {
    workflowId: string | number;
}

export default function WorkflowDAG({ workflowId }: WorkflowDAGProps) {
    const navigate = useNavigate();
    const popoverRef = useRef<HTMLDivElement>(null);

    const [nodes, setNodes] = useState<Node<DagNodeData>[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);
    const [selectedNode, setSelectedNode] = useState<Node<DagNodeData> | null>(null);
    const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
    const [popoverPosition, setPopoverPosition] = useState<PopoverPosition | null>(null);

    const wfTTStatus = useQuery({
        queryKey: ['workflow_details', 'tt_status', workflowId],
        queryFn: async () => {
            const { data } = await axios.get<TTStatusResponse>(
                workflow_tt_status_url + workflowId,
                { ...jobmonAxiosConfig }
            );
            return data;
        },
    });

    const dagQuery = useQuery({
        queryKey: ['workflow_details', 'task_template_dag', workflowId],
        queryFn: async () => {
            const { data } = await axios.get<TaskTemplateDAGResponse>(
                get_task_template_dag(workflowId),
                { ...jobmonAxiosConfig }
            );
            return data;
        },
        enabled: !!workflowId,
    });

    const ttStatusByName = useMemo(() => {
        if (!wfTTStatus.data) return {};
        return Object.fromEntries(
            Object.values(wfTTStatus.data).map(tt => [tt.name, tt])
        ) as Record<string, TTStatus>;
    }, [wfTTStatus.data]);

    const laidOutNodes = useMemo(() => {
        const nodesWithDimensions = nodes.map(node => {
            const taskCount = ttStatusByName[node.id]?.tasks ?? 0;
            const { width, height } = getNodeDimensions(
                node.data?.label ?? '',
                taskCount
            );
            return { ...node, data: { ...node.data, width, height } };
        });
        return applyDagreLayout(nodesWithDimensions, edges);
    }, [nodes, edges, ttStatusByName]);

    const styledEdges = useMemo(() => {
        return edges.map(edge => {
            const isConnected =
                hoveredNodeId &&
                (edge.source === hoveredNodeId || edge.target === hoveredNodeId);
            return {
                ...edge,
                style: isConnected
                    ? { stroke: '#2196f3', strokeWidth: 2 }
                    : undefined,
                animated: isConnected,
            };
        });
    }, [edges, hoveredNodeId]);

    useEffect(() => {
        if (dagQuery.data) {
            const { nodes: newNodes, edges: newEdges } = buildNodesAndEdges(
                dagQuery.data.tt_dag
            );
            setNodes(newNodes);
            setEdges(newEdges);
        }
    }, [dagQuery.data]);

    const hoveredTTData = selectedNode
        ? ttStatusByName[selectedNode.id]
        : undefined;

    const handleNodeMouseEnter = useCallback(
        (event: React.MouseEvent, node: Node<DagNodeData>) => {
            setSelectedNode(node);
            setHoveredNodeId(node.id);
            setPopoverPosition({ x: event.clientX, y: event.clientY });
        },
        []
    );

    const handleNodeMouseLeave = useCallback(() => {
        setSelectedNode(null);
        setHoveredNodeId(null);
        setPopoverPosition(null);
    }, []);

    const handleNodeClick = useCallback(
        (_event: React.MouseEvent, node: Node<DagNodeData>) => {
            const tt = ttStatusByName[node.id];
            if (tt) {
                navigate(`/workflow/${workflowId}/task_template/${tt.id}`);
            }
        },
        [navigate, workflowId, ttStatusByName]
    );

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            const target = event.target as HTMLElement | null;
            if (popoverRef.current && target && !popoverRef.current.contains(target)) {
                setSelectedNode(null);
                setHoveredNodeId(null);
                setPopoverPosition(null);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const showPopover = selectedNode && hoveredTTData && popoverPosition;

    if (dagQuery.isLoading || nodes.length === 0) {
        return (
            <div style={{ height: '100vh', width: '100vw', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <CircularProgress />
            </div>
        );
    }

    return (
        <div style={{ height: '100vh', width: '100vw', position: 'relative' }}>
            <ReactFlow
                nodes={laidOutNodes}
                edges={styledEdges}
                nodeTypes={nodeTypes}
                onNodeMouseEnter={handleNodeMouseEnter}
                onNodeMouseLeave={handleNodeMouseLeave}
                onNodeClick={handleNodeClick}
            >
                <MiniMap />
                <Controls />
                <Background />
            </ReactFlow>

            {showPopover && (
                <TaskTemplatePopover
                    ref={popoverRef}
                    data={hoveredTTData}
                    placement="top"
                    style={{
                        position: 'fixed',
                        left: popoverPosition.x + POPOVER_OFFSET,
                        top: popoverPosition.y + POPOVER_OFFSET,
                        zIndex: POPOVER_Z_INDEX,
                    }}
                />
            )}
        </div>
    );
}
