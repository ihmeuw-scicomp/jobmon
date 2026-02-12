import React, {
    useState,
    useMemo,
    useEffect,
    useCallback,
    createContext,
    useContext,
    useSyncExternalStore,
} from 'react';
import ReactFlow, {
    Controls,
    Background,
    Handle,
    Position,
    Node,
    Edge,
    NodeProps,
    useReactFlow,
    ReactFlowProvider,
} from 'reactflow';
import dagre from 'dagre';
import axios from 'axios';
import { useNavigate } from 'react-router-dom';
import { jobmonAxiosConfig } from '@jobmon_gui/configs/Axios.ts';
import { get_task_template_dag } from '@jobmon_gui/configs/ApiUrls.ts';
import TaskTemplatePopover from '@jobmon_gui/components/TaskTemplatePopover.tsx';
import { useQuery } from '@tanstack/react-query';
import { TTStatus } from '@jobmon_gui/types/TaskTemplateStatus.ts';
import { CircularProgress } from '@mui/material';
import {
    TEMPLATE_STATUS_COLORS,
    TEMPLATE_STATUS_KEYS,
} from '@jobmon_gui/constants/taskStatus';

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
    width?: number;
    height?: number;
    statusCounts?: {
        PENDING: number;
        SCHEDULED: number;
        RUNNING: number;
        DONE: number;
        FATAL: number;
        tasks: number;
    };
}

interface PopoverPosition {
    x: number;
    y: number;
}

// ---------------------------------------------------------------------------
// Visual state store — lives outside React's render cycle.
// DagNode subscribes via useSyncExternalStore so only the 1-2 affected
// nodes re-render on hover/select changes. The nodes array passed to
// ReactFlow stays completely stable.
// ---------------------------------------------------------------------------

interface DagVisualState {
    hoveredId: string | null;
    selectedId: string | null;
}

function createDagVisualStore() {
    let state: DagVisualState = { hoveredId: null, selectedId: null };
    const listeners = new Set<() => void>();

    const notify = () => listeners.forEach(l => l());

    return {
        getSnapshot: () => state,
        subscribe: (listener: () => void) => {
            listeners.add(listener);
            return () => {
                listeners.delete(listener);
            };
        },
        setHovered: (id: string | null) => {
            if (state.hoveredId !== id) {
                state = { ...state, hoveredId: id };
                notify();
            }
        },
        setSelected: (id: string | null) => {
            if (state.selectedId !== id) {
                state = { ...state, selectedId: id };
                notify();
            }
        },
    };
}

type DagVisualStore = ReturnType<typeof createDagVisualStore>;

const DagVisualStoreContext = createContext<DagVisualStore | null>(null);

function useDagNodeIsHovered(id: string): boolean {
    const store = useContext(DagVisualStoreContext)!;
    return useSyncExternalStore(
        store.subscribe,
        useCallback(
            () => store.getSnapshot().hoveredId === id,
            [store, id]
        )
    );
}

function useDagNodeIsSelected(id: string): boolean {
    const store = useContext(DagVisualStoreContext)!;
    return useSyncExternalStore(
        store.subscribe,
        useCallback(
            () => store.getSnapshot().selectedId === id,
            [store, id]
        )
    );
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NODE_DIMENSIONS = {
    minWidth: 120,
    maxWidthBonus: 60,
    minHeight: 36,
    maxHeightBonus: 24,
    charWidthEstimate: 8,
    horizontalPadding: 24,
    statusBarHeight: 8,
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

// Node & edge colors (inline styles for ReactFlow — theme not available)
const COLOR_BORDER = '#b1b1b7';
const COLOR_PRIMARY = '#1976d2';
const COLOR_HOVER_BG = '#e3f2fd';

const EDGE_STYLE_DEFAULT = { stroke: COLOR_BORDER, strokeWidth: 1 };
const EDGE_STYLE_HIGHLIGHTED = { stroke: COLOR_PRIMARY, strokeWidth: 2.5 };
const EDGE_STYLE_DIMMED = { stroke: '#e0e0e0', strokeWidth: 1 };


function getDominantColor(
    statusCounts?: DagNodeData['statusCounts']
): string {
    if (!statusCounts || statusCounts.tasks === 0) return COLOR_BORDER;

    // FATAL takes priority — any fatal tasks means immediate red
    if (statusCounts.FATAL > 0) return TEMPLATE_STATUS_COLORS.FATAL;

    // DONE only when 100% complete
    if (statusCounts.DONE === statusCounts.tasks)
        return TEMPLATE_STATUS_COLORS.DONE;

    // Among active states, pick the one with the highest count
    let maxCount = 0;
    let dominant = COLOR_BORDER;
    for (const key of ['RUNNING', 'SCHEDULED', 'PENDING'] as const) {
        if (statusCounts[key] > maxCount) {
            maxCount = statusCounts[key];
            dominant = TEMPLATE_STATUS_COLORS[key];
        }
    }
    return dominant;
}

const DAG_NODE_STYLE: React.CSSProperties = {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '8px 12px',
    boxSizing: 'border-box',
    borderRadius: 3,
    wordBreak: 'break-word',
    textAlign: 'center',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function computeRelativeScales(
    taskCounts: Record<string, number>
): Record<string, number> {
    const entries = Object.entries(taskCounts);
    if (entries.length === 0) return {};

    const counts = entries.map(([, c]) => Math.max(1, c));
    const logMin = Math.log(Math.min(...counts));
    const logMax = Math.log(Math.max(...counts));
    const logRange = logMax - logMin;

    const scales: Record<string, number> = {};
    for (const [name, count] of entries) {
        if (logRange === 0) {
            scales[name] = 0.5;
        } else {
            scales[name] =
                (Math.log(Math.max(1, count)) - logMin) / logRange;
        }
    }
    return scales;
}

function getNodeDimensions(
    label: string,
    hasStatus: boolean,
    relativeScale: number
): { width: number; height: number } {
    const {
        minWidth,
        maxWidthBonus,
        minHeight,
        maxHeightBonus,
        charWidthEstimate,
        horizontalPadding,
        statusBarHeight,
    } = NODE_DIMENSIONS;

    const labelWidth =
        (label?.length ?? 0) * charWidthEstimate + horizontalPadding;
    const scaleBonus = maxWidthBonus * relativeScale;
    const width = Math.max(minWidth, labelWidth) + scaleBonus;

    const height =
        minHeight +
        maxHeightBonus * relativeScale +
        (hasStatus ? statusBarHeight : 0);

    return { width, height };
}

function buildNodesAndEdges(
    ttDag: TaskTemplateDAGResponse['tt_dag']
): {
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
            edges.push({
                id: `e${index}`,
                source: sourceId,
                target: targetId,
            });
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
        graph.setEdge(edge.source, edge.target, {
            minlen: 1,
            weight: 1,
        });
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
// Custom node component — reads visual state from the store, NOT from
// node data. This means the nodes array never changes for hover/select,
// and only the 1-2 affected DagNode components re-render.
// ---------------------------------------------------------------------------

const DagNode = React.memo(function DagNode({
    data,
    id,
}: NodeProps<DagNodeData>) {
    const isHovered = useDagNodeIsHovered(id);
    const isSelected = useDagNodeIsSelected(id);

    const label = data.label ?? '';
    const width = data.width ?? NODE_DIMENSIONS.minWidth;
    const height = data.height ?? NODE_DIMENSIONS.minHeight;
    const dominantColor = getDominantColor(data.statusCounts);
    const total = data.statusCounts?.tasks ?? 0;

    return (
        <>
            <Handle type="target" position={Position.Top} />
            <div
                style={{
                    width,
                    minHeight: height,
                    ...DAG_NODE_STYLE,
                    borderLeft: `3px solid ${dominantColor}`,
                    borderTop: `1px solid ${COLOR_BORDER}`,
                    borderRight: `1px solid ${COLOR_BORDER}`,
                    borderBottom: `1px solid ${COLOR_BORDER}`,
                    outline: isSelected
                        ? `2px solid ${COLOR_PRIMARY}`
                        : 'none',
                    outlineOffset: 1,
                    background: isHovered ? COLOR_HOVER_BG : '#fff',
                    transition: 'background 0.15s ease',
                }}
            >
                <span>{label}</span>
                {total > 0 && (
                    <div
                        style={{
                            display: 'flex',
                            width: '100%',
                            height: 4,
                            marginTop: 4,
                            borderRadius: 2,
                            overflow: 'hidden',
                        }}
                    >
                        {TEMPLATE_STATUS_KEYS.map(key => {
                            const count =
                                data.statusCounts?.[key] ?? 0;
                            if (count === 0) return null;
                            return (
                                <div
                                    key={key}
                                    style={{
                                        width: `${(count / total) * 100}%`,
                                        backgroundColor:
                                            TEMPLATE_STATUS_COLORS[key],
                                    }}
                                />
                            );
                        })}
                    </div>
                )}
            </div>
            <Handle type="source" position={Position.Bottom} />
        </>
    );
});

const nodeTypes = { dagNode: DagNode };

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

interface WorkflowDAGProps {
    workflowId: string | number;
    ttStatusByName: Record<string, TTStatus>;
    selectedTemplateName?: string | null;
    hoveredTemplateName?: string | null;
    onTemplateSelect?: (name: string) => void;
    onTemplateHover?: (name: string | null) => void;
    height?: string;
}

const EMPTY_STATUS: Record<string, TTStatus> = {};

function WorkflowDAGInner({
    workflowId,
    ttStatusByName = EMPTY_STATUS,
    selectedTemplateName,
    hoveredTemplateName,
    onTemplateSelect,
    onTemplateHover,
    height,
}: WorkflowDAGProps) {
    const navigate = useNavigate();
    const { fitView } = useReactFlow();

    const visualStore = useMemo(() => createDagVisualStore(), []);

    const [nodes, setNodes] = useState<Node<DagNodeData>[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);
    const [popoverNodeId, setPopoverNodeId] = useState<string | null>(
        null
    );
    const [popoverPosition, setPopoverPosition] =
        useState<PopoverPosition | null>(null);

    // Sync props → store. The store's no-op guard prevents spurious
    // notifications when the value hasn't actually changed.
    useEffect(() => {
        visualStore.setSelected(selectedTemplateName ?? null);
    }, [selectedTemplateName, visualStore]);

    useEffect(() => {
        visualStore.setHovered(hoveredTemplateName ?? null);
    }, [hoveredTemplateName, visualStore]);

    const dagQuery = useQuery({
        queryKey: [
            'workflow_details',
            'task_template_dag',
            workflowId,
        ],
        queryFn: async () => {
            const { data } =
                await axios.get<TaskTemplateDAGResponse>(
                    get_task_template_dag(workflowId),
                    { ...jobmonAxiosConfig }
                );
            return data;
        },
        enabled: !!workflowId,
    });

    // Structural layout — only contains label, dimensions, statusCounts.
    // No hover/select state. This array is what ReactFlow receives and
    // it only changes when the graph structure or status values change.
    const structuralNodes = useMemo(() => {
        const taskCounts: Record<string, number> = {};
        for (const node of nodes) {
            taskCounts[node.id] =
                ttStatusByName[node.id]?.tasks ?? 0;
        }
        const scales = computeRelativeScales(taskCounts);

        const nodesWithDimensions = nodes.map(node => {
            const tt = ttStatusByName[node.id];
            const { width, height: h } = getNodeDimensions(
                node.data?.label ?? '',
                (tt?.tasks ?? 0) > 0,
                scales[node.id] ?? 0
            );
            const statusCounts = tt
                ? {
                      PENDING: tt.PENDING,
                      SCHEDULED: tt.SCHEDULED,
                      RUNNING: tt.RUNNING,
                      DONE: tt.DONE,
                      FATAL: tt.FATAL,
                      tasks: tt.tasks,
                  }
                : undefined;
            return {
                ...node,
                data: {
                    ...node.data,
                    width,
                    height: h,
                    statusCounts,
                },
            };
        });
        return applyDagreLayout(nodesWithDimensions, edges);
    }, [nodes, edges, ttStatusByName]);

    // Fit the view once when the graph structure first loads.
    const graphStructureKey = useMemo(
        () => nodes.map(n => n.id).sort().join('\0'),
        [nodes]
    );

    useEffect(() => {
        if (structuralNodes.length > 0) {
            const timer = setTimeout(() => {
                fitView({ padding: 0.15, duration: 200 });
            }, 50);
            return () => clearTimeout(timer);
        }
    }, [graphStructureKey, fitView]);

    useEffect(() => {
        if (dagQuery.data) {
            const { nodes: newNodes, edges: newEdges } =
                buildNodesAndEdges(dagQuery.data.tt_dag);
            setNodes(newNodes);
            setEdges(newEdges);
        }
    }, [dagQuery.data]);

    // Subscribe to hoveredId for edge highlighting
    const hoveredId = useSyncExternalStore(
        visualStore.subscribe,
        useCallback(() => visualStore.getSnapshot().hoveredId, [visualStore])
    );

    const styledEdges = useMemo(() => {
        if (!hoveredId) {
            return edges.map(e => ({ ...e, style: EDGE_STYLE_DEFAULT }));
        }
        return edges.map(e => ({
            ...e,
            style:
                e.source === hoveredId || e.target === hoveredId
                    ? EDGE_STYLE_HIGHLIGHTED
                    : EDGE_STYLE_DIMMED,
            zIndex:
                e.source === hoveredId || e.target === hoveredId ? 1 : 0,
        }));
    }, [edges, hoveredId]);

    const popoverTTData = popoverNodeId
        ? ttStatusByName[popoverNodeId]
        : undefined;

    const handleNodeMouseEnter = useCallback(
        (event: React.MouseEvent, node: Node<DagNodeData>) => {
            visualStore.setHovered(node.id);
            setPopoverNodeId(node.id);
            setPopoverPosition({
                x: event.clientX,
                y: event.clientY,
            });
            onTemplateHover?.(node.id);
        },
        [onTemplateHover, visualStore]
    );

    const handleNodeMouseLeave = useCallback(() => {
        visualStore.setHovered(null);
        setPopoverNodeId(null);
        setPopoverPosition(null);
        onTemplateHover?.(null);
    }, [onTemplateHover, visualStore]);

    const handleNodeClick = useCallback(
        (_event: React.MouseEvent, node: Node<DagNodeData>) => {
            if (onTemplateSelect) {
                onTemplateSelect(node.id);
            } else {
                const tt = ttStatusByName[node.id];
                if (tt) {
                    navigate(
                        `/workflow/${workflowId}/task_template/${tt.id}`
                    );
                }
            }
        },
        [navigate, workflowId, ttStatusByName, onTemplateSelect]
    );

    const showPopover =
        popoverNodeId && popoverTTData && popoverPosition;

    const containerHeight = height ?? '100%';

    if (dagQuery.isLoading || nodes.length === 0) {
        return (
            <div
                style={{
                    height: containerHeight,
                    width: '100%',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                }}
            >
                <CircularProgress />
            </div>
        );
    }

    return (
        <DagVisualStoreContext.Provider value={visualStore}>
            <div
                style={{
                    height: containerHeight,
                    width: '100%',
                    position: 'relative',
                }}
            >
                <ReactFlow
                    nodes={structuralNodes}
                    edges={styledEdges}
                    nodeTypes={nodeTypes}
                    onNodeMouseEnter={handleNodeMouseEnter}
                    onNodeMouseLeave={handleNodeMouseLeave}
                    onNodeClick={handleNodeClick}
                    fitView
                    fitViewOptions={{ padding: 0.15 }}
                >
                    <Controls />
                    <Background />
                </ReactFlow>

                {showPopover && (
                    <TaskTemplatePopover
                        data={popoverTTData}
                        placement="top"
                        style={{
                            position: 'fixed',
                            left:
                                popoverPosition.x + POPOVER_OFFSET,
                            top:
                                popoverPosition.y + POPOVER_OFFSET,
                            zIndex: POPOVER_Z_INDEX,
                        }}
                    />
                )}
            </div>
        </DagVisualStoreContext.Provider>
    );
}

export default function WorkflowDAG(props: WorkflowDAGProps) {
    return (
        <ReactFlowProvider>
            <WorkflowDAGInner {...props} />
        </ReactFlowProvider>
    );
}
