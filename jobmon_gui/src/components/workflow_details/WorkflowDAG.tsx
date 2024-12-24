import React, {useState, useMemo, useEffect, useRef} from "react";
import ReactFlow, {MiniMap, Controls, Background, useReactFlow} from 'reactflow';
import dagre from 'dagre';
import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {get_task_template_dag, workflow_tt_status_url} from "@jobmon_gui/configs/ApiUrls.ts";
import TaskTemplatePopover from "@jobmon_gui/components/TaskTemplatePopover.tsx";
import {useQuery} from "@tanstack/react-query";
import {TTStatusResponse} from "@jobmon_gui/types/TaskTemplateStatus.ts";

export default function WorkflowDAG(workflowId) {
    const [nodes, setNodes] = useState([]);
    const [edges, setEdges] = useState([]);
    const [selectedNode, setSelectedNode] = useState(null);
    const [popoverPosition, setPopoverPosition] = useState({x: 0, y: 0});
    const [nodeData, setNodeData] = useState(null);
    const popoverRef = useRef(null);
    const [tt_data, setTTData] = useState<typeof wfTTStatus.data[any] | undefined>(undefined);

    const wfTTStatus = useQuery({
        queryKey: ['workflow_details', 'tt_status', workflowId.workflowId],
        queryFn: async () => {
            return axios
                .get<TTStatusResponse>(workflow_tt_status_url + workflowId.workflowId, {
                    ...jobmonAxiosConfig,
                    data: null,
                })
                .then((r) => {
                    return r.data;
                });
        },
    });

    const createNodesAndEdgesFromTTDAG = (tt_dag) => {
        const nodes = [];
        const edges = [];
        const nodeSet = new Set();

        tt_dag.forEach((task, index) => {
            const sourceId = task.name;
            const targetId = task.downstream_task_template_id;

            if (!nodeSet.has(sourceId)) {
                nodes.push({id: sourceId, data: {label: sourceId}});
                nodeSet.add(sourceId);
            }
            if (targetId && !nodeSet.has(targetId)) {
                nodes.push({id: targetId, data: {label: targetId}});
                nodeSet.add(targetId);
            }

            if (targetId) {
                edges.push({id: `e${index}`, source: sourceId, target: targetId});
            }
        });

        return {nodes, edges};
    };

    useEffect(() => {
        if (!workflowId.workflowId) return;

        axios.get(get_task_template_dag(workflowId.workflowId), {
            ...jobmonAxiosConfig,
            data: null,
        }).then((r) => {
            const {nodes, edges} = createNodesAndEdgesFromTTDAG(r.data.tt_dag);
            setNodes(nodes);
            setEdges(edges);
        }).catch((error) => {
            console.error('Error fetching task template DAG:', error);
        });
    }, [workflowId]);

    const getLayoutNodes = (nodes, edges) => {
        const g = new dagre.graphlib.Graph();
        g.setGraph({
            rankdir: 'TB',
            ranksep: 60,
            nodesep: 50,
            marginx: 20,
            marginy: 20,
        });
        g.setDefaultEdgeLabel(() => ({}));

        nodes.forEach((node) => {
            g.setNode(node.id, {width: 172, height: 36});
        });

        edges.forEach((edge) => {
            g.setEdge(edge.source, edge.target, {
                minlen: 1,
                weight: 1,
            });
        });

        dagre.layout(g);

        nodes.forEach((node) => {
            const nodeWithPosition = g.node(node.id);
            node.position = {
                x: nodeWithPosition.x - 172 / 2,
                y: nodeWithPosition.y - 36 / 2,
            };
        });

        return nodes;
    };

    const laidOutNodes = useMemo(() => {
        return getLayoutNodes(nodes, edges);
    }, [nodes, edges]);

    const handleNodeHover = (event, node) => {
        setSelectedNode(node);
        setPopoverPosition(node.position);
        setTTData(Object.values(wfTTStatus.data).find(item => item.name === node.id))
    };

    const handleNodeLeave = () => {
        setSelectedNode(null);
    };

    useEffect(() => {
        const handleClickOutside = (event) => {
            if (popoverRef.current && !popoverRef.current.contains(event.target)) {
                setSelectedNode(null);
            }
        };

        document.addEventListener("mousedown", handleClickOutside);

        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    return (
        <div style={{height: 500}}>
            <ReactFlow
                nodes={laidOutNodes}
                edges={edges}
                onNodeMouseEnter={handleNodeHover}
                onNodeMouseLeave={handleNodeLeave}
            >
                <MiniMap/>
                <Controls/>
                <Background/>
            </ReactFlow>

            {selectedNode && tt_data && (
                <TaskTemplatePopover
                    ref={popoverRef}
                    data={tt_data}
                    placement="top"
                    style={{
                        position: 'absolute',
                        left: popoverPosition.x + 300,
                        top: popoverPosition.y + 300,
                        transform: 'translate(-50%, -50%)',
                    }}
                />
            )}
        </div>
    );
}
