import React, {useState, useMemo, useEffect} from "react";
import ReactFlow, {MiniMap, Controls, Background} from 'reactflow';
import dagre from 'dagre';
import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {get_task_template_dag} from "@jobmon_gui/configs/ApiUrls.ts";
import {useQuery} from "@tanstack/react-query";
import TaskTemplateTotalPopover from "@jobmon_gui/components/TaskTemplateTotalPopover.tsx";

export default function WorkflowDAG(workflowId) {
    const [nodes, setNodes] = useState([]);
    const [edges, setEdges] = useState([]);

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

    // useQuery({
    //     queryKey: ['taskTemplateDAG', workflowId.workflowId],
    //     queryFn: async () => {
    //         const response = await axios.get(get_task_template_dag(workflowId.workflowId), {
    //             ...jobmonAxiosConfig,
    //             data: null,
    //         });
    //         const {nodes, edges} = createNodesAndEdgesFromTTDAG(response.data.tt_dag);
    //         setNodes(nodes);
    //         setEdges(edges);
    //     },
    // });

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
    

    return (
        <div style={{height: 500}}>
            <ReactFlow nodes={laidOutNodes} edges={edges}>
                <MiniMap/>
                <Controls/>
                <Background/>
            </ReactFlow>
        </div>
    )
}