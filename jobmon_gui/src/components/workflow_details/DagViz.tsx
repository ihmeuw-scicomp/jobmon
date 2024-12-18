import React, {useState, useEffect} from "react";
import ReactFlow, {MiniMap, Controls, Background} from 'reactflow';
import dagre from 'dagre';
import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {get_task_template_dag} from "@jobmon_gui/configs/ApiUrls.ts";

export default function DagViz(workflowId) {
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

    useEffect(() => {
        if (!workflowId.workflowId) return;

        console.log("!!!!!! WORKFLOW ID !!!!!", workflowId.workflowId)

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
        g.setGraph({rankdir: 'TB'});
        g.setDefaultEdgeLabel(() => ({}));

        nodes.forEach((node) => {
            g.setNode(node.id, {width: 172, height: 36});
        });

        edges.forEach((edge) => {
            g.setEdge(edge.source, edge.target);
        });

        dagre.layout(g);

        nodes.forEach((node) => {
            const nodeWithPosition = g.node(node.id);
            node.position = {x: nodeWithPosition.x, y: nodeWithPosition.y};
        });

        return nodes;
    };

    const laidOutNodes = getLayoutNodes(nodes, edges);

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