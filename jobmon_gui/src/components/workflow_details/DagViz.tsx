import React from "react";
import ReactFlow, {MiniMap, Controls, Background} from 'reactflow';
import dagre from 'dagre';

export default function DagViz() {


    const getLayoutedNodes = (nodes, edges) => {
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


    const nodes = [
        {id: 'stage1', data: {label: 'stage1'}},
        {id: 'stage1_upload', data: {label: 'stage1_upload'}},
        {id: 'rake', data: {label: 'rake'}},
        {id: 'post_rake', data: {label: 'post_rake'}},
        {id: 'spacetime', data: {label: 'spacetime'}},
        {id: 'eval', data: {label: 'eval'}},
        {id: 'eval_upload', data: {label: 'eval_upload'}},
        {id: 'prep', data: {label: 'prep'}},
        {id: 'rake_upload', data: {label: 'rake_upload'}},
        {id: 'spacetime_upload', data: {label: 'spacetime_upload'}},
        {id: 'post_gpr', data: {label: 'post_gpr'}},
        {id: 'gpr_upload', data: {label: 'gpr_upload'}},
        {id: 'amp_nsv', data: {label: 'amp_nsv'}},
        {id: 'amp_nsv_upload', data: {label: 'amp_nsv_upload'}},
        {id: 'gpr', data: {label: 'gpr'}},
        {id: 'clean', data: {label: 'clean'}},
    ];

    const edges = [
        {id: 'e1', source: 'stage1', target: 'stage1_upload'},
        {id: 'e2', source: 'rake', target: 'post_rake'},
        {id: 'e3', source: 'stage1_upload', target: 'spacetime'},
        {id: 'e4', source: 'eval', target: 'eval_upload'},
        {id: 'e5', source: 'prep', target: 'stage1'},
        {id: 'e6', source: 'post_rake', target: 'rake_upload'},
        {id: 'e7', source: 'spacetime', target: 'spacetime_upload'},
        {id: 'e8', source: 'post_gpr', target: 'gpr_upload'},
        {id: 'e9', source: 'post_gpr', target: 'post_rake'},
        {id: 'e10', source: 'amp_nsv', target: 'amp_nsv_upload'},
        {id: 'e11', source: 'gpr', target: 'post_gpr'},
        {id: 'e12', source: 'gpr', target: 'rake'},
        {id: 'e13', source: 'rake_upload', target: 'clean'},
        {id: 'e14', source: 'spacetime_upload', target: 'amp_nsv'},
        {id: 'e15', source: 'gpr_upload', target: 'eval'},
        {id: 'e16', source: 'eval_upload', target: 'rake_upload'},
        {id: 'e17', source: 'amp_nsv_upload', target: 'gpr'},
    ];


    const layoutedNodes = getLayoutedNodes(nodes, edges);

    return (
        <div style={{height: 500}}>
            <ReactFlow nodes={layoutedNodes} edges={edges}>
                <MiniMap/>
                <Controls/>
                <Background/>
            </ReactFlow>
        </div>
    )
}