import React from 'react';
import Plot from 'react-plotly.js';

export default function MemoryBoxPlot({ taskMemory }) {
    const hoverText = taskMemory.map(item => `Task ID: ${item.task_id}<br />Requested Memory (GiB): ${item.requestedMemory}`);
    const memory = taskMemory.map(item => item.memory);
    const percentageMemory = taskMemory.map(item => item.percentageMemory);

    return (
        <Plot
            data={[
                {
                    y: memory,
                    type: 'box',
                    name: 'Utilized Memory (GiB)',
                    marker: { color: '#3e853c' },
                    boxpoints: 'all',
                    text: hoverText,
                    hoverinfo: 'text+y',
                    xaxis: 'x1',
                    yaxis: 'y1',
                },
                {
                    y: percentageMemory,
                    type: 'box',
                    name: 'Utilization vs. Requested Memory (%)',
                    marker: { color: '#1f77b4' },
                    boxpoints: 'all',
                    text: hoverText,
                    hoverinfo: 'text+y',
                    xaxis: 'x2',
                    yaxis: 'y2',
                }
            ]}
            layout={{
                width: 1000,
                height: 600,
                grid: { rows: 1, columns: 2, pattern: 'independent' },
                title: 'Memory Box Plots'
            }}
        />
    );
}
