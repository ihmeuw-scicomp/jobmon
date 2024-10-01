import React from 'react';
import Plot from 'react-plotly.js';

export default function RuntimeBoxPlot({taskRuntime}) {
    const requestedRuntimes = taskRuntime.map(item => item.requestedRuntime)
    const hoverText = taskRuntime.map(item => `Task ID: ${item.task_id}<br />Requested Runtime: ${item.requestedRuntime}`);
    const runtimes = taskRuntime.map(item => item.runtime);
    const percentageRuntimes = taskRuntime.map(item => item.percentageRuntime);

    return (
        <Plot
            data={[
                {
                    y: runtimes,
                    type: 'box',
                    name: 'Utilized Runtime (seconds)',
                    marker: {color: '#3e853c'},
                    boxpoints: 'all',
                    text: hoverText,
                    hoverinfo: 'text+y',
                    xaxis: 'x1',
                    yaxis: 'y1',
                },
                {
                    y: percentageRuntimes,
                    type: 'box',
                    name: 'Utilization vs. Requested Runtime (%)',
                    marker: {color: '#1f77b4'},
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
                grid: {rows: 1, columns: 2, pattern: 'independent'},
                title: 'Runtime Box Plots',
            }}
        />
    );
}
