import React from 'react';
import Plot from 'react-plotly.js';

export default function RuntimeBoxPlot({ taskRuntime }) {
    const hoverText = taskRuntime.map(item => `Task ID: ${item.task_id}`);
    const runtimes = taskRuntime.map(item => item.runtime);
    const requestedRuntimes = taskRuntime.map(item => item.requestedRuntime); // Extract requestedRuntime

    return (
        <Plot
            data={[
                {
                    y: runtimes,
                    type: 'box',
                    name: 'Utilized Runtime',
                    marker: { color: '#3e853c' },
                    boxpoints: 'all',
                    text: hoverText,
                    hoverinfo: 'text+y',
                },
                {
                    y: requestedRuntimes, // Use requestedRuntime here
                    type: 'box',
                    name: 'Requested Runtime',
                    marker: { color: '#1f77b4' },
                    boxpoints: 'all',
                    text: hoverText, // You can change this if you want separate hover text for requested runtimes
                    hoverinfo: 'text+y',
                }
            ]}
            layout={{
                width: 1000,
                height: 600,
                title: 'Runtime Box Plot - Linear Scale',
                updatemenus: [{
                    active: 1,
                    buttons: [
                        {
                            label: "Log Scale",
                            method: 'relayout',
                            args: [{ 'yaxis.type': 'log' }, { 'title': 'Runtime Box Plot - Log Scale' }],
                        },
                        {
                            label: "Linear Scale",
                            method: 'relayout',
                            args: [{ 'yaxis.type': 'linear' }, { 'title': 'Runtime Box Plot - Linear Scale' }],
                        }
                    ]
                }],
                yaxis: {
                    title: 'Runtime',
                },
                xaxis: {
                    title: 'Tasks',
                },
            }}
        />
    );
}
