import React from 'react';
import Plot from 'react-plotly.js';

export default function MemoryHistogram({taskMemory}) {
    const hoverText = taskMemory.map(item =>
        `Task ID: ${item.task_id}`
    );

    const memory = taskMemory.map(item => item.memory);

    return (
        <Plot
            data={[
                {
                    y: memory,
                    type: 'box',
                    marker: {color: '#3e853c'},
                    boxpoints: 'all',
                    text: hoverText,
                    hoverinfo: 'text+y',
                }
            ]}
            layout={{
                width: 1000,
                height: 600,
                title: 'Memory Box Plot - Linear Scale',
                updatemenus: [{
                    active: 1,
                    buttons: [{
                        label: "Log Scale",
                        method: 'relayout',
                        args: [{'yaxis.type': 'log'}, {'title': 'Memory Box Plot - Log Scale'}],
                    },
                        {
                            label: "Linear Scale",
                            method: 'relayout',
                            args: [{'yaxis.type': 'linear'}, {'title': 'Memory Box Plot - Linear Scale'}],
                        }]
                }]
            }}
        />
    );
}