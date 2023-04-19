import React from 'react';
import Plot from 'react-plotly.js';

export default function MemoryHistogram({taskMemory}) {
    return (
        <Plot
            data={[
                {
                    x: taskMemory,
                    type: 'histogram',
                    marker: { color: '#3e853c' },
                }
            ]}
            layout={{ 
                width: 1300, 
                height: 300, 
                title: 'Memory Histogram (GiB) - Linear Scale',
                updatemenus: [{
                    active: 1,
                    buttons: [{
                        label: "Log Scale",
                        method: 'update',
                        args: [
                            {'visible': [true, true]},
                            {'title': 'Memory (GiB) - Log Scale', 'yaxis': {'type': 'log'}}
                        ],
                      },
                      {
                        label: "Linear Scale",
                        method: 'update',
                        args: [
                            {'visible': [true, false]},
                            {'title': 'Memory (GiB) - Linear Scale', 'yaxis': {'type': 'linear'}}
                        ],
                      }
                    ]
                  }]
            }}
        />
    )
}