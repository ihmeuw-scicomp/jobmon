import React from 'react';
import Plot from 'react-plotly.js';

export default function RuntimeHistogram({taskRuntime}) {
    return (
        <Plot
            data={[
                {
                    x: taskRuntime,
                    type: 'histogram',
                    marker: { color: '#3e853c' },
                }
            ]}
            layout={{ 
                width: 1300, 
                height: 300, 
                title: 'Runtime Histogram (seconds) - Linear Scale',
                updatemenus: [{
                    active: 1,
                    buttons: [{
                        label: "Log Scale",
                        method: 'update',
                        args: [
                            {'visible': [true, true]},
                            {'title': 'Runtime (seconds) - Log Scale', 'yaxis': {'type': 'log'}}
                        ],
                      },
                      {
                        label: "Linear Scale",
                        method: 'update',
                        args: [
                            {'visible': [true, false]},
                            {'title': 'Runtime (seconds) - Linear Scale', 'yaxis': {'type': 'linear'}}
                        ],
                      }
                    ]
                  }]
            }}
        />
    )
}