import React from 'react';
import MemoryHistogram from './memory_histogram';
import RuntimeHistogram from './runtime_histogram';
import { formatBytes, formatNumber, bytes_to_gib } from '../functions'

import { safe_rum_start_span, safe_rum_unit_end } from '../functions'

export default function Usage({ taskTemplateName, taskTemplateVersionId, usageInfo, apm}) {
    const s: any = safe_rum_start_span(apm, "resource_usage", "custom");

    var runtime: any = []
    var memory: any = []
    var run_mem = usageInfo[11]
    for (var item in run_mem) {
        var run = run_mem[item].r
        var mem = run_mem[item].m
        if (run !== null && run !== 0 && run !== "0") {
            runtime.push(run)
        }
        if (mem !== null && mem !== 0 && mem !== "0") {
            memory.push(bytes_to_gib(mem))
        }
    }

    return (
        <div>
            <header className="App-header">
                <h3>Resource Usage Summary</h3>
            </header>
            <div className="container">
                <p>
                    <b>TaskTemplate Name:</b> {taskTemplateName} <br></br>
                    <b>TaskTemplate Version ID:</b> {taskTemplateVersionId} <br></br>
                    <b>Number of Tasks in Summary Calulation:</b> {usageInfo[0]}</p>
                <div className="card-columns d-flex justify-content-center">
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header">Memory</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {formatBytes(usageInfo[1])}<br></br>
                                    Maximum: {formatBytes(usageInfo[2])}<br></br>
                                    Mean: {formatBytes(usageInfo[3])}<br></br>
                                    Median: {formatBytes(usageInfo[7])}<br></br>
                                </p>
                            </div>
                        </div>
                    </div>
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header">Runtime (Seconds)</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {formatNumber(usageInfo[4])}<br></br>
                                    Maximum: {formatNumber(usageInfo[5])}<br></br>
                                    Mean: {formatNumber(usageInfo[6])}<br></br>
                                    Median: {formatNumber(usageInfo[8])}<br></br>
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div className="center-histogram">
                <MemoryHistogram taskMemory={memory}/>
            </div>
            <div className="center-histogram">
                <RuntimeHistogram taskRuntime={runtime}/>
            </div>
        </div >
    )
    safe_rum_unit_end(s);
}