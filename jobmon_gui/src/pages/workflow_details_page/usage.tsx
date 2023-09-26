import React from 'react';
import MemoryHistogram from './memory_histogram';
import RuntimeHistogram from './runtime_histogram';
import { formatBytes, formatNumber, bytes_to_gib, convertTime } from '../../utilities/formatters'
import { safe_rum_start_span, safe_rum_unit_end } from '../../utilities/rum'

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

    function formatRuntime(runtime) {
        const formatted_runtime = convertTime(formatNumber(runtime), false)
        return `${formatted_runtime.d} days ${formatted_runtime.h} hours ${formatted_runtime.m} minutes ${formatted_runtime.s} seconds`
    }
    return (
        <div>
            <div className="container w-100 mt-5">
                <p>
                    <b className='font-weight-bold'>TaskTemplate Name:</b> {taskTemplateName} <br></br>
                    <b className='font-weight-bold'>TaskTemplate Version ID:</b> {taskTemplateVersionId} <br></br>
                    <b className='font-weight-bold'>Number of Tasks in Summary Calculation:</b> {usageInfo[0]}</p>
                <div className="card-columns d-flex justify-content-center">
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header font-weight-bold">Memory</div>
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
                            <div className="card-header font-weight-bold">Runtime (Seconds)</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {formatRuntime(usageInfo[4])}<br></br>
                                    Maximum: {formatRuntime(usageInfo[5])}<br></br>
                                    Mean: {formatRuntime(usageInfo[6])}<br></br>
                                    Median: {formatRuntime(usageInfo[8])}<br></br>
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