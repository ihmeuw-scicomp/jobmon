import React, { useEffect } from 'react';
import MemoryHistogram from './memory_histogram';
import RuntimeHistogram from './runtime_histogram';
import { formatBytes, bytes_to_gib } from '../../utilities/formatters'
import { safe_rum_start_span, safe_rum_unit_end } from '../../utilities/rum'
import humanizeDuration from 'humanize-duration';

export default function Usage({ taskTemplateName, taskTemplateVersionId, usageInfo, apm}) {

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

    useEffect(() => {
        const s = safe_rum_start_span(apm, "tasks", "custom");
        return () => {
            safe_rum_unit_end(s);
        };
    }, [apm]);

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
                                    Minimum: {humanizeDuration(usageInfo[4] * 1000)}<br></br>
                                    Maximum: {humanizeDuration(usageInfo[5] * 1000)}<br></br>
                                    Mean: {humanizeDuration(usageInfo[6] * 1000)}<br></br>
                                    Median: {humanizeDuration(usageInfo[8] * 1000)}<br></br>
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
}