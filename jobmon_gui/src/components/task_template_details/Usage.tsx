import React from 'react';
import MemoryBoxPlot from '@jobmon_gui/components/task_template_details/MemoryBoxPlot';
import RuntimeBoxPlot from '@jobmon_gui/components/task_template_details/RuntimeBoxPlot';
import {formatBytes, bytes_to_gib} from '@jobmon_gui/utils/formatters'
import humanizeDuration from 'humanize-duration';
import {useQuery} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import {CircularProgress} from "@mui/material";
import {getWorkflowUsageQueryFn} from "@jobmon_gui/queries/GetWorkflowUsage.ts";

type UsageProps = {
    taskTemplateName: string
    taskTemplateVersionId: string
    workflowId: number | string
}

export default function Usage({taskTemplateName, taskTemplateVersionId, workflowId}: UsageProps) {
    const usageInfo = useQuery({
        queryKey: ["workflow_details", "usage", taskTemplateVersionId, workflowId],
        queryFn: getWorkflowUsageQueryFn,
        staleTime: 5000,
    })
    if (!taskTemplateName) {
        return (<Typography sx={{pt: 5}}>Could not retrieve resource usage for this task template.</Typography>)
    }

    if (usageInfo.isLoading) {
        return (<CircularProgress/>)
    }

    if (usageInfo.isError) {
        return (<Typography>Unable to retrieve resource usage. Please refresh and try again</Typography>)
    }
    var runtime: any = []
    var memory: any = []
    var run_mem = usageInfo.data?.[11]
    for (var item in run_mem) {
        var run = Number(run_mem[item].r)
        var mem = Number(run_mem[item].m)
        var taskId = run_mem[item].task_id;
        var requestedResources = run_mem[item].requested_resources ? JSON.parse(run_mem[item].requested_resources) : {};
        // Default time in Jobmon is 24 hours
        var requestedRuntimeValue = requestedResources.runtime || 86400;
        // Default time in Jobmon is 1GiB
        var requestedMemoryValue = requestedResources.memory || 1;

        if (run !== null && run !== 0) {
            var percentageRuntime = (run / requestedRuntimeValue) * 100;
            runtime.push({
                task_id: taskId,
                runtime: run,
                percentageRuntime: percentageRuntime,
                requestedRuntime: requestedRuntimeValue
            })
        }
        if (mem !== null && mem !== 0) {
            var percentageMemory = (bytes_to_gib(mem) / requestedMemoryValue) * 100;
            memory.push({
                task_id: taskId,
                memory: bytes_to_gib(mem),
                percentageMemory: percentageMemory,
                requestedMemory: requestedMemoryValue
            })
        }
    }


    return (
        <div>
            <div className="container w-100 mt-5">
                <p>
                    <b className='font-weight-bold'>TaskTemplate Name:</b> {taskTemplateName} <br></br>
                    <b className='font-weight-bold'>TaskTemplate Version ID:</b> {taskTemplateVersionId} <br></br>
                    <b className='font-weight-bold'>Number of Tasks in Summary
                        Calculation:</b> {usageInfo?.data[0]}</p>
                <div className="card-columns d-flex justify-content-center">
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header font-weight-bold">Memory</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {formatBytes(usageInfo?.data[1])}<br></br>
                                    Maximum: {formatBytes(usageInfo?.data[2])}<br></br>
                                    Mean: {formatBytes(usageInfo?.data[3])}<br></br>
                                    Median: {formatBytes(usageInfo?.data[7])}<br></br>
                                </p>
                            </div>
                        </div>
                    </div>
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header font-weight-bold">Runtime (Seconds)</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {usageInfo?.data[4] && humanizeDuration(usageInfo?.data[4] * 1000)}<br></br>
                                    Maximum: {humanizeDuration(usageInfo?.data[5] * 1000)}<br></br>
                                    Mean: {humanizeDuration(usageInfo?.data[6] * 1000)}<br></br>
                                    Median: {humanizeDuration(usageInfo?.data[8] * 1000)}<br></br>
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div className="center-histogram">
                <MemoryBoxPlot taskMemory={memory}/>
            </div>
            <div className="center-histogram">
                <RuntimeBoxPlot taskRuntime={runtime}/>
            </div>
        </div>
    )
}