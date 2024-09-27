import React, {useEffect} from 'react';
import MemoryHistogram from '@jobmon_gui/components/workflow_details/MemoryHistogram';
import RuntimeHistogram from '@jobmon_gui/components/workflow_details/RuntimeHistogram';
import {formatBytes, bytes_to_gib} from '@jobmon_gui/utils/formatters'
import {safe_rum_start_span, safe_rum_unit_end} from '@jobmon_gui/utils/rum'
import humanizeDuration from 'humanize-duration';
import {task_table_url, usage_url} from "@jobmon_gui/configs/ApiUrls";
import {useQuery} from "@tanstack/react-query";
import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import Typography from "@mui/material/Typography";
import {CircularProgress} from "@mui/material";

type UsageProps = {
    taskTemplateName: string
    taskTemplateVersionId: string
    workflowId: number | string
}

export default function Usage({taskTemplateName, taskTemplateVersionId, workflowId}: UsageProps) {
    const usageInfo = useQuery({
        queryKey: ["workflow_details", "usage", taskTemplateVersionId, workflowId],
        queryFn: async () => {
            return await axios.post(
                usage_url,
                {
                    task_template_version_id: taskTemplateVersionId,
                    workflows: [workflowId],
                    viz: true
                }, jobmonAxiosConfig,
            ).then((r) => {
                return r.data
            })
        },
        staleTime: 5000,
    })

    var runtime: any = []
    var memory: any = []
    var run_mem = usageInfo.data?.[11]
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

    if (!taskTemplateName) {
        return (<Typography sx={{pt: 5}}>Select a task template from above to view resource usage</Typography>)
    }

    if (usageInfo.isLoading) {
        return (<CircularProgress/>)
    }

    if (usageInfo.isError) {
        return (<Typography>Unable to retrieve resource usage. Please refresh and try again</Typography>)
    }

    return (
        <div>
            <div className="container w-100 mt-5">
                <p>
                    <b className='font-weight-bold'>TaskTemplate Name:</b> {taskTemplateName} <br></br>
                    <b className='font-weight-bold'>TaskTemplate Version ID:</b> {taskTemplateVersionId} <br></br>
                    <b className='font-weight-bold'>Number of Tasks in Summary Calculation:</b> {usageInfo.data[0]}</p>
                <div className="card-columns d-flex justify-content-center">
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header font-weight-bold">Memory</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {formatBytes(usageInfo.data[1])}<br></br>
                                    Maximum: {formatBytes(usageInfo.data[2])}<br></br>
                                    Mean: {formatBytes(usageInfo.data[3])}<br></br>
                                    Median: {formatBytes(usageInfo.data[7])}<br></br>
                                </p>
                            </div>
                        </div>
                    </div>
                    <div className="card">
                        <div className="card-block">
                            <div className="card-header font-weight-bold">Runtime (Seconds)</div>
                            <div className="card-body">
                                <p className="card-text">
                                    Minimum: {humanizeDuration(usageInfo.data[4] * 1000)}<br></br>
                                    Maximum: {humanizeDuration(usageInfo.data[5] * 1000)}<br></br>
                                    Mean: {humanizeDuration(usageInfo.data[6] * 1000)}<br></br>
                                    Median: {humanizeDuration(usageInfo.data[8] * 1000)}<br></br>
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
        </div>
    )
}