import React from 'react';
import ProgressBar from 'react-bootstrap/ProgressBar';
import {OverlayTrigger} from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';

import '@jobmon_gui/styles/jobmon_gui.css';
import {useQuery} from "@tanstack/react-query";
import axios from "axios";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import {CircularProgress} from "@mui/material";
import Typography from "@mui/material/Typography";
import {workflow_tt_status_url} from "@jobmon_gui/configs/ApiUrls";
import {TTStatusResponse} from "@jobmon_gui/types/TaskTemplateStatus";

type WfDetails = {
    DONE: number,
    FATAL: number,
    MAXC: number,
    PENDING: number,
    RUNNING: number,
    SCHEDULED: number,
    id: number,
    num_attempts_avg: number,
    num_attempts_max: number,
    num_attempts_min: number,
    tasks: number,
}


type WfDetailsResponse = Record<string | number, WfDetails>


type JobmonProgressBarProps = {
    workflowId: number | string
    ttId?: string | number
    placement?: "top" | "bottom" | "left" | "right"
    style?: "striped" | "animated"
}
export default function JobmonProgressBar({
                                              workflowId,
                                              ttId,
                                              placement = "bottom",
                                              style = "striped"
                                          }: JobmonProgressBarProps) {

    // style can be striped or animated
    const workflow_status = useQuery({
        queryKey: ["workflow_details", "progress_bar", workflowId],
        queryFn: async () => {
            const url = import.meta.env.VITE_APP_BASE_URL + "/workflow_status_viz";
            const wf_ids = [workflowId];
            return axios.get<WfDetailsResponse>(url,
                {
                    ...jobmonAxiosConfig,
                    data: null,
                    params: {workflow_ids: wf_ids},
                }
            ).then((r) => {
                return r.data[workflowId]
            })
        },
    })

    const wfTTStatus = useQuery({
        queryKey: ["workflow_details", "tt_status", workflowId],
        queryFn: async () => {

            return axios.get<TTStatusResponse>(workflow_tt_status_url + workflowId, {
                    ...jobmonAxiosConfig,
                    data: null,
                }
            ).then((r) => {
                return r.data
            })
        },
    })

    if (workflow_status.isLoading) {
        return (<CircularProgress/>)
    }
    if (workflow_status.isError) {
        return (<Typography>Unable to retrieve workflow status. Please reload and try again.</Typography>)
    }

    if (!!ttId && wfTTStatus.isLoading) {
        return (<CircularProgress/>)
    }
    if (!!ttId && wfTTStatus.isError) {
        return (<Typography>Error loading workflow task template details. Please refresh and try again.</Typography>)
    }

    const data = !!ttId ? wfTTStatus.data[ttId] : workflow_status.data

    if (!data) {
        return (<CircularProgress/>)
    }

    const num_attempts_avg = parseFloat(data.num_attempts_avg.toString()).toFixed(1);
    const INT_32_MAX = 2147483647
    const maxc = data.MAXC === INT_32_MAX ? "No Limit" : data.MAXC;

    // Animated
    if (style === "animated") {
        return (
            <OverlayTrigger
                placement={placement}
                trigger={["hover", "focus"]}
                overlay={(
                    <Popover id="task_count">
                        <table id="tt-tasks">
                            <tbody>
                            <tr>
                                <th className="scheduled">Scheduled:</th>
                                <td>{data.SCHEDULED}</td>
                            </tr>
                            <tr>
                                <th className="pending"> Pending:</th>
                                <td>{data.PENDING}</td>
                            </tr>
                            <tr>
                                <th className="running">Running:</th>
                                <td>{data.RUNNING}</td>
                            </tr>
                            <tr>
                                <th className="done">Done:</th>
                                <td>{data.DONE}</td>
                            </tr>
                            <tr>
                                <th className="fatal">Fatal:</th>
                                <td>{data.FATAL}</td>
                            </tr>
                            <tr>
                                <th> Total:</th>
                                <td>{data.tasks}</td>
                            </tr>
                            </tbody>
                        </table>
                        <hr/>
                        <table id="tt-stats">
                            <tbody>
                            <tr>
                                <th># Attempts:</th>
                                <td>{num_attempts_avg} ({data.num_attempts_min} - {data.num_attempts_max})</td>
                            </tr>
                            <tr>
                                <th>Concurrency Limit:</th>
                                <td>{maxc.toLocaleString()}</td>
                            </tr>
                            </tbody>
                        </table>
                    </Popover>
                )}
            >

                <ProgressBar>
                    <ProgressBar className="pending-progress-bar" animated max={data.tasks}
                                 now={data.PENDING} key={1}
                                 isChild={true}
                                 label={((data.PENDING / data.tasks) * 100).toFixed(1) + "%"}/>
                    <ProgressBar className="scheduled-progress-bar" animated max={data.tasks}
                                 now={data.SCHEDULED} key={2}
                                 isChild={true}
                                 label={((data.SCHEDULED / data.tasks) * 100).toFixed(1) + "%"}/>
                    <ProgressBar className="running-progress-bar" animated max={data.tasks}
                                 now={data.RUNNING} key={3}
                                 isChild={true}
                                 label={((data.RUNNING / data.tasks) * 100).toFixed(1) + "%"}/>
                    <ProgressBar className="done-progress-bar" animated max={data.tasks}
                                 now={data.DONE} key={4} isChild={true}
                                 label={((data.DONE / data.tasks) * 100).toFixed(1) + "%"}/>
                    <ProgressBar className="fatal-progress-bar" animated max={data.tasks}
                                 now={data.FATAL} key={5} isChild={true}
                                 label={((data.FATAL / data.tasks) * 100).toFixed(1) + "%"}/>
                </ProgressBar>
            </OverlayTrigger>

        );
    }
    // Striped
    return (
        <OverlayTrigger
            placement={placement}
            trigger={["hover", "focus"]}
            overlay={(
                <Popover id="task_count">
                    <table id="tt-tasks">
                        <tbody>
                        <tr>
                            <th className="scheduled">Scheduled:</th>
                            <td>{data.SCHEDULED}</td>
                        </tr>
                        <tr>
                            <th className="pending"> Pending:</th>
                            <td>{data.PENDING}</td>
                        </tr>
                        <tr>
                            <th className="running">Running:</th>
                            <td>{data.RUNNING}</td>
                        </tr>
                        <tr>
                            <th className="done">Done:</th>
                            <td>{data.DONE}</td>
                        </tr>
                        <tr>
                            <th className="fatal">Fatal:</th>
                            <td>{data.FATAL}</td>
                        </tr>
                        <tr>
                            <th className='bg-dark text-light'> Total:</th>
                            <td>{data.tasks}</td>
                        </tr>
                        </tbody>
                    </table>
                    <hr/>
                    <table id="tt-stats">
                        <tbody>
                        <tr>
                            <th># Attempts:</th>
                            <td>{num_attempts_avg} ({data.num_attempts_min} - {data.num_attempts_max})</td>
                        </tr>
                        <tr>
                            <th>Concurrency Limit:</th>
                            <td>{maxc.toLocaleString()}</td>
                        </tr>
                        </tbody>
                    </table>
                </Popover>
            )}
        >

            <ProgressBar>
                <ProgressBar className="pending-progress-bar" max={data.tasks}
                             now={data.PENDING} key={1} isChild={true}
                             label={((data.PENDING / data.tasks) * 100).toFixed(1) + "%"}/>
                <ProgressBar className="scheduled-progress-bar" max={data.tasks}
                             now={data.SCHEDULED} key={2} isChild={true}
                             label={((data.SCHEDULED / data.tasks) * 100).toFixed(1) + "%"}/>
                <ProgressBar className="running-progress-bar" max={data.tasks}
                             now={data.RUNNING} key={3} isChild={true}
                             label={((data.RUNNING / data.tasks) * 100).toFixed(1) + "%"}/>
                <ProgressBar className="done-progress-bar" max={data.tasks}
                             now={data.DONE} key={4} isChild={true}
                             label={((data.DONE / data.tasks) * 100).toFixed(1) + "%"}/>
                <ProgressBar className="fatal-progress-bar" max={data.tasks}
                             now={data.FATAL} key={5} isChild={true}
                             label={((data.FATAL / data.tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
        </OverlayTrigger>

    );
}
