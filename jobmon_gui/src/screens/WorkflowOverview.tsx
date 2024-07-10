import React, {useEffect, useState, useRef} from 'react';
import {useForm} from 'react-hook-form';
import {useSearchParams, useNavigate} from 'react-router-dom';
import {Form, Row, Col} from 'react-bootstrap';
import axios from 'axios';

import 'bootstrap/dist/css/bootstrap.min.css';

import WorkflowTable from '@jobmon_gui/components/workflow_overview/WorkflowTable';
import WorkflowStatus from '@jobmon_gui/components/workflow_overview/WorkflowStatus';

import {init_apm, safe_rum_add_label, safe_rum_start_span, safe_rum_unit_end} from '@jobmon_gui/utils/rum';
import '@jobmon_gui/styles/jobmon_gui.css';
import {Button, FormControl, Grid, InputLabel, MenuItem, Select, TextField} from "@mui/material";
import {DatePicker, LocalizationProvider} from "@mui/x-date-pickers";
import {AdapterDayjs} from "@mui/x-date-pickers/AdapterDayjs";
import dayjs from "dayjs";
import {useWorkflowSearchSettings} from "@jobmon_gui/stores/workflow_settings";
import {useQuery} from "@tanstack/react-query";
import {workflow_status_url} from "@jobmon_gui/configs/ApiUrls";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import Typography from "@mui/material/Typography";


function WorkflowOverview() {
    const apm: any = init_apm("workflow_overview_page");
    const [refresh, setRefresh] = useState(false)
    const workflowSettings = useWorkflowSearchSettings()
    // const [workflows, setWorkflows] = useState([])
    const [searchParams, setSearchParams] = useSearchParams();

    const workflows = useQuery({
        queryKey: ["workflow_overview", "workflows"],
        queryFn: async () => {
            setRefresh(false)
            const params = new URLSearchParams({
                user: workflowSettings.get().user,
                tool: workflowSettings.get().tool,
                wf_name: workflowSettings.get().wf_name,
                wf_args: workflowSettings.get().wf_args,
                wf_attribute_key: workflowSettings.get().wf_attribute_key,
                wf_attribute_value: workflowSettings.get().wf_attribute_value,
                wf_id: workflowSettings.get().wf_id,
                date_submitted: dayjs(workflowSettings.get().date_submitted).format("YYYY-MM-DD"),
                status: workflowSettings.get().status
            });
            return axios.get(workflow_status_url, {...jobmonAxiosConfig, params: params}).then((response) => {
                response.data?.workflows?.forEach((workflow) => {
                    workflow.wf_status = <WorkflowStatus status={workflow.wf_status}/>;
                })

                return response.data?.workflows
            })
        },
        enabled: refresh
    })

    useEffect(() => workflowSettings.loadValuesFromSearchParams(searchParams), [])

    const handleClear = () => {
        workflowSettings.clear()
        setRefresh(true)
    }

    const ShowWFTable = () => {
        return (
            <div id="wftable" className="div-level-2">
                {/*If there are no workflows and at least one URL search param is not empty*/}
                {workflows?.data?.length === 0 ? (
                    <Typography>No workflows found for specified filters.</Typography>
                ) : workflows?.data?.length !== 0 ? (
                    <WorkflowTable allData={workflows?.data}/>
                ) : null}
            </div>
        )
    }

    const handleSubmit = (event) => {
        setRefresh(true)
        event.preventDefault();
    }

    return (
        <div id="div-main" className="App">
            <div id="div-header" className="div-level-2">
                <header className="App-header">
                </header>
            </div>

            <div className="div-level-2">
                <form onSubmit={handleSubmit}>
                    <Grid container spacing={2}>
                        <Grid item xs={3}>
                            <TextField label="Username"
                                       fullWidth={true}
                                       value={workflowSettings.get().user}
                                       onChange={(e) => workflowSettings.setUser(e.target.value)}/>
                        </Grid>
                        <Grid item xs={3}>
                            <TextField label="Workflow Args"
                                       fullWidth={true}
                                       value={workflowSettings.get().wf_args}
                                       onChange={(e) => workflowSettings.setWfArgs(e.target.value)}/>
                        </Grid>
                        <Grid item xs={1.5}>
                            <TextField label="Workflow Attribute Key"
                                       fullWidth={true}
                                       value={workflowSettings.get().wf_attribute_key}
                                       onChange={(e) => workflowSettings.setWfAttributeKey(e.target.value)}/>

                        </Grid>
                        <Grid item xs={1.5}>
                            <TextField label="Workflow Attribute Value" fullWidth={true}
                                       value={workflowSettings.get().wf_attribute_value}
                                       onChange={(e) => workflowSettings.setWfAttributeValue(e.target.value)}/>

                        </Grid>
                        <Grid item xs={3}>
                            <TextField label="Tool"
                                       fullWidth={true}
                                       value={workflowSettings.get().tool}
                                       onChange={(e) => workflowSettings.setTool(e.target.value)}/>

                        </Grid>
                        <Grid item xs={3}>
                            {/*fullWidth={true}*/}
                            {/*          defaultValue={two_weeks_ago_date}*/}
                            <LocalizationProvider dateAdapter={AdapterDayjs}>
                                <DatePicker
                                    label={"Submitted Workflow Date - On or After"}
                                    value={dayjs(workflowSettings.get().date_submitted) || dayjs("1970-01-01")}
                                    onChange={(value) => workflowSettings.setDateSubmitted(value)}
                                    sx={{width: "100%"}}

                                />
                            </LocalizationProvider>

                        </Grid>
                        <Grid item xs={3}>
                            <TextField label="Workflow Name"
                                       fullWidth={true}
                                       value={workflowSettings.get().wf_name}
                                       onChange={(e) => workflowSettings.setWfName(e.target.value)}/>

                        </Grid>
                        <Grid item xs={3}>
                            <FormControl fullWidth={true}>
                                <InputLabel id="LABEL-workflow-status">Workflow Status</InputLabel>
                                <Select labelId="LABEL-workflow-status"

                                        label="Workflow Status"
                                        id={"SELECT-workflow-status"}
                                        onChange={(e) => workflowSettings.setStatus(e.target.value)}
                                        value={workflowSettings.get().status}
                                        fullWidth={true}>
                                    <MenuItem>{undefined}</MenuItem>
                                    <MenuItem value="A">Aborted</MenuItem>
                                    <MenuItem value="D">Done</MenuItem>
                                    <MenuItem value="F">Failed</MenuItem>
                                    <MenuItem value="G">Registering</MenuItem>
                                    <MenuItem value="H">Halted</MenuItem>
                                    <MenuItem value="I">Instantiating</MenuItem>
                                    <MenuItem value="O">Launched</MenuItem>
                                    <MenuItem value="Q">Queued</MenuItem>
                                    <MenuItem value="R">Running</MenuItem>
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={3}>
                            <TextField label="Workflow ID" fullWidth={true}
                                       value={workflowSettings.get().wf_id}
                                       onChange={(e) => workflowSettings.setWfId(e.target.value)}/>

                        </Grid>
                        <Grid item xs={12}>
                            <Grid container spacing={2}>
                                <Grid item xs={4}/>
                                <Grid item xs={2}>
                                    <Button variant="contained" type="submit">
                                        Submit
                                    </Button>
                                </Grid>
                                <Grid item xs={2}>

                                    <Button variant="contained" onClick={() => handleClear()}>Clear
                                        All
                                    </Button>
                                </Grid>
                                <Grid item xs={4}/>
                            </Grid>
                        </Grid>
                    </Grid>
                </form>
            </div>
            <ShowWFTable/>
        </div>
    );
}

export default WorkflowOverview;
