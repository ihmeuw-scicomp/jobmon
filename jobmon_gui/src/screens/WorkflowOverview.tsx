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


function WorkflowOverview() {
    const apm: any = init_apm("workflow_overview_page");
    const [refresh, setRefresh] = useState(false)
    const workflowSettings = useWorkflowSearchSettings()
    const [workflows, setWorkflows] = useState([])
    const navigate = useNavigate();


    useEffect(() => {
        console.log("refreshing data")
        if (!refresh) {
            return
        }
        setRefresh(false)
        const rum_s1: any = safe_rum_start_span(apm, "landing_page", "external.http");
        // safe_rum_add_label(rum_s1, "user", user);
        const params = new URLSearchParams({
            user: workflowSettings.get().user,
            tool: workflowSettings.get().tool,
            wf_name: workflowSettings.get().wf_name,
            wf_args: workflowSettings.get().wf_args,
            wf_attribute_key: workflowSettings.get().wf_attribute_key,
            wf_attribute_value: workflowSettings.get().wf_attribute_value,
            wf_id: workflowSettings.get().wf_id,
            date_submitted: workflowSettings.get().date_submitted.format("YYYY-MM-DD"),
            status: workflowSettings.get().status
        });
        const workflow_status_url = import.meta.env.VITE_APP_BASE_URL + "/workflow_overview_viz";
        const fetchData = async () => {
            const result: any = await axios({
                    method: 'get',
                    url: workflow_status_url,
                    params: params,
                    data: null,
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                }
            );
            let wfs = result.data.workflows;
            wfs.forEach((workflow) => {
                workflow.wf_status = <WorkflowStatus status={workflow.wf_status}/>;
            });
            setWorkflows(wfs)
        };
        fetchData();
        safe_rum_unit_end(rum_s1);
    }, [refresh]);


    const handleClear = () => {
        workflowSettings.clear()
        setRefresh(true)
    }

    const ShowWFTable = () => {
        return (
            <div id="wftable" className="div-level-2">
                {/*If there are no workflows and at least one URL search param is not empty*/}
                {workflows.length === 0 ? (
                    <p>No workflows found for specified filters.</p>
                ) : workflows.length !== 0 ? (
                    <WorkflowTable allData={workflows}/>
                ) : null}
            </div>
        )
    }

    return (
        <div id="div-main" className="App">
            <div id="div-header" className="div-level-2">
                <header className="App-header">
                </header>
            </div>

            <div className="div-level-2">
                <form>
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
                                    value={workflowSettings.get().date_submitted}
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
                                       defaultValue={workflowSettings.get().wf_id}/>

                        </Grid>
                        <Grid item xs={12}>
                            <Grid container spacing={2}>
                                <Grid item xs={4}/>
                                <Grid item xs={2}>
                                    <Button variant="contained" onClick={() => setRefresh(true)}>
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

                    <div className="text-center">
                        <div className="btn-toolbar d-inline-block">

                        </div>
                    </div>
                </form>
            </div>
            <ShowWFTable/>
        </div>
    );
}

export default WorkflowOverview;
