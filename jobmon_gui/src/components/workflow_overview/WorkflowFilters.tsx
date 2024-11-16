import {Button, FormControl, Grid, InputLabel, MenuItem, Select, TextField} from "@mui/material";
import {DatePicker, LocalizationProvider} from "@mui/x-date-pickers";
import {AdapterDayjs} from "@mui/x-date-pickers/AdapterDayjs";
import dayjs from "dayjs";
import React, {useEffect} from "react";
import {useWorkflowSearchSettings} from "@jobmon_gui/stores/workflow_settings";
import {useSearchParams} from "react-router-dom";
import {useQueryClient} from "@tanstack/react-query";
import Box from "@mui/material/Box";

export default function WorkflowFilters() {
    const queryClient = useQueryClient()
    const workflowSettings = useWorkflowSearchSettings()
    const [searchParams, setSearchParams] = useSearchParams();


    useEffect(() => workflowSettings.loadValuesFromSearchParams(searchParams), [])

    const handleClear = () => {
        void queryClient.invalidateQueries({queryKey: ["workflow_overview", "workflows"]})
        workflowSettings.clear()
        workflowSettings.triggerDataRefresh()
    }

    const handleSubmit = (event) => {
        void queryClient.invalidateQueries({queryKey: ["workflow_overview", "workflows"]})
        workflowSettings.triggerDataRefresh()
        event.preventDefault();
    }

    return (<Box className="div-level-2">
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
                    <TextField label="WF Attribute Key"
                               fullWidth={true}
                               value={workflowSettings.get().wf_attribute_key}
                               onChange={(e) => workflowSettings.setWfAttributeKey(e.target.value)}/>

                </Grid>
                <Grid item xs={1.5}>
                    <TextField label="WF Attribute Value" fullWidth={true}
                               value={workflowSettings.get().wf_attribute_value}
                               onChange={(e) => workflowSettings.setWfAttributeValue(e.target.value)}/>

                </Grid>
                <Grid item xs={3}>
                    <TextField label="Tool"
                               fullWidth={true}
                               value={workflowSettings.get().tool}
                               onChange={(e) => workflowSettings.setTool(e.target.value)}/>

                </Grid>
                <Grid item xs={1.5}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                        <DatePicker
                            label={"Submitted Date Start"}
                            value={dayjs(workflowSettings.get().date_submitted) || dayjs("1970-01-01")}
                            onChange={(value) => workflowSettings.setDateSubmitted(value)}
                            sx={{width: "100%"}}

                        />
                    </LocalizationProvider>

                </Grid>
                <Grid item xs={1.5}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                        <DatePicker
                            label={"Submitted Date End"}
                            value={dayjs(workflowSettings.get().date_submitted_end) || dayjs("1970-01-01")}
                            onChange={(value) => workflowSettings.setDateSubmittedEnd(value)}
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
    </Box>)
}