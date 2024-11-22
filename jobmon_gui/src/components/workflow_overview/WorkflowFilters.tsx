import {Button, FormControl, Grid, InputLabel, MenuItem, Select, TextField} from "@mui/material";
import {DatePicker, LocalizationProvider} from "@mui/x-date-pickers";
import {AdapterDayjs} from "@mui/x-date-pickers/AdapterDayjs";
import dayjs from "dayjs";
import React, {useEffect, useState} from "react";
import {useWorkflowSearchSettings} from "@jobmon_gui/stores/workflow_settings";
import {useSearchParams} from "react-router-dom";
import {useQueryClient} from "@tanstack/react-query";
import Box from "@mui/material/Box";

export default function WorkflowFilters() {
    const queryClient = useQueryClient()
    const workflowSettings = useWorkflowSearchSettings()
    const [searchParams] = useSearchParams();

    const [localFilters, setLocalFilters] = useState({
        user: "",
        wf_args: "",
        wf_attribute_key: "",
        wf_attribute_value: "",
        tool: "",
        date_submitted: null,
        date_submitted_end: null,
        wf_name: "",
        status: "",
        wf_id: ""
    });

    useEffect(() => {
        workflowSettings.loadValuesFromSearchParams(searchParams);
        setLocalFilters({
            user: workflowSettings.get().user,
            wf_args: workflowSettings.get().wf_args,
            wf_attribute_key: workflowSettings.get().wf_attribute_key,
            wf_attribute_value: workflowSettings.get().wf_attribute_value,
            tool: workflowSettings.get().tool,
            date_submitted: workflowSettings.get().date_submitted,
            date_submitted_end: workflowSettings.get().date_submitted_end,
            wf_name: workflowSettings.get().wf_name,
            status: workflowSettings.get().status,
            wf_id: workflowSettings.get().wf_id
        });
    }, []);

    const handleClear = () => {
        workflowSettings.clear();
        setLocalFilters({
            user: "",
            wf_args: "",
            wf_attribute_key: "",
            wf_attribute_value: "",
            tool: "",
            date_submitted: null,
            date_submitted_end: null,
            wf_name: "",
            status: "",
            wf_id: ""
        });
        workflowSettings.triggerDataRefresh();
        void queryClient.invalidateQueries({ queryKey: ["workflow_overview", "workflows"] });
    };

    const handleSubmit = (event) => {
        event.preventDefault();
        workflowSettings.set({
            user: localFilters.user,
            wf_args: localFilters.wf_args,
            wf_attribute_key: localFilters.wf_attribute_key,
            wf_attribute_value: localFilters.wf_attribute_value,
            tool: localFilters.tool,
            date_submitted: localFilters.date_submitted,
            date_submitted_end: localFilters.date_submitted_end,
            wf_name: localFilters.wf_name,
            status: localFilters.status,
            wf_id: localFilters.wf_id
        });
        workflowSettings.triggerDataRefresh();
        void queryClient.invalidateQueries({ queryKey: ["workflow_overview", "workflows"] });
    };

    const handleInputChange = (key, value) => {
        setLocalFilters((prev) => ({ ...prev, [key]: value }));
    };

    return (<Box className="div-level-2">
        <form onSubmit={handleSubmit}>
            <Grid container spacing={2}>
                <Grid item xs={3}>
                    <TextField label="Username"
                               fullWidth={true}
                               value={localFilters.user}
                               onChange={(e) => handleInputChange("user", e.target.value)}/>
                </Grid>
                <Grid item xs={3}>
                    <TextField label="Workflow Args"
                               fullWidth={true}
                               value={localFilters.wf_args}
                               onChange={(e) => handleInputChange("wf_args", e.target.value)}/>
                </Grid>
                <Grid item xs={1.5}>
                    <TextField label="WF Attribute Key"
                               fullWidth={true}
                               value={localFilters.wf_attribute_key}
                               onChange={(e) => handleInputChange("wf_attribute_key", e.target.value)}/>

                </Grid>
                <Grid item xs={1.5}>
                    <TextField label="WF Attribute Value" fullWidth={true}
                               value={localFilters.wf_attribute_value}
                               onChange={(e) => handleInputChange("wf_attribute_value", e.target.value)}/>

                </Grid>
                <Grid item xs={3}>
                    <TextField label="Tool"
                               fullWidth={true}
                               value={localFilters.tool}
                               onChange={(e) => handleInputChange("tool", e.target.value)}/>

                </Grid>
                <Grid item xs={1.5}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                        <DatePicker
                            label={"Submitted Date Start"}
                            value={localFilters.date_submitted ? dayjs(localFilters.date_submitted) : dayjs("1970-01-01")}
                            onChange={(value) => handleInputChange("date_submitted", value)}
                            sx={{width: "100%"}}
                        />
                    </LocalizationProvider>

                </Grid>
                <Grid item xs={1.5}>
                    <LocalizationProvider dateAdapter={AdapterDayjs}>
                        <DatePicker
                            label={"Submitted Date End"}
                            value={localFilters.date_submitted ? dayjs(localFilters.date_submitted_end) : dayjs("1970-01-01")}
                            onChange={(value) => handleInputChange("date_submitted_end", value)}
                            sx={{width: "100%"}}
                        />
                    </LocalizationProvider>

                </Grid>
                <Grid item xs={3}>
                    <TextField label="Workflow Name"
                               fullWidth={true}
                               value={localFilters.wf_name}
                               onChange={(e) => handleInputChange("wf_name", e.target.value)}/>

                </Grid>
                <Grid item xs={3}>
                    <FormControl fullWidth={true}>
                        <InputLabel id="LABEL-workflow-status">Workflow Status</InputLabel>
                        <Select labelId="LABEL-workflow-status"

                                label="Workflow Status"
                                id={"SELECT-workflow-status"}
                                onChange={(e) => handleInputChange("status", e.target.value)}
                                value={localFilters.status}
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
                               value={localFilters.wf_id}
                               onChange={(e) => handleInputChange("wf_id", e.target.value)}/>

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
                            <Button variant="contained" onClick={handleClear}>
                                Clear All
                            </Button>
                        </Grid>
                        <Grid item xs={4}/>
                    </Grid>
                </Grid>
            </Grid>
        </form>
    </Box>)
}