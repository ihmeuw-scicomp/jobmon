import "@jobmon_gui/styles/jobmon_gui.css";
import React, {useContext, useState} from "react";
import {useMutation, useQuery, useQueryClient} from "@tanstack/react-query";
import IconButton from "@mui/material/IconButton";
import {Check, Error} from "@mui/icons-material";
import AuthContext from "@jobmon_gui/contexts/AuthContext.tsx";
import {getWorkflowDetailsQueryFn} from "@jobmon_gui/queries/GetWorkflowDetails.ts";
import Typography from "@mui/material/Typography";
import {Alert, Box, Button, Grid, Paper} from "@mui/material";
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import axios from "axios";
import {workflow_set_resume_url} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";

type StopWorkflowButtonProps = {
    wf_id: number | string
}

export default function StopWorkflowButton({
                                               wf_id,
                                               disabled,
                                           }: StopWorkflowButtonProps) {
    const {user} = useContext(AuthContext)
    const queryClient = useQueryClient();
    const user_name = user?.preferred_username ? user?.preferred_username.split("@")[0] : "unknown"
    const [confirmModalOpen, setConfirmModalOpen] = useState(false);
    const wfDetails = useQuery({
        queryKey: ["workflow_details", "details", wf_id],
        queryFn: getWorkflowDetailsQueryFn,
        staleTime: 60000, // 60 seconds
    })

    const stoppedStates = ['']

    const stopWorkflow = useMutation({
        mutationKey: ['stopWorkflow', wf_id],
        mutationFn: async (wf_id: number | string) => {
            return axios.post(workflow_set_resume_url(wf_id), {reset_running_jobs: true}, jobmonAxiosConfig)
        },
        onSettled: () => {
            void queryClient.invalidateQueries({queryKey: ["workflow_details", "details", wf_id]})
        }
    })

    if (wfDetails.isLoading) {
        return <></>
    }
    if (wfDetails.isError) {
        return <></>
    }

    const ModalChildren = () => {

        if (stopWorkflow.isError) {
            return (<Grid container spacing={2}>
                    <Grid item xs={12}>
                        <Alert icon={<Error fontSize="inherit"/>} severity="error" variant="outlined">
                            <Typography variant={"h6"}>Error stopping workflow. Please refresh the page and try
                                again.</Typography>
                        </Alert>
                    </Grid>
                    <Grid item xs={12}></Grid>
                    <Grid item xs={6}>

                    </Grid>
                    <Grid item xs={6}>
                        <Box display="flex" justifyContent="flex-end">
                            <Button onClick={() => {
                                stopWorkflow.reset()
                                setConfirmModalOpen(false)
                            }} variant={"contained"}
                                    size={"large"}>Close</Button>
                        </Box>
                    </Grid>
                </Grid>
            )
        }

        if (stopWorkflow.isSuccess) {
            return (<Grid container spacing={2}>
                    <Grid item xs={12}>
                        <Alert icon={<Check fontSize="inherit"/>} severity="success" variant="outlined">
                            <Typography variant={"h6"}>Workflow stop request sent successfully.</Typography>
                        </Alert>
                    </Grid>
                    <Grid item xs={12}></Grid>
                    <Grid item xs={6}>

                    </Grid>
                    <Grid item xs={6}>
                        <Box display="flex" justifyContent="flex-end">
                            <Button onClick={() => {
                                stopWorkflow.reset()
                                setConfirmModalOpen(false)
                            }} variant={"contained"}
                                    size={"large"}>Close</Button>
                        </Box>
                    </Grid>
                </Grid>
            )
        }
        return (<Grid container spacing={2}>
            <Grid item xs={12}>
                <Typography variant={"h6"}>
                    Do you want to stop workflow {wf_id}?
                    This action cannot be undone.
                </Typography>
            </Grid>
            <Grid item xs={12}></Grid>
            <Grid item xs={6}>
                <Button variant={"contained"} color={"error"} onClick={() => stopWorkflow.mutate(wf_id)}>
                    Stop Workflow {wf_id}
                </Button>
            </Grid>
            <Grid item xs={6}>
                <Box display="flex" justifyContent="flex-end">
                    <Button onClick={() => setConfirmModalOpen(false)} variant={"contained"}
                            size={"large"}>Do not stop workflow</Button>
                </Box>
            </Grid>
        </Grid>)
    }

    const ConfirmationModal = () => {
        return <JobmonModal open={confirmModalOpen}
                            width={"600px"}
                            minHeight={"300px"}
                            title={`Stop Workflow ${wf_id}?`}
                            onClose={() => setConfirmModalOpen(false)}
                            children={<ModalChildren/>}/>

    }

    return (<>
            <IconButton color="inherit"
                        disabled={user_name != wfDetails.data.wfr_user || stoppedStates.includes(wfDetails.data.wf_status) || disabled}
                        onClick={() => {
                            setConfirmModalOpen(true)
                        }}>
                <Button variant="contained" disabled={disabled}
                        sx={{bgcolor: 'red', borderRadius: '0', '&:hover': {bgcolor: 'darkred'}}}>
                    Stop Workflow
                </Button>
            </IconButton>
            <ConfirmationModal/>
        </>

    )

}