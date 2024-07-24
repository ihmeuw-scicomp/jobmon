import React, {useEffect, useState} from 'react';
import Box from "@mui/material/Box";
import axios from "axios";
import {useQuery, useQueryClient} from "@tanstack/react-query";
import {error_log_viz_url, task_table_url} from "@jobmon_gui/configs/ApiUrls";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios";
import Typography from "@mui/material/Typography";
import {CircularProgress, Fade, Grid} from "@mui/material";
import {MaterialReactTable, useMaterialReactTable} from "material-react-table";
import {Button} from '@mui/material';
import {JobmonModal} from "@jobmon_gui/components/JobmonModal";
import {ScrollableCodeBlock} from "@jobmon_gui/components/ScrollableTextArea";
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import NavigateBeforeIcon from '@mui/icons-material/NavigateBefore';
import IconButton from "@mui/material/IconButton";
import {useTaskTableColumnsStore} from "@jobmon_gui/stores/task_table";
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import HtmlTooltip from "@jobmon_gui/components/HtmlToolTip";

type ClusteredErrorsProps = {
    taskTemplateId: string | number
    workflowId: number | string
}

type ErrorSampleModalDetails = {
    sample_index: number
    sample_ids: number[]
}

interface ErrorLog {
    task_id: number
    workflow_id: number
    workflow_run_id: number
    task_instance_err_id: number
    task_instance_stderr_log: string
    error_time: string
    error: string
}

interface ErrorDetails {
    data?: {
        error_logs?: ErrorLog[];
    };
}

export default function ClusteredErrors({taskTemplateId, workflowId}: ClusteredErrorsProps) {
    const queryClient = useQueryClient()
    const taskTableColumnFilters = useTaskTableColumnsStore()
    const [errorDetailIndex, setErrorDetailIndex] = useState<boolean | ErrorSampleModalDetails>(false)
    const errors = useQuery({
        queryKey: ["workflow_details", "clustered_errors", workflowId, taskTemplateId],
        queryFn: async () => {
            return axios.get(
                `${error_log_viz_url}${workflowId}/${taskTemplateId}#`,
                {
                    ...jobmonAxiosConfig,
                    data: null,
                    params: {cluster_errors: "true"}
                }
            ).then((r) => {
                return r.data
            })
        },
        enabled: !!taskTemplateId
    })
    const prefetchErrorDetails = async (nextErrorDetailIndex: boolean | ErrorSampleModalDetails) => {
        /*
        Pre-fetch the next error detail to provide a better ux to the user, cache the results in the
        react-query cache
        */
        await queryClient.prefetchQuery({
            queryKey: ["workflow_details", "error_details", workflowId, taskTemplateId, nextErrorDetailIndex],
            queryFn: async () => {
                if (nextErrorDetailIndex === false || nextErrorDetailIndex === true) {
                    return;
                }
                const ti_id = nextErrorDetailIndex.sample_ids[nextErrorDetailIndex.sample_index]
                return axios.get(
                    `${error_log_viz_url}${workflowId}/${taskTemplateId}/${ti_id}`,
                    {
                        ...jobmonAxiosConfig,
                        data: null,
                    }
                ).then((r) => {
                    return r.data
                })
            },
        })

    }

    const errorDetails = useQuery({
        queryKey: ["workflow_details", "error_details", workflowId, taskTemplateId, errorDetailIndex],
        queryFn: async () => {
            if (errorDetailIndex === false || errorDetailIndex === true) {
                return;
            }
            const ti_id = errorDetailIndex.sample_ids[errorDetailIndex.sample_index]
            return axios.get(
                `${error_log_viz_url}${workflowId}/${taskTemplateId}/${ti_id}`,
                {
                    ...jobmonAxiosConfig,
                    data: null,
                }
            ).then((r) => {

                return r.data
            })
        },
        enabled: !!taskTemplateId && errorDetailIndex !== false && errorDetailIndex !== true
    })

    useEffect(() => {
        if (errorDetailIndex === false || errorDetailIndex === true) {
            return;
        }
        if (errorDetailIndex.sample_index < errorDetailIndex.sample_ids.length - 1) {
            const nextErrorDetails = {...errorDetailIndex, sample_index: errorDetailIndex.sample_index + 1}
            void prefetchErrorDetails(nextErrorDetails)
        }
    }, [errorDetailIndex]);

    const columns = [
        {
            header: "Sample Error",
            accessorKey: "sample_error",
            size: 750,
            Cell: ({renderedCellValue, row}) => (
                <Typography>
                    <Button sx={{textTransform: 'none', textAlign: "left"}}
                            onClick={() => setErrorDetailIndex({
                                sample_index: 0,
                                sample_ids: row.original.task_instance_ids
                            })}>
                        {renderedCellValue}
                    </Button>
                </Typography>
            ),
        },
        {
            header: "First Seen",
            accessorKey: "first_error_time",
        },
        {
            header: "Occurrences",
            accessorKey: "group_instance_count",
            desc: true,
        },
        {
            header: "Actions",
            accessorKey: "actions",
            Cell: ({row}) => (
                <HtmlTooltip title={"Filter tasks table for tasks with this error"}
                             arrow={true}
                             placement={"right"}>
                    <IconButton sx={{textTransform: 'none', textAlign: "left"}}
                                onClick={() => {
                                    taskTableColumnFilters.set([{
                                        id: "task_id",
                                        value: row.original.task_ids.join(",")
                                    }])
                                }}>
                        <FilterAltIcon/>
                    </IconButton>
                </HtmlTooltip>
            ),
        },
    ];

    const table = useMaterialReactTable({
        data: (errors as ErrorDetails)?.data?.error_logs || [],
        columns: columns,
        initialState: {density: 'comfortable'},
        enableColumnResizing: true,
        layoutMode: "grid-no-grow",
    });


    if (!taskTemplateId) {
        return (<Typography sx={{pt: 5}}>Select a task template from above to clustered errors</Typography>)
    }
    if (errors.isLoading) {
        return (<CircularProgress/>)
    }
    if (errors.isError) {
        return (<Typography>Unable to retrieve clustered errors. Please refresh and try again</Typography>)
    }


    const nextSample = () => {
        if (errorDetailIndex === false || errorDetailIndex === true) {
            return;
        }
        setErrorDetailIndex({
            ...errorDetailIndex,
            sample_index: errorDetailIndex.sample_index + 1
        })
    }
    const previousSample = () => {
        if (errorDetailIndex === false || errorDetailIndex === true || errorDetailIndex.sample_index == 0) {
            return;
        }
        setErrorDetailIndex({
            ...errorDetailIndex,
            sample_index: errorDetailIndex.sample_index - 1
        })
    }
    const modalChildren = () => {
        if (errorDetails.isLoading) {
            return (<CircularProgress/>)
        }
        const error = (errorDetails as ErrorDetails)?.data?.error_logs?.[0] || false;
        if (errorDetails.isError || !error) {
            return (<Typography>Failed to retrieve error details. Please refresh and try again</Typography>)
        }

        const labelStyles = {
            fontWeight: "bold",
        }

        return (<Box minHeight={"80%"}>
            <Fade in={true}>
                <Box>
                    <Box>

                        <Typography sx={labelStyles}>Error Sample:
                            <IconButton
                                onClick={previousSample}
                                disabled={typeof errorDetailIndex !== 'boolean' && errorDetailIndex?.sample_index === 0}
                            >
                                <NavigateBeforeIcon/>
                            </IconButton>

                            {
                                errorDetailIndex && typeof errorDetailIndex !== 'boolean' ? (
                                    `${errorDetailIndex.sample_index + 1} of ${errorDetailIndex.sample_ids?.length}`
                                ) : (
                                    'No error logs available'
                                )
                            }
                            <IconButton
                                onClick={nextSample}
                                disabled={
                                    typeof errorDetailIndex !== 'boolean' &&
                                    errorDetailIndex?.sample_index === errorDetailIndex?.sample_ids?.length - 1
                                }
                            >
                                <NavigateNextIcon/>
                            </IconButton>
                        </Typography>
                    </Box>
                    <Grid container spacing={2}>
                        <Grid item xs={3}><Typography sx={labelStyles}>Error Time:</Typography></Grid>
                        <Grid item xs={9}>{error.error_time}</Grid>

                        <Grid item xs={3}><Typography sx={labelStyles}>task_id:</Typography></Grid>
                        <Grid item xs={9}>{error.task_id}</Grid>

                        <Grid item xs={3}><Typography sx={labelStyles}>Task Instance Error ID:</Typography></Grid>
                        <Grid item xs={9}>{error.task_instance_err_id}</Grid>

                        <Grid item xs={3}><Typography sx={labelStyles}>workflow_id:</Typography></Grid>
                        <Grid item xs={9}>{error.workflow_id}</Grid>

                        <Grid item xs={3}><Typography sx={labelStyles}>workflow_run_id:</Typography></Grid>
                        <Grid item xs={9}>{error.workflow_run_id}</Grid>

                        <Grid item xs={12}><Typography sx={labelStyles}>Error Message:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>
                                {error.error}
                            </ScrollableCodeBlock>
                        </Grid>

                        <Grid item xs={12}><Typography sx={labelStyles}>Task Instance stderr:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>
                                {error.task_instance_stderr_log || "No stderr output found"}
                            </ScrollableCodeBlock>
                        </Grid>
                    </Grid>
                </Box>
            </Fade>
        </Box>)
    }

    const currentTiID = () => {
        if (errorDetailIndex === false || errorDetailIndex === true) {
            return ""
        }
        return errorDetailIndex?.sample_ids[errorDetailIndex?.sample_index]
    }


    return (
        <Box p={2} display="flex" justifyContent="center" width="100%">
            <MaterialReactTable table={table}/>
            <JobmonModal
                title={`Error Sample for Task Instance ID: ${currentTiID()}`}
                open={errorDetailIndex !== false}
                onClose={() => setErrorDetailIndex(false)} children={modalChildren()}
                width={"80%"} minHeight={"80%"}
            />
        </Box>
    )
}