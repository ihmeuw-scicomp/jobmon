import React, {useState} from 'react';
import {HiInformationCircle} from "react-icons/hi";
import {formatBytes} from "@jobmon_gui/utils/formatters";
import humanizeDuration from 'humanize-duration';
import {MaterialReactTable, useMaterialReactTable} from "material-react-table";
import {Box, Grid} from "@mui/material";
import {useTaskInstanceTableStore} from "@jobmon_gui/stores/TaskInstanceTable.ts";
import {useQuery} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import {ScrollableCodeBlock} from "@jobmon_gui/components/ScrollableTextArea.tsx";
import {getTaskInstanceDetailsQueryFn} from "@jobmon_gui/queries/GetTaskInstanceDetails.ts";
import {TaskInstance} from "@jobmon_gui/types/TaskInstance.ts";

type TaskInstanceTableProps = {
    taskId: number | string
}
import {HtmlTooltip} from "@jobmon_gui/components/HtmlToolTip";
import IconButton from "@mui/material/IconButton";


export default function TaskInstanceTable({taskId}: TaskInstanceTableProps) {
    const [modalVisibility, setModalVisibility] = useState({
        stdout: false,
        stderr: false,
        resources: false,
        status: false,
    });
    const tableStore = useTaskInstanceTableStore()
    const ti_details = useQuery({
        queryKey: ["ti_details", taskId],
        refetchInterval: 60_000,
        queryFn: getTaskInstanceDetailsQueryFn
    })

    const showModal = (modalName) => setModalVisibility({...modalVisibility, [modalName]: true});
    const hideModal = (modalName) => setModalVisibility({...modalVisibility, [modalName]: false});

    // ti_stderr_log is pulled from task_instance.stderr_log, ti_error_log_description is pulled from task_instance_error_log.description
    const [rowDetail, setRowDetail] = useState<TaskInstance>({
        'ti_id': '', 'ti_status': '', 'ti_stdout': '',
        'ti_stderr': '', 'ti_stdout_log': '', 'ti_stderr_log': '',
        'ti_distributor_id': '', 'ti_nodename': '', 'ti_error_log_description': '',
        'ti_wallclock': 0, 'ti_maxrss': '', 'ti_resources': ''
    });

    const handleCellClick = (rowIndex, modalName) => {
        setRowDetail(ti_details.data[rowIndex]);
        showModal(modalName);
    };


    const columns = [
        {
            accessorKey: "ti_id",
            header: "ID",
        },
        {
            accessorKey: "ti_status",
            header: "Status",
        },
        {
            accessorKey: "ti_stderr",
            header: "Standard Error",
            Cell: ({cell, row}) => (
                <Box onClick={() => handleCellClick(row.index, "stderr")} sx={{cursor: 'pointer'}}>
                    {cell.getValue()?.length > 30 ? "..." + cell.getValue().trim().slice(-30) : cell.getValue()}
                    <OpenInNewIcon/>
                </Box>
            ),
        },
        {
            accessorKey: "ti_stdout",
            header: "Standard Out",
            Cell: ({cell, row}) => (
                <Box onClick={() => handleCellClick(row.index, "stdout")} sx={{cursor: 'pointer'}}>
                    {cell.getValue()?.length > 30 ? "..." + cell.getValue().trim().slice(-30) : cell.getValue()}
                    <OpenInNewIcon/>
                </Box>
            )
        },
        {
            accessorKey: "ti_distributor_id",
            header: "Distributor ID",
        },
        {
            accessorKey: "ti_nodename",
            header: "Node Name",
        },
        {
            accessorKey: "ti_resources",
            header: "Resources",
            Cell: ({cell, row}) => (
                <Box onClick={() => handleCellClick(row.index, "resources")} sx={{cursor: 'pointer'}}>
                    {cell.getValue()} <OpenInNewIcon/>
                </Box>
            )
        },
    ]

    const table = useMaterialReactTable({
        data: ti_details.data || [],
        columns: columns,
        state: {
            isLoading: ti_details.isLoading,
            pagination: tableStore.getPagination(),
            columnFilters: tableStore.getFilters(),
            sorting: tableStore.getSorting(),
            columnOrder: tableStore.getColumnOrder(),
            density: tableStore.getDensity(),
            columnVisibility: tableStore.getColumnVisibility(),
            showColumnFilters: tableStore.getFilterVisibility(),
        },
        onPaginationChange: (s) => {
            tableStore.setPagination(s)
        },
        onColumnFiltersChange: (s) => {
            tableStore.setFilters(s)
        },
        onSortingChange: (s) => {
            tableStore.setSorting(s)
        },
        onColumnOrderChange: (s) => {
            tableStore.setColumnOrder(s)
        },
        onDensityChange: (s) => {
            tableStore.setDensity(s)
        },
        onColumnVisibilityChange: (s) => {
            tableStore.setColumnVisibility(s)
        },
        onShowColumnFiltersChange: (s) => {
            tableStore.setFilterVisibility(s)
        },
        enableColumnResizing: true,
        layoutMode: "grid-no-grow",
    });

    if (ti_details.isError) {
        console.log(ti_details.error)
        return <Typography>Error loading task instance table. Please reload and try again</Typography>;
    }

    return (
        <div>
            <div style={{display: "flex"}}>
                <header className="header-1">
                    <p className='color-dark'>
                        Task Instances&nbsp;
                        <span>
                            <HtmlTooltip
                                title="Task Instance Statuses"
                                arrow={true}
                                placement={"right"} 
                            >
                                <IconButton
                                    color="inherit"
                                    sx={{
                                        padding: 0,
                                        fontSize: 'inherit',
                                    }}
                                >
                                    <HiInformationCircle
                                        style={{cursor: 'pointer'}}
                                        onClick={() => showModal("status")}
                                    />
                                </IconButton>
                            </HtmlTooltip>
                        </span>
                    </p>
                </header>
            </div>

            <Box p={2} display="flex" justifyContent="center" width="100%">
                <MaterialReactTable table={table}/>
            </Box>

            <JobmonModal
                title={
                    "Standard Out"
                }
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={12}><Typography variant={"h6"}>Standard Out Path:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>{rowDetail.ti_stdout}</ScrollableCodeBlock> <br></br>
                        </Grid>
                        <Grid item xs={12}><Typography variant={"h6"}>Standard Out Log:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>{rowDetail.ti_stdout_log}</ScrollableCodeBlock>
                        </Grid>
                    </Grid>
                }
                open={modalVisibility.stdout}
                onClose={() => hideModal('stdout')}
                width={"80%"}
            />

            <JobmonModal
                title={"Standard Error"}
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={12}><Typography variant={"h6"}>Standard Error Path:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>
                                {rowDetail.ti_stderr}
                            </ScrollableCodeBlock>
                        </Grid>
                        <Grid item xs={12}><Typography variant={"h6"}>Standard Error Log:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>
                                {rowDetail.ti_stderr_log}
                            </ScrollableCodeBlock>
                        </Grid>
                        <Grid item xs={12}><Typography variant={"h6"}>Standard Error Description:</Typography></Grid>
                        <Grid item xs={12}>
                            <ScrollableCodeBlock>
                                {rowDetail.ti_error_log_description}
                            </ScrollableCodeBlock>
                        </Grid>
                    </Grid>
                }
                open={modalVisibility.stderr}
                onClose={() => hideModal('stderr')}
                width={"80%"}

            />
            <JobmonModal
                title={
                    "Resources"
                }
                children={
                    <p>
                        <b>Requested Resources:</b> <br></br>
                        {rowDetail.ti_resources && (
                            <>
                                <ul style={{listStyleType: 'none', padding: 0}}>
                                    {Object.keys(JSON.parse(rowDetail.ti_resources)).map(key => {
                                        let value = JSON.parse(rowDetail.ti_resources)[key];
                                        if (key === "memory") {
                                            value += " GiB";
                                        }
                                        if (key === "runtime") {
                                            value = humanizeDuration(value * 1000)
                                        }
                                        return (
                                            <li key={key}>
                                                <i>{key}</i>: {value}
                                            </li>
                                        );
                                    })}
                                </ul>
                                <br/>
                            </>
                        )}
                        <br></br>
                        <b>Utilized Resources:</b> <br></br>
                        <i>memory</i>: {formatBytes(rowDetail.ti_maxrss)}<br></br>
                        <i>runtime</i>: {rowDetail.ti_wallclock ? humanizeDuration(parseInt(rowDetail.ti_wallclock.toString()) * 1000) : ""}
                    </p>
                }
                open={modalVisibility.resources}
                onClose={() => hideModal("resources")}
                width={"80%"}
            />

            <JobmonModal
                title={"Task Instance Statuses"}
                children={
                    <Grid container spacing={2}>
                        <Grid item xs={3}><b>Submitted to Batch Distributor:</b></Grid>
                        <Grid item xs={9}> TaskInstance registered in the Jobmon database.</Grid>
                        <Grid item xs={3}><b>Done:</b></Grid>
                        <Grid item xs={9}> TaskInstance finished successfully.</Grid>
                        <Grid item xs={3}><b>Error:</b></Grid>
                        <Grid item xs={9}> TaskInstance stopped with an application error (non-zero return code).</Grid>
                        <Grid item xs={3}><b>Error Fatal:</b></Grid>
                        <Grid item xs={9}> TaskInstance killed itself as part of a cold workflow resume, and cannot be
                            retried.</Grid>
                        <Grid item xs={3}><b>Instantiated:</b></Grid>
                        <Grid item xs={9}> TaskInstance is created within Jobmon, but not queued for submission to the
                            cluster.</Grid>
                        <Grid item xs={3}><b>Kill Self:</b></Grid>
                        <Grid item xs={9}> TaskInstance has been ordered to kill itself if it is still alive, as part of
                            a cold workflow resume.</Grid>
                        <Grid item xs={3}><b>Launched:</b></Grid>
                        <Grid item xs={9}> TaskInstance submitted to the cluster normally, part of a Job Array.</Grid>
                        <Grid item xs={3}><b>Queued:</b></Grid>
                        <Grid item xs={9}> TaskInstance is queued for submission to the cluster.</Grid>
                        <Grid item xs={3}><b>Running:</b></Grid>
                        <Grid item xs={9}> TaskInstance has started running normally.</Grid>
                        <Grid item xs={3}><b>Triaging:</b></Grid>
                        <Grid item xs={9}> TaskInstance has errored, Jobmon is determining the category of error.</Grid>
                        <Grid item xs={3}><b>Unknown Error:</b></Grid>
                        <Grid item xs={9}> TaskInstance stopped reporting that it was alive for an unknown
                            reason.</Grid>
                        <Grid item xs={3}><b>No Distributor ID:</b></Grid>
                        <Grid item xs={9}> TaskInstance submission within Jobmon failed – did not receive a job
                            number from the cluster.</Grid>
                        <Grid item xs={3}><b>Resource Error:</b></Grid>
                        <Grid item xs={9}> TaskInstance died because of insufficient resource request, i.e.
                            insufficient memory or runtime.</Grid>
                    </Grid>
                }
                open={modalVisibility.status}
                onClose={() => hideModal("status")}
                width={"80%"}
            />
        </div>
    );
}