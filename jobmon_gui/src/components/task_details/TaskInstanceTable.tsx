import React, {useState} from 'react';
import {HiInformationCircle} from "react-icons/hi";
import CustomModal from '@jobmon_gui/components/Modal';
import {formatBytes} from "@jobmon_gui/utils/formatters";
import humanizeDuration from 'humanize-duration';
import {MaterialReactTable} from "material-react-table";
import {Box} from "@mui/material";


export default function TaskInstanceTable({taskInstanceData}) {
    const [modalVisibility, setModalVisibility] = useState({
        stdout: false,
        stderr: false,
        resources: false,
        status: false,
    });

    const showModal = (modalName) => setModalVisibility({...modalVisibility, [modalName]: true});
    const hideModal = (modalName) => setModalVisibility({...modalVisibility, [modalName]: false});

    // ti_stderr_log is pulled from task_instance.stderr_log, ti_error_log_description is pulled from task_instance_error_log.description
    const [rowDetail, setRowDetail] = useState({
        'ti_id': '', 'ti_status': '', 'ti_stdout': '',
        'ti_stderr': '', 'ti_stdout_log': '', 'ti_stderr_log': '',
        'ti_distributor_id': '', 'ti_nodename': '', 'ti_error_log_description': '',
        'ti_wallclock': 0, 'ti_maxrss': '', 'ti_resources': ''
    });

    function get_data_brief(data) {
        return data.map(data => ({
            "ti_id": data.ti_id,
            "ti_status": data.ti_status,
            "ti_stdout": data.ti_stdout,
            "ti_stderr": data.ti_stderr,
            "ti_distributor_id": data.ti_distributor_id,
            "ti_nodename": data.ti_nodename,
            "ti_stdout_log": data.ti_stdout_log,
            "ti_stderr_log": data.ti_stderr_log,
            "ti_error_log_description": data.ti_error_log_description,
            "ti_wallclock": data.ti_wallclock,
            "ti_maxrss": data.ti_maxrss,
            "ti_resources": data.ti_resources,
        }));
    }

    const handleCellClick = (rowIndex, modalName) => {
        setRowDetail(taskInstanceData[rowIndex]);
        showModal(modalName);
    };

    const data_brief = get_data_brief(taskInstanceData)

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
                <div onClick={() => handleCellClick(row.index, "stderr")}>
                    {cell.getValue()?.length > 30 ? "..." + cell.getValue().slice(-30) : cell.getValue()}
                </div>
            ),
        },
        {
            accessorKey: "ti_stdout",
            header: "Standard Out",
            Cell: ({cell, row}) => (
                <div onClick={() => handleCellClick(row.index, "stdout")}>
                    {cell.getValue()?.length > 30 ? "..." + cell.getValue().slice(-30) : cell.getValue()}
                </div>
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
                <div onClick={() => handleCellClick(row.index, "resources")}>
                    {cell.getValue()}
                </div>
            )
        },
    ]

    return (
        <div>
            <div style={{display: "flex"}}>
                <header className="header-1">
                    <p className='color-dark'>
                        Task Instances&nbsp;
                        <span>
                            <HiInformationCircle onClick={() => showModal("status")}/>
                        </span>
                    </p>
                </header>
            </div>

            <Box p={2} display="flex" justifyContent="center" width="100%">
                <MaterialReactTable columns={columns} data={data_brief}/>
            </Box>

            <CustomModal
                className="ti_stdout_modal"
                headerContent={
                    <h5> Standard Out</h5>
                }
                bodyContent={
                    <p>
                        <b>Standard Out Path:</b> <br></br>
                        {rowDetail.ti_stdout} <br></br>
                        <br></br>
                        <b>Standard Out Log:</b> <br></br>
                        {rowDetail.ti_stdout_log}
                    </p>
                }
                showModal={modalVisibility.stdout}
                setShowModal={() => hideModal('stdout')}
            />

            <CustomModal
                className="ti_error_modal"
                headerContent={
                    <h5> Standard Error</h5>
                }
                bodyContent={
                    <p>
                        <b>Standard Error Path:</b> <br></br>
                        {rowDetail.ti_stderr}<br></br>
                        <br></br>
                        <b>Standard Error Log:</b> <br></br>
                        {rowDetail.ti_stderr_log}<br></br>
                        <br></br>
                        {rowDetail.ti_error_log_description}
                    </p>
                }
                showModal={modalVisibility.stderr}
                setShowModal={() => hideModal('stderr')}

            />
            <CustomModal
                className="ti_resources_modal"
                headerContent={
                    <h5>Resources</h5>
                }
                bodyContent={
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
                        <i>runtime</i>: {humanizeDuration(rowDetail.ti_wallclock * 1000)}
                    </p>
                }
                showModal={modalVisibility.resources}
                setShowModal={() => hideModal("resources")}
            />

            <CustomModal
                className="task_instance_status_modal"
                headerContent={
                    <h5> Task Instance Statuses</h5>
                }
                bodyContent={
                    <p>
                        <b>Submitted to Batch Distributor:</b> TaskInstance registered in the Jobmon database.<br/>
                        <b>Done:</b> TaskInstance finished successfully.<br/>
                        <b>Error:</b> TaskInstance stopped with an application error (non-zero return code).<br/>
                        <b>Error Fatal:</b> TaskInstance killed itself as part of a cold workflow resume, and cannot be
                        retried.<br/>
                        <b>Instantiated:</b> TaskInstance is created within Jobmon, but not queued for submission to the
                        cluster.<br/>
                        <b>Kill Self:</b> TaskInstance has been ordered to kill itself if it is still alive, as part of
                        a cold workflow resume.<br/>
                        <b>Launched:</b> TaskInstance submitted to the cluster normally, part of a Job Array.<br/>
                        <b>Queued:</b> TaskInstance is queued for submission to the cluster.<br/>
                        <b>Running:</b> TaskInstance has started running normally.<br/>
                        <b>Triaging:</b> TaskInstance has errored, Jobmon is determining the category of error.<br/>
                        <b>Unknown Error:</b> TaskInstance stopped reporting that it was alive for an unknown
                        reason.<br/>
                        <b>No Distributor ID:</b> TaskInstance submission within Jobmon failed – did not receive a job
                        number from the cluster.<br/>
                        <b>Resource Error:</b> TaskInstance died because of insufficient resource request, i.e.
                        insufficient memory or runtime.<br/>
                    </p>
                }
                showModal={modalVisibility.status}
                setShowModal={() => hideModal("status")}
            />
        </div>
    );
}