import React, {useState} from 'react';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import BootstrapTable, {ColumnDescription} from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import {FaCaretDown, FaCaretUp} from "react-icons/fa";
import {HiInformationCircle} from "react-icons/hi";
import CustomModal from '../Modal';
import {sanitize} from 'dompurify';
import {formatBytes} from "../../utils/formatters";
import humanizeDuration from 'humanize-duration';


const customCaret = (order, column) => {
    if (!order) return (<span><FaCaretUp style={{marginLeft: "5px"}}/></span>);
    else if (order === 'asc') return (<span><FaCaretUp style={{marginLeft: "5px"}}/></span>);
    else if (order === 'desc') return (<span><FaCaretDown style={{marginLeft: "5px"}}/></span>);
    return null;
}

export default function TaskInstanceTable({taskInstanceData}) {
    const [showStdoutModal, setShowStdoutModal] = useState(false)
    const [showStderrModal, setShowStderrModal] = useState(false)
    const [showResourcesModal, setShowResourcesModal] = useState(false)
    const [showTIStatusModal, setShowTIStatusModal] = useState(false)

    // ti_stderr_log is pulled from task_instance.stderr_log, ti_error_log_description is pulled from task_instance_error_log.description
    const [rowDetail, setRowDetail] = useState({
        'ti_id': '', 'ti_status': '', 'ti_stdout': '',
        'ti_stderr': '', 'ti_stdout_log': '', 'ti_stderr_log': '',
        'ti_distributor_id': '', 'ti_nodename': '', 'ti_error_log_description': '',
        'ti_wallclock': 0, 'ti_maxrss': '', 'ti_resources': ''
    });

    const htmlFormatter = cell => {
        // add sanitize to prevent xss attack
        return <div dangerouslySetInnerHTML={{__html: sanitize(`${cell}`)}}/>;
    };

    function get_data_brief(data) {
        let r: any = [];

        for (let i in data) {
            let e = data[i];
            let stderr_display = e.ti_stderr_log
            let stdout_display = e.ti_stdout_log

            // Currently not using. Leave in, in case we want to switch to showing logs in table when more users switch to 3.2.1
            if (stderr_display != null) {
                stderr_display = `
                <div class="ti-logs">${e.ti_stderr_log.trim().split("\n").slice(-1)}</div>
                `;
            }
            if (stdout_display != null) {
                stdout_display = `
                <div class="ti-logs">${e.ti_stdout_log.trim().split("\n").slice(-1)}</div>
                `;
            }

            r.push({
                "ti_id": e.ti_id,
                "ti_status": e.ti_status,
                "stderr_brief": stderr_display,
                "stdout_brief": stdout_display,
                "ti_stdout": e.ti_stdout,
                "ti_stderr": e.ti_stderr,
                "ti_distributor_id": e.ti_distributor_id,
                "ti_nodename": e.ti_nodename,
                "ti_stdout_log": e.ti_stdout_log,
                "ti_stderr_log": e.ti_stderr_log,
                "ti_error_log_description": e.ti_error_log_description,
                "ti_wallclock": e.ti_wallclock,
                "ti_maxrss": e.ti_maxrss,
                "ti_resources": e.ti_resources,
            })

        }
        return r;
    }

    const data_brief = get_data_brief(taskInstanceData)

    const columns: Array<ColumnDescription> = [
        {
            dataField: "ti_id",
            text: "ID",
            sort: true,
            sortCaret: customCaret,
            headerStyle: {width: "10%"},
            formatter: (cell) => (
                <div id={`${cell}`}>{cell}</div>
            )
        },
        {
            dataField: "ti_status",
            text: "Status",
            sort: true,
            sortCaret: customCaret,
            headerStyle: {width: "10%"},
            style: {overflowWrap: 'break-word'},
        },
        {
            dataField: "ti_stderr",
            text: "Standard Error",
            formatter: htmlFormatter,
            // @ts-ignore
            events: {
                onClick: (e: any, column: any, columnIndex: any, row: any, rowIndex: any) => {
                    setShowStderrModal(true)
                    setRowDetail(taskInstanceData[rowIndex])
                }
            },
            sort: true,
            sortCaret: customCaret,
            style: {overflowWrap: 'break-word'},
        },
        {
            dataField: "ti_stdout",
            text: "Standard Out",
            formatter: htmlFormatter,
            // @ts-ignore
            events: {
                onClick: (e: any, column: any, columnIndex: any, row: any, rowIndex: any) => {
                    setShowStdoutModal(true)
                    setRowDetail(taskInstanceData[rowIndex])
                }
            },
            sort: true,
            sortCaret: customCaret,
            style: {overflowWrap: 'break-word'},
        },
        {
            dataField: "ti_distributor_id",
            text: "Distributor ID",
            headerStyle: {width: "15%"},
            sort: true,
            sortCaret: customCaret,
        },
        {
            dataField: "ti_nodename",
            text: "Node Name",
            sort: true,
            sortCaret: customCaret,
            style: {overflowWrap: 'break-word'},
        },
        {
            dataField: "ti_resources",
            text: "Resources",
            // @ts-ignore
            events: {
                onClick: (e: any, column: any, columnIndex: any, row: any, rowIndex: any) => {
                    setShowResourcesModal(true)
                    setRowDetail(taskInstanceData[rowIndex])
                }
            },
            style: {overflowWrap: 'break-word'},
        },
    ]

    return (
        <div>
            <div style={{display: "flex"}}>
                <header className="header-1">
                    <p className='color-dark'>
                        Task Instances&nbsp;
                        <span>
                            <HiInformationCircle onClick={() => setShowTIStatusModal(true)}/>
                        </span>
                    </p>
                </header>
            </div>
            <BootstrapTable
                keyField="ti_id"
                data={data_brief}
                columns={columns}
                bootstrap4
                headerClasses="thead-dark"
                striped
                pagination={taskInstanceData.length === 0 ? undefined : paginationFactory({sizePerPage: 10})}
                selectRow={{
                    mode: "radio",
                    hideSelectColumn: true,
                    clickToSelect: true,
                    bgColor: "#848884",
                }}
            />

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
                showModal={showStdoutModal}
                setShowModal={setShowStdoutModal}
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
                showModal={showStderrModal}
                setShowModal={setShowStderrModal}

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
                showModal={showResourcesModal}
                setShowModal={setShowResourcesModal}

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
                showModal={showTIStatusModal}
                setShowModal={setShowTIStatusModal}

            />
        </div>
    );
}