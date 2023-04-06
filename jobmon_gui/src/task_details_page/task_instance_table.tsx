import React, { useState } from 'react';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import BootstrapTable, { ColumnDescription }  from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faCaretDown, faCaretUp, faLightbulb } from '@fortawesome/free-solid-svg-icons';
import CustomModal from '../Modal';
import { sanitize } from 'dompurify';

const customCaret = (order, column) => {
    if (!order) return (<span>&nbsp;&nbsp;<FontAwesomeIcon icon={faCaretUp} /></span>);
    else if (order === 'asc') return (<span>&nbsp;&nbsp;<FontAwesomeIcon icon={faCaretUp} /></span>);
    else if (order === 'desc') return (<span>&nbsp;&nbsp;<FontAwesomeIcon icon={faCaretDown} /></span>);
    return null;
}

export default function TaskInstanceTable({ taskInstanceData }) {
    const [showStdoutModal, setShowStdoutModal] = useState(false)
    const [showStderrModal, setShowStderrModal] = useState(false)

    const [rowDetail, setRowDetail] = useState({
        'ti_id': '', 'ti_status': '', 'ti_stdout': '',
        'ti_stderr': '', 'ti_stdout_log': '', 'ti_stderr_log': '',
        'ti_distributor_id': '', 'ti_nodename': '',
    });

    const htmlFormatter = cell => {
        // add sanitize to prevent xss attack
        return <div dangerouslySetInnerHTML={{ __html: sanitize(`${cell}`) }} />;
    };

    function get_data_brief(data) {
        let r: any = [];

        for (let i in data) {
            let e = data[i];
            let stderr_display = e.ti_stderr_log
            let stdout_display = e.ti_stdout_log

            // Currently not using. Leave in, in case we want to switch to showing logs in table when more users switch to 3.2.1
            if (stderr_display  !== null && stderr_display  !== undefined) {
                stderr_display = `
                <div class="ti-logs">${e.ti_stderr_log.trim().split("\n").slice(-1)}</div>
                `;
            }
            if (stdout_display !== null && stdout_display !== undefined) {
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
                "ti_stderr_log": e.ti_stderr_log
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
            headerStyle: { width: "10%" },
            formatter: (cell) => (
                <div id={`${cell}`}>{cell}</div>
            )
        },
        {
            dataField: "ti_status",
            text: "Status",
            sort: true,
            sortCaret: customCaret,
            headerStyle: { width: "10%" },
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
            style: { overflowWrap: 'break-word' },
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
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_distributor_id",
            text: "Distributor ID",
            headerStyle: { width: "15%" },
            sort: true,
            sortCaret: customCaret,
        },
        {
            dataField: "ti_nodename",
            text: "Node Name",
            sort: true,
            sortCaret: customCaret,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_error_log_description",
            text: "Error Log",
            sort: true,
            sortCaret: customCaret,
            style: { overflowWrap: 'break-word' },
            headerStyle: { width: "30%" },

        }
    ]

    // Create and return the React Bootstrap Table
    return (
        <div>
            <div style={{ display: "flex" }}>
                <header className="header-1">
                    <p className='color-dark'>
                        Task Instances&nbsp;
                        <OverlayTrigger
                            placement="right"
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_instance_explanation" className='ti-popover-body'>
                                    <p><b>Submitted to Batch Distributor:</b> TaskInstance registered in the Jobmon database.</p>
                                    <p><b>Done:</b> TaskInstance finished successfully.</p>
                                    <p><b>Error:</b> TaskInstance stopped with an application error (non-zero return code).</p>
                                    <p><b>Error Fatal:</b> TaskInstance killed itself as part of a cold workflow resume, and cannot be retried.</p>
                                    <p><b>Instantiated:</b> TaskInstance is created within Jobmon, but not queued for submission to the cluster.</p>
                                    <p><b>Kill Self:</b> TaskInstance has been ordered to kill itself if it is still alive, as part of a cold workflow resume.</p>
                                    <p><b>Launched:</b> TaskInstance submitted to the cluster normally, part of a Job Array.</p>
                                    <p><b>Queued:</b> TaskInstance is queued for submission to the cluster.</p>
                                    <p><b>Running:</b> TaskInstance has started running normally.</p>
                                    <p><b>Triaging:</b> TaskInstance has errored, Jobmon is determining the category of error.</p>
                                    <p><b>Unknown Error:</b> TaskInstance stopped reporting that it was alive for an unknown reason.</p>
                                    <p><b>No Distributor ID:</b> TaskInstance submission within Jobmon failed – did not receive a job number from the cluster.</p>
                                    <p><b>Resource Error:</b> TaskInstance died because of insufficient resource request, i.e. insufficient memory or runtime.</p>
                                </Popover>
                            )}
                        >
                            <span><FontAwesomeIcon icon={faLightbulb} /></span>
                        </OverlayTrigger>
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
                pagination={taskInstanceData.length === 0 ? undefined : paginationFactory({ sizePerPage: 10 })}
                selectRow={{
                    mode: "radio",
                    hideSelectColumn: true,
                    clickToSelect: true,
                    bgColor: "#848884",
                }}
            />

            <CustomModal
                className="task_instance_modal"
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
                className="task_instance_modal"
                headerContent={
                    <h5> Standard Error</h5>
                }
                bodyContent={
                    <p>
                        <b>Standard Error Path:</b> <br></br>
                        {rowDetail.ti_stderr}<br></br>
                        <br></br>
                        <b>Standard Error Log:</b> <br></br>
                        {rowDetail.ti_stderr_log}
                    </p>
                }
                showModal={showStderrModal}
                setShowModal={setShowStderrModal}

            />
        </div>
    );
}