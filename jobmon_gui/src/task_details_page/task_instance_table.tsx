import React from 'react';
import "react-bootstrap-table-next/dist/react-bootstrap-table2.min.css"
import BootstrapTable, { ColumnDescription } from "react-bootstrap-table-next";
import paginationFactory from "react-bootstrap-table2-paginator";
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faLightbulb } from '@fortawesome/free-solid-svg-icons';


export default function TaskInstanceTable({ taskInstanceData }) {

    const columns: Array<ColumnDescription> = [
        {
            dataField: "ti_id",
            text: "ID",
            sort: true,
            formatter: (cell) => (
                <div id={`${cell}`}>{cell}</div>
              ) 
        },
        {
            dataField: "ti_status",
            text: "Status",
            sort: true,
        },
        {
            dataField: "ti_stdout",
            text: "Stdout Path",
            sort: true,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_stderr",
            text: "Stderr Path",
            sort: true,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_distributor_id",
            text: "Distributor ID",
            sort: true,
        },
        {
            dataField: "ti_nodename",
            text: "Node Name",
            sort: true,
            style: { overflowWrap: 'break-word' },
        },
        {
            dataField: "ti_error_log_description",
            text: "Error Log",
            sort: true,
            style: { overflowWrap: 'break-word' },
        }
    ]

    // Create and return the React Bootstrap Table
    return (
        <div>
            <div style={{ display: "flex" }}>
                <header className="header-1">
                    <p>
                        TaskInstances&nbsp;
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
                data={taskInstanceData}
                columns={columns}
                bootstrap4
                headerClasses="thead-dark"
                striped
                pagination={taskInstanceData.length === 0 ? undefined : paginationFactory({ sizePerPage: 10 })}
            />
        </div>
    );
}