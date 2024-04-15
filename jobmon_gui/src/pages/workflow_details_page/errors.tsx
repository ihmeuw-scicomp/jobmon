import React, { useEffect, useState } from 'react';
import BootstrapTable from "react-bootstrap-table-next";
import filterFactory, { textFilter, dateFilter } from 'react-bootstrap-table2-filter';
import { sanitize } from 'dompurify';
import paginationFactory from "react-bootstrap-table2-paginator";
import 'react-bootstrap-table2-paginator/dist/react-bootstrap-table2-paginator.min.css';
import { HashLink } from 'react-router-hash-link';


import '../../css/jobmon_gui.css';
import { convertDatePST } from '../../utilities/formatters';
import { safe_rum_start_span, safe_rum_unit_end } from '../../utilities/rum';
import CustomModal from '../../components/Modal';

export default function Errors({ errorLogs, tt_name, loading, apm }) {

    const [errorDetail, setErrorDetail] = useState({
        'error': '', 'error_time': '', 'task_id': '',
        'task_instance_err_id': '', 'task_instance_id': '', 'time_since': '',
        'task_instance_stderr_log': ''
    });
    const [helper, setHelper] = useState("");
    const [showModal, setShowModal] = useState(false)
    const [justRecentErrors, setRecentErrors] = useState(false)

    function handleToggle() {
        setRecentErrors(!justRecentErrors)
    }

    function get_error_brief(errors) {
        let r: any = [];

        for (let i in errors) {

            let e = errors[i];
            if (!justRecentErrors || (justRecentErrors && e.most_recent_attempt)) {

                let date_display = `
            <div class="error-time">
            <span>${convertDatePST(e.error_time)}</span>
            </div>
            `;
                let error_display = `
            <div class="error-log">${e.error.trim().split("\n").slice(-1)}</div>
            `;

                r.push({
                    "id": e.task_instance_err_id,
                    "task_id": e.task_id,
                    "task_instance_id": e.task_instance_id,
                    "brief": error_display,
                    "date": date_display,
                    "time": e.error_time,
                    "error": e.error,
                    "task_instance_stderr_log": e.task_instance_stderr_log
                })
            }
        }
        return r;
    }

    const htmlFormatter = cell => {
        // add sanitize to prevent xss attack
        return <div dangerouslySetInnerHTML={{ __html: sanitize(`${cell}`) }} />;
    };

    const error_brief = get_error_brief(errorLogs);
    const columns = [
        {
            dataField: "id",
            text: "Error Index",
            hidden: true,
        },
        {
            dataField: "task_id",
            text: "Task ID",
            headerStyle: { width: "10%" },
        },
        {
            dataField: "task_instance_id",
            text: "Task Instance ID",
            headerStyle: { width: "15%" },
            sort: true,
            formatter: (cell, row) => <nav>
                <HashLink
                    to={`/task_details/${row.task_id}#${cell}`}
                >
                    {cell}
                </HashLink>
            </nav>
        },
        {
            dataField: "date",
            text: "Error Date",
            formatter: htmlFormatter,
            headerStyle: { width: "20%" },
            filter: dateFilter()

        },

        {
            dataField: "brief",
            text: "Error Log",
            filter: textFilter(),
            formatter: htmlFormatter,
            headerEvents: {
                onMouseEnter: () => {
                    setHelper("The list of task instance error logs with filter. Click to view the error detail.");
                },
                onMouseLeave: () => {
                    setHelper("");
                }
            },
            events: {
                onClick: (e, column, columnIndex, row, rowIndex) => {
                    setShowModal(true)
                    setErrorDetail(row);
                }
            }
        }
    ];

    //hook
    useEffect(() => {
        // clean the error log detail display (right side) when task template changes
        let temp = {
            'error': '', 'error_time': '', 'task_id': '',
            'task_instance_err_id': '', 'task_instance_id': '', 'time_since': '',
            'task_instance_stderr_log': ''
        };
        setErrorDetail(temp);
    }, [errorLogs]);


    useEffect(() => {
        const s = safe_rum_start_span(apm, "tasks", "custom");
        return () => {
            safe_rum_unit_end(s);
        };
    }, [apm]);

    // logic: when task template name selected, show a loading spinner; when loading finished and there is no error, show a no error message; when loading finished and there are errors, show error logs
    return (
        <div>
            <div>
                <span className="span-helper"><i>{helper}</i></span>
                <br />
                {errorLogs.length !== 0 && loading === false &&
                    <>
                        <div className="d-flex pt-4">
                            <p className=''>Show only most recent task instances</p>
                            <div className='px-4' onClick={handleToggle}>
                                <div className={"toggle-switch " + (justRecentErrors ? "active" : "")}>
                                    <div />
                                    <span className={"toggle-slider " + (justRecentErrors ? "active" : "")}
                                    ></span>
                                </div>
                            </div>
                        </div>
                        <hr />

                        <BootstrapTable
                            keyField="id"
                            bootstrap4
                            headerClasses="thead-dark"
                            striped
                            data={error_brief}
                            columns={columns}
                            filter={filterFactory()}
                            pagination={error_brief.length === 0 ? undefined : paginationFactory({ sizePerPage: 10 })}
                            selectRow={{
                                mode: "radio",
                                hideSelectColumn: true,
                                clickToSelect: true,
                                bgColor: "#848884",
                            }}
                        />
                    </>
                }

            </div>

            <CustomModal
                className="error-log-modal"
                headerContent={
                    <h5> Task ID {errorDetail.task_id} - Error log</h5>
                }
                bodyContent={
                    <p>
                        {errorDetail.error}<br></br>
                        <br></br>
                        {errorDetail.task_instance_stderr_log}
                    </p>
                }
                showModal={showModal}
                setShowModal={setShowModal}
            />


            {errorLogs.length === 0 && tt_name !== "" && loading === false &&
                <div>
                    <br />
                    There is no error log associated with task template <i>{tt_name}</i>.
                </div>
            }

            {tt_name !== "" && loading &&
                <div>
                    <br />
                    <div className="loader" />
                </div>
            }
        </div>
    )
}