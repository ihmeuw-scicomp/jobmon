import React, {useEffect, useState} from 'react';
import BootstrapTable from "react-bootstrap-table-next";
import filterFactory, {textFilter, dateFilter} from 'react-bootstrap-table2-filter';
import {sanitize} from 'dompurify';
import paginationFactory from "react-bootstrap-table2-paginator";
import 'react-bootstrap-table2-paginator/dist/react-bootstrap-table2-paginator.min.css';
import {HashLink} from 'react-router-hash-link';


import '../../styles/jobmon_gui.css';
import {convertDatePST} from '../../utils/formatters';
import {safe_rum_start_span, safe_rum_unit_end} from '../../utils/rum';
import CustomModal from '../Modal';
import axios from "axios";

export default function Errors({taskTemplateName, taskTemplateId, workflowId, apm}) {

    const [errorDetail, setErrorDetail] = useState({
        'error': '', 'error_time': '', 'task_id': '',
        'task_instance_err_id': '', 'task_instance_id': '', 'time_since': '',
        'task_instance_stderr_log': ''
    });
    const [helper, setHelper] = useState("");
    const [showModal, setShowModal] = useState(false)
    const [justRecentErrors, setRecentErrors] = useState(false)
    const [errorLoading, setErrorLoading] = useState(false);
    const [errorLogs, setErrorLogs] = useState([]);
    const [page, setPage] = useState(1);
    const [sizePerPage, setSizePerPage] = useState(10);
    const [totalSize, setTotalSize] = useState(0);

    function handleToggle() {
        setRecentErrors(!justRecentErrors)
    }

    function getAsyncErrorLogs(wf_id: string, tt_id?: string) {
        setErrorLoading(true);
        const url = process.env.REACT_APP_BASE_URL + "/tt_error_log_viz/" + wf_id + "/" + tt_id;
        const fetchData = async () => {
            const result: any = await axios({
                    method: 'get',
                    url: url,
                    data: null,
                    params: {
                        page: page,
                        page_size: sizePerPage,
                        just_recent_errors: justRecentErrors,
                    },
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                }
            )
            setErrorLogs(result.data.error_logs);
            setErrorLoading(false);
            setTotalSize(result.data.total_count);
        };
        return fetchData
    }

    function get_error_brief(errors) {
        let r: any = [];

        for (let i in errors) {

            let e = errors[i];
            let date_display = `
                <div class="error-time">
                    <span>${convertDatePST(e.error_time)}</span>
                </div>
            `;
            let error_display = `
                <div class="error-log">
                    ${e.error.trim().split("\n").slice(-1)}
                </div>
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
        return r;
    }

    const htmlFormatter = cell => {
        // add sanitize to prevent xss attack
        return <div dangerouslySetInnerHTML={{__html: sanitize(`${cell}`)}}/>;
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
            headerStyle: {width: "10%"},
        },
        {
            dataField: "task_instance_id",
            text: "Task Instance ID",
            headerStyle: {width: "15%"},
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
            headerStyle: {width: "20%"},
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
    useEffect(() => {
        if (typeof workflowId !== 'undefined' && taskTemplateId !== 'undefined' && taskTemplateId !== '') {
            getAsyncErrorLogs(workflowId, taskTemplateId)();
        }
    }, [taskTemplateId, workflowId, page, sizePerPage, justRecentErrors]);

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

    const handleTableChange = (type, {page, sizePerPage}) => {
        setPage(page);
        setSizePerPage(sizePerPage);
    };


    // logic: when task template name selected, show a loading spinner; when loading finished and there is no error, show a no error message; when loading finished and there are errors, show error logs
    return (
        <div>
            <div>
                <span className="span-helper"><i>{helper}</i></span>
                <br/>
                {errorLogs.length !== 0 && !errorLoading &&
                    <>
                        <div className="d-flex pt-4">
                            <p className=''>Show latest TaskInstances for latest WorkflowRun</p>
                            <div className='px-4' onClick={handleToggle}>
                                <div className={"toggle-switch " + (justRecentErrors ? "active" : "")}>
                                    <div/>
                                    <span className={"toggle-slider " + (justRecentErrors ? "active" : "")}></span>
                                </div>
                            </div>
                        </div>
                        <hr/>

                        <BootstrapTable
                            keyField="id"
                            bootstrap4
                            headerClasses="thead-dark"
                            striped
                            data={error_brief}
                            columns={columns}
                            filter={filterFactory()}
                            pagination={paginationFactory({
                                page,
                                sizePerPage,
                                totalSize,
                                onPageChange: (page) => setPage(page),
                                onSizePerPageChange: (sizePerPage) => setSizePerPage(sizePerPage),
                            })}
                            remote={{pagination: true}}
                            onTableChange={handleTableChange}
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


            {errorLogs.length === 0 && taskTemplateName !== "" && !errorLoading &&
                <div>
                    <br/>
                    There is no error log associated with task template <i>{taskTemplateName}</i>.
                </div>
            }

            {taskTemplateName !== "" && errorLoading &&
                <div>
                    <br/>
                    <div className="loader"/>
                </div>
            }
        </div>
    )
}