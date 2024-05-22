import React, {useEffect, useState, useRef} from 'react';
import {useForm} from 'react-hook-form';
import {useSearchParams, useNavigate} from 'react-router-dom';
import {Form, Row, Col} from 'react-bootstrap';
import axios from 'axios';

import 'bootstrap/dist/css/bootstrap.min.css';

import WorkflowTable from '../components/workflow_overview/WorkflowTable';
import WorkflowStatus from '../components/workflow_overview/WorkflowStatus';

import {init_apm, safe_rum_add_label, safe_rum_start_span, safe_rum_unit_end} from '../utils/rum';
import '../styles/jobmon_gui.css';


function WorkflowOverview() {
    const apm: any = init_apm("workflow_overview_page");
    const [user, setUser] = useState('');
    const [tool, setTool] = useState('');
    const [wf_name, setWFName] = useState('');
    const [wf_args, setWFArgs] = useState('');
    const [wf_attribute, setWFAttribute] = useState('');
    const [wf_id, setWFID] = useState('')
    const [date_submitted, setDateSubmitted] = useState('');
    const [two_weeks_ago_date, setTwoWeeksAgoDate] = useState('');
    const [status, setStatus] = useState('');
    const [workflows, setWorkflows] = useState([]);
    const [searchParams] = useSearchParams();
    const navigate = useNavigate();

    useEffect(() => {
        const queryParams = {
            user: setUser,
            tool: setTool,
            wf_name: setWFName,
            wf_args: setWFArgs,
            wf_attribute: setWFAttribute,
            wf_id: setWFID,
            date_submitted: setDateSubmitted,
            status: setStatus
        };

        Object.keys(queryParams).forEach(key => {
            const value = searchParams.get(key);
            if (value) {
                queryParams[key](value);
            }
        });
    }, [searchParams]);

    const firstUpdate = useRef(true);
    useEffect(() => {
        if (firstUpdate.current) {
            // By default, only show workflows that were submitted in the last two weeks
            const twoWeeksAgo = new Date(Date.now() - 12096e5); // 2 weeks in milliseconds
            const formattedDate = twoWeeksAgo.toISOString().split('T')[0];
            setTwoWeeksAgoDate(formattedDate);
            firstUpdate.current = false;
            return;
        }
        const rum_s1: any = safe_rum_start_span(apm, "landing_page", "external.http");
        safe_rum_add_label(rum_s1, "user", user);
        const params = new URLSearchParams({
            user,
            tool,
            wf_name,
            wf_args,
            wf_attribute,
            wf_id,
            date_submitted,
            status
        });
        let workflow_status_url = import.meta.env.VITE_APP_BASE_URL + "/workflow_overview_viz";

        const fetchData = async () => {
            const result: any = await axios({
                    method: 'get',
                    url: workflow_status_url,
                    params: params,
                    data: null,
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                }
            );
            let wfs = result.data.workflows;
            wfs.forEach((workflow) => {
                workflow.wf_status = <WorkflowStatus status={workflow.wf_status}/>;
            });
            setWorkflows(wfs);
        };
        fetchData();
        safe_rum_unit_end(rum_s1);
    }, [user, tool, wf_name, wf_args, wf_attribute, date_submitted, status, wf_id, apm]);


    const {register, handleSubmit} = useForm();
    const onSubmit = handleSubmit(({
                                       user_input: user,
                                       tool_input: tool,
                                       wf_name_input: wf_name,
                                       wf_args_input: wf_args,
                                       wf_attribute_input: wf_attribute,
                                       wf_id: wf_id,
                                       date_submitted_input: date_submitted,
                                       status: status
                                   }) => {
        if (!date_submitted) {
            date_submitted = two_weeks_ago_date;
        }
        const queryParams = new URLSearchParams({
            user,
            tool,
            wf_name,
            wf_args,
            wf_attribute,
            wf_id,
            date_submitted,
            status
        });
        navigate('/?' + queryParams.toString());
        setUser(user);
        setTool(tool);
        setWFName(wf_name);
        setWFArgs(wf_args);
        setWFAttribute(wf_attribute);
        setWFID(wf_id);
        setDateSubmitted(date_submitted);
        setStatus(status);
    });

    const handleClear = handleSubmit((d) => {
        navigate("/")
        navigate(0)
    });

    return (
        <div id="div-main" className="App">
            <div id="div-header" className="div-level-2">
                <header className="App-header">
                </header>
            </div>

            <div className="div-level-2">
                <form>
                    <Row className="mb-3">
                        <Form.Group as={Col} controlId="formUsername">
                            <Form.Label><span className='m-2'> Username</span></Form.Label>
                            <Form.Control type="text" placeholder="Username"
                                          defaultValue={user} {...register("user_input")} />
                        </Form.Group>

                        <Form.Group as={Col} controlId="formWFArgs">
                            <Form.Label><span className='m-2'> Workflow Args</span></Form.Label>
                            <Form.Control type="text" placeholder="Workflow Args"
                                          defaultValue={wf_args} {...register("wf_args_input")} />
                        </Form.Group>

                        <Form.Group as={Col} controlId="formWFAttribute">
                            <Form.Label><span className='m-2'> Workflow Attribute Value</span></Form.Label>
                            <Form.Control type="text" placeholder="Workflow Attribute Value"
                                          defaultValue={wf_attribute} {...register("wf_attribute_input")} />
                        </Form.Group>

                        <Form.Group as={Col} controlId="formTool">
                            <Form.Label><span className='m-2'> Tool</span></Form.Label>
                            <Form.Control type="text" placeholder="Tool"
                                          defaultValue={tool} {...register("tool_input")} />
                        </Form.Group>
                    </Row>

                    <Row className="mb-3">
                        <Form.Group as={Col} controlId="formWFDate">
                            <Form.Label><span className='m-2'> Submitted Workflow Date - On or After</span></Form.Label>
                            <Form.Control type="date"
                                          defaultValue={two_weeks_ago_date} {...register("date_submitted_input")} />
                        </Form.Group>

                        <Form.Group as={Col} controlId="formWFName">
                            <Form.Label><span className='m-2'> Workflow Name</span></Form.Label>
                            <Form.Control type="text" placeholder="Workflow Name"
                                          defaultValue={wf_name} {...register("wf_name_input")} />
                        </Form.Group>

                        <Form.Group as={Col} controlId="status">
                            <Form.Label>Workflow Status</Form.Label>
                            <Form.Control as="select" defaultValue={undefined} {...register("status")} >
                                <option>{undefined}</option>
                                <option value="A">Aborted</option>
                                <option value="D">Done</option>
                                <option value="F">Failed</option>
                                <option value="G">Registering</option>
                                <option value="H">Halted</option>
                                <option value="I">Instantiating</option>
                                <option value="O">Launched</option>
                                <option value="Q">Queued</option>
                                <option value="R">Running</option>
                            </Form.Control>
                        </Form.Group>

                        <Form.Group as={Col} controlId="formWFID">
                            <Form.Label><span className='m-2'> Workflow ID</span></Form.Label>
                            <Form.Control type="text" placeholder="Workflow ID"
                                          defaultValue={wf_id} {...register("wf_id")} />
                        </Form.Group>
                    </Row>

                    <div className="text-center">
                        <div className="btn-toolbar d-inline-block">
                            <button type="submit" className="btn btn-custom mr-1" onClick={onSubmit}>Submit</button>
                            <button type="submit" className="btn btn-custom mr-1" onClick={handleClear}>Clear All
                            </button>
                        </div>
                    </div>
                </form>
            </div>
            <div id="wftable" className="div-level-2">
                {workflows.length !== 0 && <WorkflowTable allData={workflows}/>}
                {workflows.length === 0 && <p>No workflows found for specified filters.</p>}
            </div>
        </div>
    );
}

export default WorkflowOverview;
