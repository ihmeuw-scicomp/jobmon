import React, { useEffect, useState, useRef } from 'react';
import { useForm } from "react-hook-form";
import { useSearchParams, useNavigate } from "react-router-dom";
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import 'bootstrap/dist/css/bootstrap.min.css';

import axios from 'axios';

// @ts-ignore
import JobmonWFTable from './wf_table.tsx';
import '../jobmon_gui.css';
import { init_apm, safe_rum_add_label, safe_rum_transaction, safe_rum_start_span, safe_rum_unit_end } from '../functions';
import { FaCircle } from "react-icons/fa";

function App() {
  const apm: any = init_apm("workflow_overview_page");
  let rum_t: any = safe_rum_transaction(apm);
  const [user, setUser] = useState('');
  const [tool, setTool] = useState('');
  const [wf_name, setWFName] = useState('');
  const [wf_args, setWFArgs] = useState('');
  const [date_submitted, setDateSubmitted] = useState('');
  const [status, setStatus] = useState('');
  const [workflows, setWorkflows] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();


  //***********************hooks*****************************

  //page loading hook
  useEffect(() => {
    let url_user = searchParams.get("user");
    let url_tool = searchParams.get("tool");
    let url_wf_name = searchParams.get("wf_name");
    let url_wf_args = searchParams.get("wf_args");
    let url_date_submitted = searchParams.get("date_submitted");
    let url_status = searchParams.get("status");
    if (url_user !== null && url_user !== "" && url_user !== undefined) {
      setUser(url_user);
    }
    if (url_tool !== null && url_tool !== "" && url_tool !== undefined) {
      setTool(url_tool)
    }
    if (url_wf_name !== null && url_wf_name !== "" && url_wf_name !== undefined) {
      setWFName(url_wf_name);
    }
    if (url_wf_args !== null && url_wf_args !== "" && url_wf_args !== undefined) {
      setWFArgs(url_wf_args)
    }
    if (url_date_submitted !== null && url_date_submitted !== "" && url_date_submitted !== undefined) {
      setDateSubmitted(url_date_submitted)
    }
    if (url_status !== null && url_status !== "" && url_status !== undefined) {
      setStatus(url_status)
    }
  }, []);

  const firstUpdate = useRef(true);
  //user change hook
  useEffect(() => {
    if (firstUpdate.current) {
      firstUpdate.current = false;
      return;
    }
    const rum_s1: any = safe_rum_start_span(apm, "landing_page", "external.http");
    safe_rum_add_label(rum_s1, "user", user);
    const params = new URLSearchParams();
    params.append("user", user)
    params.append("tool", tool)
    params.append("wf_name", wf_name)
    params.append("wf_args", wf_args)
    params.append("date_submitted", date_submitted)
    params.append("status", status)
    const request = {
      params: params,
    };
    let workflow_status_url = process.env.REACT_APP_BASE_URL + "/workflow_overview_viz";
    const workflow_status_renders = {
      "PENDING": (<div>< label className="label-middle" > <FaCircle className="bar-pp" /> </label><label className="label-left font-weight-300">PENDING  </label></div >),
      "SCHEDULED": (<div><label className="label-middle"><FaCircle className="bar-ss" /> </label><label className="label-left font-weight-300">SCHEDULED  </label></div>),
      "RUNNING": (<div>< label className="label-middle" > <FaCircle className="bar-rr" /> </label><label className="label-left font-weight-300">RUNNING  </label></div >),
      "FAILED": (<div>< label className="label-middle" > <FaCircle className="bar-ff" /> </label><label className="label-left font-weight-300">FAILED  </label></div >),
      "DONE": (<div>< label className="label-middle" > <FaCircle className="bar-dd" /> </label><label className="label-left font-weight-300">DONE  </label></div >)
    }
    const fetchData = async () => {
      const result: any = await axios({
        method: 'get',
        url: workflow_status_url,
        params: params,
        data: null,
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'}
      }
//         workflow_status_url,
//         request
      );
      let wfs = result.data.workflows;
      wfs.forEach((workflow) => {
        if (workflow.wf_status in workflow_status_renders) {
          workflow.wf_status = workflow_status_renders[workflow.wf_status]
        } else {
          workflow.wf_status = (<div>< label className="label-middle" > <FaCircle className="bar-pp" /> </label><label className="label-left font-weight-300">{workflow.wf_status} </label></div >)
        }
      })
      setWorkflows(wfs);
    };
    fetchData();
    safe_rum_unit_end(rum_s1);
  }, [user, tool, wf_name, wf_args, date_submitted, status]);


  //*******************event handling****************************
  // user form
  const { register, handleSubmit } = useForm();
  const onSubmit = handleSubmit((d) => {
    navigate('/?user=' + d["user_input"] + "&tool=" + d["tool_input"] + "&wf_name=" + d["wf_name_input"] + "&wf_args=" + d["wf_args_input"] + "&date_submitted=" + d["date_submitted_input"] + "&status=" + d["status"]);
    setUser(d["user_input"]);
    setTool(d["tool_input"])
    setWFName(d["wf_name_input"])
    setWFArgs(d["wf_args_input"])
    setDateSubmitted(d["date_submitted_input"])
    setStatus(d["status"])
  });

  const handleClear = handleSubmit((d) => {
    navigate('/?user=' + "&tool=" + "&wf_name=" + "&wf_args=" + "&date_submitted=" + "&status=");
    navigate(0)
  });
  //********************html page*************************************
  return (
    <div id="div-main" className="App">
      <div id="div-header" className="div-level-2">
        <header className="App-header">
        </header>
      </div>

      <div className="div-level-2">
        <form>
          <Row className="mb-3">
            <Form.Group as={Col} controlId="formGridEmail">
              <Form.Label><span className='m-2'> Username</span></Form.Label>
              <Form.Control type="text" placeholder="Username" defaultValue={user} {...register("user_input")} />
            </Form.Group>

            <Form.Group as={Col} controlId="formGridEmail">
              <Form.Label><span className='m-2'> Workflow Args</span></Form.Label>
              <Form.Control type="text" placeholder="Workflow Args" defaultValue={wf_args} {...register("wf_args_input")} />
            </Form.Group>

            <Form.Group as={Col} controlId="formGridPassword">
              <Form.Label><span className='m-2'> Tool</span></Form.Label>
              <Form.Control type="text" placeholder="Tool" defaultValue={tool} {...register("tool_input")} />
            </Form.Group>
          </Row>

          <Row className="mb-3">
            <Form.Group as={Col} controlId="formGridPassword">
              <Form.Label><span className='m-2'> Date Workflow Was Submitted - On or After Date</span></Form.Label>
              <Form.Control type="date" defaultValue={date_submitted} {...register("date_submitted_input")} />
            </Form.Group>

            <Form.Group as={Col} controlId="formGridPassword">
              <Form.Label><span className='m-2'> Workflow Name</span></Form.Label>
              <Form.Control type="text" placeholder="Workflow Name" defaultValue={wf_name} {...register("wf_name_input")} />
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
          </Row>

          <div className="text-center">
            <div className="btn-toolbar d-inline-block">
              <button type="submit" className="btn btn-custom mr-1" onClick={onSubmit}>Submit</button>
              <button type="submit" className="btn btn-custom mr-1" onClick={handleClear}>Clear All</button>
            </div>
          </div>
        </form>
      </div>
      <div id="wftable" className="div-level-2">

        <JobmonWFTable allData={workflows} />

      </div>
    </div>
  );
}

export default App;
