import React, { useEffect, useState } from 'react';
import { useForm } from "react-hook-form";
import { useSearchParams, useNavigate} from "react-router-dom";
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import 'bootstrap/dist/css/bootstrap.min.css';

import axios from 'axios';

// @ts-ignore
import JobmonWFTable from './wf_table.tsx';
import './jobmon_gui.css';

function App() {
  const [user, setUser] = useState('');
  const [tool, setTool] = useState('');
  const [wf_name, setWFName] = useState('');
  const [wf_args, setWFArgs] = useState('');
  const [date_submitted, setDateSubmitted] = useState('');
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
  }, []);

  //user change hook
  useEffect(() => {
    const params = new URLSearchParams();
    params.append("user", user)
    params.append("tool", tool)
    params.append("wf_name", wf_name)
    params.append("wf_args", wf_args)
    params.append("date_submitted", date_submitted)
    const request = {
      params: params,
    };
    let workflow_status_url = process.env.REACT_APP_BASE_URL + "/workflow_overview_viz";
    const fetchData = async () => {
      const result: any = await axios(
        workflow_status_url,
        request
      );
      let wfs = result.data.workflows;
      setWorkflows(wfs);
    };
    fetchData();

  }, [user, tool, wf_name, wf_args, date_submitted]);

  //*******************event handling****************************
  // user form
  const { register, handleSubmit } = useForm();
  const onSubmit = handleSubmit((d) => {
    navigate('/?user=' + d["user_input"] + "&tool=" + d["tool_input"] + "&wf_name=" + d["wf_name_input"] + "&wf_args=" + d["wf_args_input"] + "&date_submitted=" + d["date_submitted_input"]);
    setUser(d["user_input"]);
    setTool(d["tool_input"])
    setWFName(d["wf_name_input"])
    setWFArgs(d["wf_args_input"])
    setDateSubmitted(d["date_submitted_input"])
  });

  //********************html page*************************************
  return (
    <div id="div-main" className="App">
      <div id="div-header" className="div-level-2">
        <header className="App-header">
          <p>Jobmon GUI</p>
        </header>
        <hr className="hr-1" />
      </div>

      <div className="div-level-2">
        <form>
          <Row className="mb-3">
            <Form.Group as={Col} controlId="formGridEmail">
              <Form.Label>Username</Form.Label>
              <Form.Control type="text" placeholder="Username" defaultValue={user} {...register("user_input")}/>
            </Form.Group>

            <Form.Group as={Col} controlId="formGridPassword">
              <Form.Label>Tool</Form.Label>
              <Form.Control type="text" placeholder="Tool" defaultValue={tool} {...register("tool_input")}/>
            </Form.Group>

            <Form.Group as={Col} controlId="formGridPassword">
              <Form.Label>Workflow Name</Form.Label>
              <Form.Control type="text" placeholder="Workflow Name" defaultValue={wf_name} {...register("wf_name_input")}/>
            </Form.Group>
          </Row>

          <Row className="mb-3">
            <Form.Group as={Col} controlId="formGridEmail">
              <Form.Label>Workflow Args</Form.Label>
              <Form.Control type="text" placeholder="Workflow Args" defaultValue={wf_args} {...register("wf_args_input")}/>
            </Form.Group>

            <Form.Group as={Col} controlId="formGridPassword">
              <Form.Label>Date Workflow Was Submitted - On or After Date</Form.Label>
              <Form.Control type="date" defaultValue={date_submitted} {...register("date_submitted_input")}/>
            </Form.Group>
          </Row>
          <Button variant="primary" type="submit" className="btn btn-dark"  onClick={onSubmit}>
            Submit
          </Button>
        </form>
      </div>
      <hr/>
      <div id="wftable" className="div-level-2">

        <JobmonWFTable allData={workflows} />

      </div>
    </div>
  );
}

export default App;