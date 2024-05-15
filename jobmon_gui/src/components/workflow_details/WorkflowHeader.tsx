import "../../styles/jobmon_gui.css";

import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';

import { BiRun} from "react-icons/bi";
import { IoMdCloseCircle, IoMdCloseCircleOutline } from "react-icons/io";
import { AiFillSchedule, AiFillCheckCircle} from "react-icons/ai";
import { TbHandStop } from "react-icons/tb";
import {HiRocketLaunch} from "react-icons/hi2";

export default function WorkflowHeader({
    wf_id,
    wf_status,
    wf_status_desc,
    wf_tool,
    wf_name,
    wf_args,
    wf_submitted_date,
    wf_status_date,
    wf_elapsed_time,
    jobmon_version
}) {

  return (
     <header className="App-header">
       <OverlayTrigger
            placement="bottom"
            trigger={["hover", "focus"]}
            overlay={(
              <Popover id="task_count" className="header-popover" style={{ zIndex: 9999 }}>
                        <p><b>Workflow Status:</b> {wf_status_desc}</p>
                        <hr/>
                        <p><b>Workflow Tool:</b> {wf_tool}</p>
                        <p><b>Workflow Name:</b> {wf_name}</p>
                        <p><b>Workflow Args:</b> {wf_args}</p>
                        <p><b>Workflow Submitted Date:</b> {wf_submitted_date}</p>
                        <p><b>Workflow Status Date:</b> {wf_status_date}</p>
                        <p><b>Workflow Elapsed Time:</b> {wf_elapsed_time}</p>
                        <p><b>Jobmon Version:</b> {jobmon_version} </p>
              </Popover>
            )}
          >
          {wf_status === 'A' ? (
            <p> <span className="icon-aa"><IoMdCloseCircleOutline/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'D' ? (
            <p> <span className="icon-dd"><AiFillCheckCircle/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'F' ? (
            <p> <span className="icon-ff"><IoMdCloseCircle/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'G' ? (
            <p> <span className="icon-pp"><AiFillSchedule/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'H' ? (
            <p> <span className="icon-aa"><TbHandStop/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'I' ? (
            <p> <span className="icon-pp"><AiFillSchedule/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'O' ? (
            <p> <span className="icon-ss"><HiRocketLaunch/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'Q' ? (
            <p> <span className="icon-pp"><AiFillSchedule/></span>Workflow ID: {wf_id}</p>
          ) : wf_status === 'R' ? (
            <p> <span className="icon-rr"><BiRun/></span>Workflow ID: {wf_id}</p>
          ) : (
          <p>Workflow ID: {wf_id}</p>
          )}
      </OverlayTrigger>
    </header>
  )

}