import "@jobmon_gui/styles/jobmon_gui.css";


import {BiRun} from "react-icons/bi";
import {IoMdCloseCircle, IoMdCloseCircleOutline} from "react-icons/io";
import {AiFillSchedule, AiFillCheckCircle} from "react-icons/ai";
import {TbHandStop} from "react-icons/tb";
import {HiRocketLaunch} from "react-icons/hi2";
import {HiInformationCircle} from "react-icons/hi";
import React, {useState} from "react";
import CustomModal from '@jobmon_gui/components/Modal';

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
    const [showWFInfo, setShowWFInfo] = useState(false)

    return (
        <header className="App-header">
            <div style={{display: 'flex', alignItems: 'center'}}>
                {wf_status === 'A' ? (
                    <p><span className="icon-aa"><IoMdCloseCircleOutline/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'D' ? (
                    <p><span className="icon-dd"><AiFillCheckCircle/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'F' ? (
                    <p><span className="icon-ff"><IoMdCloseCircle/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'G' ? (
                    <p><span className="icon-pp"><AiFillSchedule/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'H' ? (
                    <p><span className="icon-aa"><TbHandStop/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'I' ? (
                    <p><span className="icon-pp"><AiFillSchedule/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'O' ? (
                    <p><span className="icon-ss"><HiRocketLaunch/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'Q' ? (
                    <p><span className="icon-pp"><AiFillSchedule/></span>Workflow ID: {wf_id}</p>
                ) : wf_status === 'R' ? (
                    <p><span className="icon-rr"><BiRun/></span>Workflow ID: {wf_id}</p>
                ) : (
                    <p>Workflow ID: {wf_id}</p>
                )}

                <span style={{ transform: 'translateY(-5px)' }}>
                    <HiInformationCircle onClick={() => setShowWFInfo(true)}/>
                </span>
            </div>
            <div>
                <CustomModal
                    className="workflow_info_modal"
                    headerContent={
                        <h5> Workflow Information</h5>
                    }
                    bodyContent={
                        <p>
                            <b>Workflow Status:</b> {wf_status_desc}<br/>
                            <b>Workflow Tool:</b> {wf_tool}<br/>
                            <b>Workflow Name:</b> {wf_name}<br/>
                            <b>Workflow Args:</b> {wf_args}<br/>
                            <b>Workflow Submitted Date:</b> {wf_submitted_date}<br/>
                            <b>Workflow Status Date:</b> {wf_status_date}<br/>
                            <b>Workflow Elapsed Time:</b> {wf_elapsed_time}<br/>
                            <b>Jobmon Version:</b> {jobmon_version}<br/>
                        </p>
                    }
                    showModal={showWFInfo}
                    setShowModal={setShowWFInfo}
                />
            </div>
        </header>
    )

}