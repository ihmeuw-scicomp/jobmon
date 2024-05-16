import React from 'react';
import {FaCircle} from "react-icons/fa";
import '../../styles/jobmon_gui.css';

const WorkflowStatus = ({status}) => {
    const statusRenderMap = {
        "PENDING": (
            <div>
                <label className="label-middle"><FaCircle className="bar-pp"/></label>
                <label className="label-left font-weight-300">PENDING</label>
            </div>
        ),
        "SCHEDULED": (
            <div>
                <label className="label-middle"><FaCircle className="bar-ss"/></label>
                <label className="label-left font-weight-300">SCHEDULED</label>
            </div>
        ),
        "RUNNING": (
            <div>
                <label className="label-middle"><FaCircle className="bar-rr"/></label>
                <label className="label-left font-weight-300">RUNNING</label>
            </div>
        ),
        "FAILED": (
            <div>
                <label className="label-middle"><FaCircle className="bar-ff"/></label>
                <label className="label-left font-weight-300">FAILED</label>
            </div>
        ),
        "DONE": (
            <div>
                <label className="label-middle"><FaCircle className="bar-dd"/></label>
                <label className="label-left font-weight-300">DONE</label>
            </div>
        ),
        "DEFAULT": ( // Fallback for unhandled statuses
            <div>
                <label className="label-middle"><FaCircle className="bar-pp"/></label>
                <label className="label-left font-weight-300">{status}</label>
            </div>
        ),
    };

    return statusRenderMap[status] || statusRenderMap["DEFAULT"];
};

export default WorkflowStatus;
