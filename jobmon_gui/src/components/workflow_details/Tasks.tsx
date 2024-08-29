import React, { useEffect } from 'react';
import TaskTable from '@jobmon_gui/components/workflow_details/TaskTable';
import { FaSearch } from "react-icons/fa";

import { safe_rum_start_span, safe_rum_unit_end } from '@jobmon_gui/utils/rum'

export default function Tasks({ tasks, onSubmit, register, loading, apm }) {
    useEffect(() => {
        const s = safe_rum_start_span(apm, "tasks", "custom");
        return () => {
            safe_rum_unit_end(s);
        };
    }, [apm]);
    return (
        <div>
            <br></br>
            <div className="float_bar">
                <form className='d-flex align-items-center' onSubmit={onSubmit}>
                    <label className="label-left text-custom">Task Template Name:&nbsp;&nbsp;  </label>
                    <input id="task_template_Name" type="text" {...register("task_template_name")} required />
                    <FaSearch style={{ color: "36486b", marginLeft: "2px" }} onClick={onSubmit} />
                </form>
            </div>
            <TaskTable taskData={tasks} loading={loading} />
        </div>
    )
}