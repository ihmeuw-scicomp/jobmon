import React from 'react';
import { faSearch } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import TaskTable from './task_table';

import { safe_rum_start_span, safe_rum_unit_end } from '../functions'

export default function Tasks({ tasks, onSubmit, register, loading, apm }) {
    const s: any = safe_rum_start_span(apm, "tasks", "custom");
    return (
        <div>
            <br></br>
            <div className="float_bar">
                <form className='d-flex align-items-center' onSubmit={onSubmit}>
                    <label className="label-left text-custom">Task Template Name:&nbsp;&nbsp;  </label>
                    <input id="task_template_Name" type="text" {...register("task_template_name")} required />
                    <FontAwesomeIcon className='fa-xl ml-3' style={{ color: "36486b" }} icon={faSearch} onClick={onSubmit} /> 
                </form>
            </div>
            <TaskTable taskData={tasks} loading={loading} />
        </div>
    )
    safe_rum_unit_end(s);
}