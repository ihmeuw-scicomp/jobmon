import React, { useEffect } from 'react';
import TaskTable from '@jobmon_gui/components/workflow_details/TaskTable';
import { safe_rum_start_span, safe_rum_unit_end } from '@jobmon_gui/utils/rum'

export default function Tasks({ tasks, loading, apm }) {
    useEffect(() => {
        const s = safe_rum_start_span(apm, "tasks", "custom");
        return () => {
            safe_rum_unit_end(s);
        };
    }, [apm]);
    return (
        <div>
            <br></br>
            <TaskTable taskData={tasks} loading={loading} />
        </div>
    )
}