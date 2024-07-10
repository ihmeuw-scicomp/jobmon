import React, {useState} from 'react';

import 'bootstrap/dist/css/bootstrap.min.css';

import WorkflowTable from '@jobmon_gui/components/workflow_overview/WorkflowTable';

import {init_apm} from '@jobmon_gui/utils/rum';
import '@jobmon_gui/styles/jobmon_gui.css';
import Box from "@mui/material/Box";
import WorkflowFilters from "@jobmon_gui/components/workflow_overview/WorkflowFilters";


function WorkflowOverview() {
    const apm: any = init_apm("workflow_overview_page");

    const [refreshData, setRefreshData] = useState(false)


    return (
        <Box>
            <WorkflowFilters setRefreshData={setRefreshData}/>
            <WorkflowTable refreshData={refreshData} setRefreshData={setRefreshData}/>
        </Box>
    );
}

export default WorkflowOverview;
