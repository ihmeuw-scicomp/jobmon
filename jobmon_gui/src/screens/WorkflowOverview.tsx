import React from 'react';

import 'bootstrap/dist/css/bootstrap.min.css';

import WorkflowList from '@jobmon_gui/components/workflow_overview/WorkflowList';

import { init_apm } from '@jobmon_gui/utils/rum';
import '@jobmon_gui/styles/jobmon_gui.css';
import Box from '@mui/material/Box';
import WorkflowFilters from '@jobmon_gui/components/workflow_overview/WorkflowFilters';

function WorkflowOverview() {
    init_apm('workflow_overview_page');

    return (
        <Box>
            <WorkflowFilters />
            <WorkflowList />
        </Box>
    );
}

export default WorkflowOverview;
