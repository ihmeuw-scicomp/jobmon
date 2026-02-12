import React, { useEffect } from 'react';
import { useLocation } from 'react-router-dom';

import 'bootstrap/dist/css/bootstrap.min.css';

import WorkflowList from '@jobmon_gui/components/workflow_overview/WorkflowList';

import { init_apm } from '@jobmon_gui/utils/rum';
import '@jobmon_gui/styles/jobmon_gui.css';
import Box from '@mui/material/Box';
import WorkflowFilters from '@jobmon_gui/components/workflow_overview/WorkflowFilters';
import { saveWorkflowFilters } from '@jobmon_gui/utils/workflowFilterPersistence';

function WorkflowOverview() {
    init_apm('workflow_overview_page');
    const location = useLocation();

    // Persist filter params when on landing page
    useEffect(() => {
        if (location.pathname === '/' && location.search) {
            saveWorkflowFilters(location.search);
        }
    }, [location.pathname, location.search]);

    return (
        <Box>
            <WorkflowFilters />
            <WorkflowList />
        </Box>
    );
}

export default WorkflowOverview;
