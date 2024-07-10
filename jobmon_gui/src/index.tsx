import React from 'react';
import ReactDOM from 'react-dom/client';
import '@jobmon_gui/styles/index.css';
import WorkflowOverview from '@jobmon_gui/screens/WorkflowOverview';
import {
    HashRouter,
    Routes,
    Route,
} from "react-router-dom";
import WorkflowDetails from '@jobmon_gui/screens/WorkflowDetails'
import TaskDetails from '@jobmon_gui/screens/TaskDetails';
import Help from '@jobmon_gui/screens/Help';
import JobmonAtIHME from '@jobmon_gui/screens/JobmonAtIhme'
import PageNavigation from '@jobmon_gui/components/navigation/PageNavigation';
import CustomThemeProvider from "@jobmon_gui/contexts/CustomThemeProvider";


const root = ReactDOM.createRoot(
    document.getElementById('root') as HTMLElement
);
root.render(
    <HashRouter>
        <CustomThemeProvider>
            <PageNavigation>
                <Routes>
                    <Route path="workflow">
                        <Route path=":workflowId/tasks" element={<WorkflowDetails subpage="tasks"/>}/>
                        <Route path=":workflowId/usage" element={<WorkflowDetails subpage="usage"/>}/>
                        <Route path=":workflowId/errors" element={<WorkflowDetails subpage="errors"/>}/>
                    </Route>
                    <Route path="task_details/:taskId" element={<TaskDetails/>}></Route>
                    <Route path="help" element={<Help/>}></Route>
                    <Route path="jobmon_at_ihme" element={<JobmonAtIHME/>}></Route>
                    <Route path="/" element={<WorkflowOverview/>}/>
                    <Route
                        path="*"
                        element={
                            <main style={{padding: "1rem"}}>
                                <p>Whoops! There's nothing here!</p>
                            </main>
                        }
                    />
                </Routes>
            </PageNavigation>
        </CustomThemeProvider>
    </HashRouter>
);
