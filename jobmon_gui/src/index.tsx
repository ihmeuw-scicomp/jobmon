import React from 'react';
import ReactDOM from 'react-dom/client';
import './css/index.css';
import App from './pages/workflow_overview_page/workflow_overview';
import {
  HashRouter,
  Routes,
  Route,
} from "react-router-dom";
import WorkflowDetails from './pages/workflow_details_page/workflow_details_page'
import TaskDetails from './pages/task_details_page/task_details';
import Help from './pages/help_page/help';
import JobmonAtIHME from './pages/jobmon_at_ihme_page/jobmon_at_ihme'
import PageNavigation from './pages/page_navigation/page_navigation'

const root = ReactDOM.createRoot(
    document.getElementById('root') as HTMLElement
);
root.render(
  <HashRouter>
    <PageNavigation>
        <Routes>
          <Route path="workflow">
            <Route path=":workflowId/tasks" element={<WorkflowDetails subpage="tasks" />} />
            <Route path=":workflowId/usage" element={<WorkflowDetails subpage="usage" />} />
            <Route path=":workflowId/errors" element={<WorkflowDetails subpage="errors" />} />
          </Route>
          <Route path="task_details/:taskId" element={<TaskDetails />}></Route>
          <Route path="help" element={<Help />}></Route>
          <Route path="jobmon_at_ihme" element={<JobmonAtIHME />}></Route>
          <Route path="/" element={<App />} />
          <Route
            path="*"
            element={
              <main style={{ padding: "1rem" }}>
                <p>Whoops! There's nothing here!</p>
              </main>
            }
          />
        </Routes>
    </PageNavigation>
  </HashRouter>
);
