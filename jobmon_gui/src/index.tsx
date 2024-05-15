import React from 'react';
import ReactDOM from 'react-dom/client';
import './styles/index.css';
import App from './screens/WorkflowOverview';
import {
  HashRouter,
  Routes,
  Route,
} from "react-router-dom";
import WorkflowDetails from './screens/WorkflowDetails'
import TaskDetails from './screens/TaskDetails';
import Help from './screens/Help';
import JobmonAtIHME from './screens/JobmonAtIhme'
import PageNavigation from './components/navigation/PageNavigation';

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
