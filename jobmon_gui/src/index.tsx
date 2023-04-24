import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './components/workflow_overview_page/workflow_overview';
import {
  HashRouter,
  Routes,
  Route,
} from "react-router-dom";
import WorkflowDetails from './components/workflow_details_page/workflow_details_page'
import TaskDetails from './components/task_details_page/task_details';

const root = ReactDOM.createRoot(
    document.getElementById('root') as HTMLElement
);
root.render(
  <HashRouter>
    <Routes>
      <Route path="workflow">
        <Route path=":workflowId/tasks" element={<WorkflowDetails subpage="tasks" />} />
        <Route path=":workflowId/usage" element={<WorkflowDetails subpage="usage" />} />
        <Route path=":workflowId/errors" element={<WorkflowDetails subpage="errors" />} />
      </Route>
      <Route path="task_details/:taskId" element={<TaskDetails />}></Route>
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
  </HashRouter>
);

export * from './components';