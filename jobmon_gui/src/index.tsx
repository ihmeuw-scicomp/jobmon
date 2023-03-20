import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './workflow_overview_page/workflow_overview';
import reportWebVitals from './reportWebVitals';
import {
  HashRouter,
  Routes,
  Route,
} from "react-router-dom";
import WorkflowDetails from './workflow_details_page/workflow_details_page'
import TaskDetails from './task_details_page/task_details';

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

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
