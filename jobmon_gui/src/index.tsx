import React from 'react';
import ReactDOM from 'react-dom/client';
import WorkflowOverview from '@jobmon_gui/screens/WorkflowOverview';
import {
    HashRouter,
    Route,
} from "react-router-dom";
import WorkflowDetails from '@jobmon_gui/screens/WorkflowDetails'
import TaskDetails from '@jobmon_gui/screens/TaskDetails';
import Help from '@jobmon_gui/screens/Help';
import JobmonAtIHME from '@jobmon_gui/screens/JobmonAtIhme'
import PageNavigation from '@jobmon_gui/components/navigation/PageNavigation';
import CustomThemeProvider from "@jobmon_gui/contexts/CustomThemeProvider";
import {QueryClient, QueryClientProvider} from "@tanstack/react-query";
import '@fontsource-variable/roboto-mono';
import "@fontsource/archivo";
import {ApmRoutes} from '@elastic/apm-rum-react'
import {AuthProvider} from "@jobmon_gui/contexts/AuthContext.tsx";

const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            retry: 3,
            staleTime: 5000,
            refetchOnWindowFocus: false,
        },
    },
});

const root = ReactDOM.createRoot(
    document.getElementById('root') as HTMLElement
);
root.render(
    <QueryClientProvider client={queryClient}>
        <HashRouter>
            <CustomThemeProvider>
                <AuthProvider>
                    <PageNavigation>
                        <ApmRoutes>
                            <Route path="workflow/:workflowId" element={<WorkflowDetails/>}></Route>
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
                        </ApmRoutes>
                    </PageNavigation>
                </AuthProvider>
            </CustomThemeProvider>
        </HashRouter>
    </QueryClientProvider>
);
