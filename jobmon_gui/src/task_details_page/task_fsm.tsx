import React from "react";
import ReactFlow, {
    Background,
    Controls,
    MarkerType,
    MiniMap,
    Node,
    Position
} from 'reactflow';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';
import { faLightbulb } from '@fortawesome/free-solid-svg-icons';

import 'reactflow/dist/style.css';

function TaskFSM({ taskStatus }) {
    const task_status = taskStatus[0]
    const nodes: Node[] = [
        {
            id: 'register',
            data: { label: 'G: Registering' },
            type: 'input',
            position: { x: 100, y: 150 },
            sourcePosition: Position.Right,
            ...(task_status === "G" ? { style: { backgroundColor: '#0072b2' } } : null)
        },
        {
            id: 'queued',
            data: { label: 'Q: Queued' },
            position: { x: 275, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "Q" ? { style: { backgroundColor: '#0072b2' } } : null)
        },
        {
            id: 'instantiate',
            data: { label: 'I: Instantiating' },
            position: { x: 450, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "I" ? { style: { backgroundColor: '#0072b2' } } : null)
        },
        {
            id: 'launched',
            data: { label: 'O: Launched' },
            position: { x: 625, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "O" ? { style: { backgroundColor: '#0072b2' } } : null)
        },
        {
            id: 'running',
            data: { label: 'R: Running' },
            position: { x: 800, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "R" ? { style: { backgroundColor: '#0072b2' } } : null)
        },
        {
            id: 'done',
            data: { label: 'D: Done' },
            position: { x: 1000, y: 200 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "D" ? { style: { backgroundColor: '#009e73' } } : null)
        },
        {
            id: 'recoverable',
            data: { label: 'E: Error Recoverable' },
            position: { x: 1000, y: 100 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "E" ? { style: { backgroundColor: '#e69f00' } } : null)
        },
        {
            id: 'fatal',
            data: { label: 'F: Error Fatal ' },
            position: { x: 1200, y: 150 },
            sourcePosition: Position.Right,
            targetPosition: Position.Left,
            ...(task_status === "F" ? { style: { backgroundColor: '#d55e00' } } : null)
        },
        {
            id: 'adjusting',
            data: { label: 'A: Adjusting Resources' },
            position: { x: 1200, y: 50 },
            sourcePosition: Position.Top,
            targetPosition: Position.Left,
            ...(task_status === "A" ? { style: { backgroundColor: '#e69f00' } } : null)
        },
    ];

    const edges = [
        {
            id: 'register-queued',
            source: 'register',
            target: 'queued',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'queued-instantiate',
            source: 'queued',
            target: 'instantiate',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'instantiate-launched',
            source: 'instantiate',
            target: 'launched',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'launched-running',
            source: 'launched',
            target: 'running',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'running-done',
            source: 'running',
            target: 'done',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'running-recoverable',
            source: 'running',
            target: 'recoverable',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'recoverable-fatal',
            source: 'recoverable',
            target: 'fatal',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'recoverable-adjusting',
            source: 'recoverable',
            target: 'adjusting',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },
        {
            id: 'adjusting-queued',
            source: 'adjusting',
            target: 'queued',
            type: 'smoothstep',
            animated: true,
            markerEnd: {
                type: MarkerType.ArrowClosed,
            }
        },

    ];
    return (
        <div>
            <div style={{ display: "flex" }}>
                <header className="header-1">
                    <p>
                        Task Finite State Machine&nbsp;
                        <OverlayTrigger
                            placement="right"
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    <p><b>Registering:</b> Task is bound to the database.</p>
                                    <p><b>Queued:</b> Task's dependencies have successfully completed, task can be run when the scheduler is ready.</p>
                                    <p><b>Instantiating:</b> A task instance is preparing to be launched/submitted.</p>
                                    <p><b>Launched:</b> Task instance submitted to the cluster normally.</p>
                                    <p><b>Running:</b> Task is running on the specified distributor.</p>
                                    <p><b>Error Recoverable:</b> Task has errored out but has more attempts so it will be retried.</p>
                                    <p><b>Adjusting Resources:</b> Task errored with a resource error, the resources will be adjusted before retrying.</p>
                                    <p><b>Error Fatal:</b> Task errored out and has used all of the attempts, therefore has failed for this WorkflowRun. It can be resumed in a new WFR.</p>
                                    <p><b>Done:</b> Task ran successfully to completion; it has a TaskInstance that successfully completed.</p>
                                </Popover>
                            )}
                        >
                            <span><FontAwesomeIcon icon={faLightbulb} /></span>
                        </OverlayTrigger>
                    </p>
                </header>
            </div>

            <div style={{ height: 400 }}>
                <ReactFlow nodes={nodes} edges={edges}>
                    <Background />
                    <Controls />
                    <MiniMap zoomable pannable />
                </ReactFlow>
            </div>
        </div>
    );
}

export default TaskFSM;