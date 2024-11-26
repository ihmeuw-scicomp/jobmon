import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import {Link} from "react-router-dom";
import Card from 'react-bootstrap/Card';
import ListGroup from 'react-bootstrap/ListGroup';
import {useQuery} from "@tanstack/react-query";
import {CircularProgress} from "@mui/material";
import Typography from "@mui/material/Typography";
import {getTaskDependenciesQuernFn} from "@jobmon_gui/queries/GetTaskDependancies.ts";

type NodeListsProps = {
    taskId: string | number
}
export default function NodeLists({taskId}: NodeListsProps) {

    const taskDependencies = useQuery(
        {
            queryKey: ["task_dependencies", taskId],
            queryFn: getTaskDependenciesQuernFn,
            refetchInterval: 60_000,
        }
    )


    if (taskDependencies.isError) {
        return <Typography>Error loading upstream and downstream tasks. Please reload and try agian.</Typography>;
    }
    if (taskDependencies.isLoading || !taskDependencies.data) {
        return <CircularProgress/>;
    }

    return (
        <div className='div-level-2 pl-5 pt-2'>
            <p className='font-weight-bold'>Dependencies</p>
            <div className="card-columns d-flex justify-content-center flex-column">
                <Card className="dependency-list-scroll">
                    <Card.Header className="dependency-header">Upstream Task IDs</Card.Header>
                    <ListGroup variant="flush">
                        {
                            taskDependencies.data.up.flat(1).map(d => (
                                <ListGroup.Item className="dependency-list-group-item">
                                    <Link
                                        to={{pathname: `/task_details/${d["id"]}`}}
                                        key={d["id"]}>{d["name"]}
                                    </Link>
                                </ListGroup.Item>
                            ))
                        }
                    </ListGroup>
                </Card>
                <Card className="dependency-list-scroll">
                    <Card.Header className="dependency-header">Downstream Task IDs</Card.Header>
                    <ListGroup variant="flush">
                        {
                            taskDependencies.data.down.flat(1).map(d => (
                                <ListGroup.Item className="dependency-list-group-item">
                                    <Link
                                        to={{pathname: `/task_details/${d["id"]}`}}
                                        key={d["id"]}>{d["name"]}
                                    </Link>
                                </ListGroup.Item>
                            ))
                        }
                    </ListGroup>
                </Card>
            </div>
        </div>
    );
}