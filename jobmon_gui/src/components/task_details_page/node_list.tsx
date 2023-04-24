import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import { Link } from "react-router-dom";
import Card from 'react-bootstrap/Card';
import ListGroup from 'react-bootstrap/ListGroup';


export default function NodeLists({ upstreamTasks, downstreamTasks }) {
    return (
        <div className='div-level-2 pl-5 pt-2'>
            <p className='font-weight-bold'>Dependencies</p>
            <div className="card-columns d-flex justify-content-center flex-column">
                <Card className="dependency-list-scroll">
                    <Card.Header className="dependency-header">Upstream Task IDs</Card.Header>
                    <ListGroup variant="flush">
                        {
                            upstreamTasks.flat(1).map(d => (
                                <ListGroup.Item className="dependency-list-group-item">
                                    <Link
                                        to={{ pathname: `/task_details/${d["id"]}` }}
                                        key={d["id"]}>{d["id"]}
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
                            downstreamTasks.flat(1).map(d => (
                                <ListGroup.Item className="dependency-list-group-item">
                                    <Link
                                        to={{ pathname: `/task_details/${d["id"]}` }}
                                        key={d["id"]}>{d["id"]}
                                    </Link>
                                </ListGroup.Item>
                            ))
                        }
                    </ListGroup>
                </Card>
            </div>
        </div >
    );
}