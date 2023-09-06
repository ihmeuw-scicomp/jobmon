import React from 'react';
import ProgressBar from 'react-bootstrap/ProgressBar';
import { OverlayTrigger } from "react-bootstrap";
import Popover from 'react-bootstrap/Popover';

import './jobmon_gui.css';

export default function JobmonProgressBar({tasks, pending, scheduled, running, done, fatal, num_attempts_avg, num_attempts_min, num_attempts_max, maxc, placement, style="striped"}) {
    num_attempts_avg = parseFloat(num_attempts_avg).toFixed(1);
    // style can be striped or animated; others will be treated as default
    // FIXME: reduce code duplication through better use of variables with flow control
    const INT_32_MAX = 2147483647
    if (maxc === INT_32_MAX) {
        maxc = "No Limit"
    }
    if (style === "striped") {
        return (
            <OverlayTrigger
                            placement={placement}
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    <table id="tt-tasks">
                                        <tr>
                                            <th className="scheduled">Scheduled:</th>
                                            <td>{scheduled}</td>
                                        </tr>
                                        <tr>
                                            <th className="pending"> Pending:</th>
                                            <td>{pending}</td>
                                        </tr>
                                        <tr>
                                            <th className="running">Running:</th>
                                            <td>{running}</td>
                                        </tr>
                                        <tr>
                                            <th className="done">Done:</th>
                                            <td>{done}</td>
                                        </tr>
                                        <tr>
                                            <th className="fatal">Fatal:</th>
                                            <td>{fatal}</td>
                                        </tr>
                                        <tr>
                                            <th className='bg-dark text-light'> Total:</th>
                                            <td>{tasks}</td>
                                        </tr>
                                    </table>
                                    <hr />
                                    <table id="tt-stats">
                                        <tr>
                                            <th># Attempts:</th>
                                            <td>{num_attempts_avg} ({num_attempts_min} - {num_attempts_max})</td>
                                        </tr>
                                        <tr>
                                            <th>Concurrency Limit:</th>
                                            <td>{maxc.toLocaleString()}</td>
                                        </tr>
                                    </table>
                                </Popover>
                            )}
                        >

            <ProgressBar>
                 <ProgressBar className="pending-progress-bar" max={tasks} now={pending} key={1} isChild={true} label={((pending / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="scheduled-progress-bar" max={tasks} now={scheduled} key={2} isChild={true} label={((scheduled / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="running-progress-bar" max={tasks} now={running} key={3} isChild={true} label={((running / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="done-progress-bar" max={tasks} now={done} key={4} isChild={true} label={((done / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="fatal-progress-bar" max={tasks} now={fatal} key={5} isChild={true} label={((fatal / tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
            </OverlayTrigger>

        );
    }else if(style === "animated" ){
        return (
            <OverlayTrigger
                            placement={placement}
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task_count">
                                    <table id="tt-tasks">
                                        <tr>
                                            <th className="scheduled">Scheduled:</th>
                                            <td>{scheduled}</td>
                                        </tr>
                                        <tr>
                                            <th className="pending"> Pending:</th>
                                            <td>{pending}</td>
                                        </tr>
                                        <tr>
                                            <th className="running">Running:</th>
                                            <td>{running}</td>
                                        </tr>
                                        <tr>
                                            <th className="done">Done:</th>
                                            <td>{done}</td>
                                        </tr>
                                        <tr>
                                            <th className="fatal">Fatal:</th>
                                            <td>{fatal}</td>
                                        </tr>
                                        <tr>
                                            <th> Total:</th>
                                            <td>{tasks}</td>
                                        </tr>
                                    </table>
                                    <hr />
                                    <table id="tt-stats">
                                        <tr>
                                            <th># Attempts:</th>
                                            <td>{num_attempts_avg} ({num_attempts_min} - {num_attempts_max})</td>
                                        </tr>
                                        <tr>
                                            <th>Concurrency Limit:</th>
                                            <td>{maxc.toLocaleString()}</td>
                                        </tr>
                                    </table>
                                </Popover>
                            )}
                        >

            <ProgressBar>
                 <ProgressBar className="pending-progress-bar" animated max={tasks} now={pending} key={1} isChild={true} label={((pending / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="scheduled-progress-bar" animated max={tasks} now={scheduled} key={2} isChild={true} label={((scheduled / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="running-progress-bar" animated max={tasks} now={running} key={3} isChild={true} label={((running / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="done-progress-bar" animated max={tasks} now={done} key={4} isChild={true} label={((done / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="fatal-progress-bar" animated max={tasks} now={fatal} key={5} isChild={true} label={((fatal / tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
            </OverlayTrigger>

        );
    }else{
        return (
            <OverlayTrigger
                            placement={placement}
                            trigger={["hover", "focus"]}
                            overlay={(
                                <Popover id="task-count">
                                    <table id="tt-tasks">
                                        <tr>
                                            <th className="scheduled text-light px-2 rounded">Scheduled:</th>
                                            <td className='pl-2'>{scheduled}</td>
                                        </tr>
                                        <tr>
                                            <th className="pending text-light px-2 rounded"> Pending:</th>
                                            <td>{pending}</td>
                                        </tr>
                                        <tr>
                                            <th className="running text-light px-2 rounded">Running:</th>
                                            <td>{running}</td>
                                        </tr>
                                        <tr>
                                            <th className="done text-light px-2 rounded">Done:</th>
                                            <td>{done}</td>
                                        </tr>
                                        <tr>
                                            <th className="fatal text-light px-2 rounded">Fatal:</th>
                                            <td>{fatal}</td>
                                        </tr>
                                        <tr>
                                            <th className='bg-dark text-light px-2 rounded'> Total:</th>
                                            <td className='pl-2'>{tasks}</td>
                                        </tr>
                                    </table>
                                    <hr />
                                    <table id="tt-stats">
                                        <tr>
                                            <th className='font-weight-bold'># Attempts:</th>
                                            <td>{num_attempts_avg} ({num_attempts_min} - {num_attempts_max})</td>
                                        </tr>
                                        <tr>
                                            <th className='font-weight-bold'>Concurrency Limit:</th>
                                            <td>{maxc.toLocaleString()}</td>
                                        </tr>
                                    </table>
                                </Popover>
                            )}
                        >

            <ProgressBar>
                 <ProgressBar className="pending-progress-bar" max={tasks} now={pending} key={1} isChild={true} label={((pending / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="scheduled-progress-bar" max={tasks} now={scheduled} key={2} isChild={true} label={((scheduled / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="running-progress-bar" max={tasks} now={running} key={3} isChild={true} label={((running / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="done-progress-bar" max={tasks} now={done} key={4} isChild={true} label={((done / tasks) * 100).toFixed(1) + "%"} />
                 <ProgressBar className="fatal-progress-bar" max={tasks} now={fatal} key={5} isChild={true} label={((fatal / tasks) * 100).toFixed(1) + "%"}/>
            </ProgressBar>
            </OverlayTrigger>

        );
    }
}
