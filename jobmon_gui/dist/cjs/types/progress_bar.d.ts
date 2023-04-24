/// <reference types="react" />
import './jobmon_gui.css';
export default function JobmonProgressBar({ tasks, pending, scheduled, running, done, fatal, num_attempts_avg, num_attempts_min, num_attempts_max, maxc, placement, style }: {
    tasks: any;
    pending: any;
    scheduled: any;
    running: any;
    done: any;
    fatal: any;
    num_attempts_avg: any;
    num_attempts_min: any;
    num_attempts_max: any;
    maxc: any;
    placement: any;
    style?: string | undefined;
}): JSX.Element;
