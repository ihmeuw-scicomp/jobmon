import React from 'react';
import Popover from 'react-bootstrap/Popover';

type TaskTemplatePopoverProps = {
    data: {
        SCHEDULED: number;
        PENDING: number;
        RUNNING: number;
        DONE: number;
        FATAL: number;
        tasks: number;
        num_attempts_avg: number;
        num_attempts_min: number;
        num_attempts_max: number;
        MAXC: number;
    };
    placement?: React.ComponentProps<typeof Popover>['placement'];
    ref?: React.Ref<HTMLElement>;
    style?: React.CSSProperties;
};

const INT_32_MAX = 2147483647;

const TaskTemplatePopover = React.forwardRef<
    HTMLDivElement,
    TaskTemplatePopoverProps
>(({ data, placement, ...props }, ref) => {
    const num_attempts_avg = parseFloat(
        data.num_attempts_avg.toString()
    ).toFixed(1);
    const maxc = data.MAXC === INT_32_MAX ? 'No Limit' : data.MAXC;

    return (
        <Popover id="task_count" ref={ref} placement={placement} {...props}>
            <table id="tt-tasks">
                <tbody>
                    <tr>
                        <th className="scheduled">Scheduled:</th>
                        <td>{data.SCHEDULED}</td>
                    </tr>
                    <tr>
                        <th className="pending">Pending:</th>
                        <td>{data.PENDING}</td>
                    </tr>
                    <tr>
                        <th className="running">Running:</th>
                        <td>{data.RUNNING}</td>
                    </tr>
                    <tr>
                        <th className="done">Done:</th>
                        <td>{data.DONE}</td>
                    </tr>
                    <tr>
                        <th className="fatal">Fatal:</th>
                        <td>{data.FATAL}</td>
                    </tr>
                    <tr>
                        <th className="bg-dark text-light">Total:</th>
                        <td>{data.tasks}</td>
                    </tr>
                </tbody>
            </table>
            <hr />
            <table id="tt-stats">
                <tbody>
                    <tr>
                        <th># Attempts:</th>
                        <td>
                            {num_attempts_avg} ({data.num_attempts_min} -{' '}
                            {data.num_attempts_max})
                        </td>
                    </tr>
                    <tr>
                        <th>Concurrency Limit:</th>
                        <td>{maxc.toLocaleString()}</td>
                    </tr>
                </tbody>
            </table>
        </Popover>
    );
});

export default TaskTemplatePopover;
