import '@jobmon_gui/styles/jobmon_gui.css';
import React from 'react';
import { BiRun } from 'react-icons/bi';
import { IoMdCloseCircle, IoMdCloseCircleOutline } from 'react-icons/io';
import { AiFillSchedule, AiFillCheckCircle } from 'react-icons/ai';
import { TbHandStop } from 'react-icons/tb';
import { HiRocketLaunch } from 'react-icons/hi2';
import { Box } from '@mui/system';
import { WorkflowDetails } from '@jobmon_gui/types/WorkflowDetails.ts';

type WorkflowHeaderProps = {
    wf_id: number | string;
    wfDetails?: WorkflowDetails;
};

const statusIcons: Record<string, { icon: React.ReactNode; className: string }> = {
    A: { icon: <IoMdCloseCircleOutline />, className: 'icon-aa' },
    D: { icon: <AiFillCheckCircle />, className: 'icon-dd' },
    F: { icon: <IoMdCloseCircle />, className: 'icon-ff' },
    G: { icon: <AiFillSchedule />, className: 'icon-pp' },
    H: { icon: <TbHandStop />, className: 'icon-aa' },
    I: { icon: <AiFillSchedule />, className: 'icon-pp' },
    O: { icon: <HiRocketLaunch />, className: 'icon-ss' },
    Q: { icon: <AiFillSchedule />, className: 'icon-pp' },
    R: { icon: <BiRun />, className: 'icon-rr' },
};

export default function WorkflowHeader({ wf_id: _wf_id, wfDetails }: WorkflowHeaderProps) {
    if (!wfDetails) return null;

    const { icon, className } = statusIcons[wfDetails.wf_status] || {};
    if (!icon) return null;

    return (
        <Box sx={{ display: 'inline-flex', alignItems: 'center' }}>
            <span
                className={className}
                style={{ display: 'flex', alignItems: 'center', fontSize: '1.1rem' }}
            >
                {icon}
            </span>
        </Box>
    );
}
