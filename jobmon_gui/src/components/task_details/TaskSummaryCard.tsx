import React, { useState } from 'react';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Chip from '@mui/material/Chip';
import Collapse from '@mui/material/Collapse';
import LinearProgress from '@mui/material/LinearProgress';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import { BiRun } from 'react-icons/bi';
import { IoMdCloseCircle, IoMdCloseCircleOutline } from 'react-icons/io';
import {
    AiFillSchedule,
    AiFillCheckCircle,
} from 'react-icons/ai';
import { HiRocketLaunch } from 'react-icons/hi2';
import { TaskDetails } from '@jobmon_gui/types/TaskDetails';
import {
    getStatusColor,
    getStatusLabel,
    getStatusTextColor,
    ERROR_STATUSES,
} from '@jobmon_gui/constants/taskStatus';
import { formatJobmonDate } from '@jobmon_gui/utils/DayTime';
import { ScrollableCodeBlock } from
    '@jobmon_gui/components/ScrollableTextArea';

const STATUS_ICONS: Record<string, React.ReactNode> = {
    G: <AiFillSchedule />,
    Q: <AiFillSchedule />,
    I: <AiFillSchedule />,
    O: <HiRocketLaunch />,
    R: <BiRun />,
    D: <AiFillCheckCircle />,
    A: <IoMdCloseCircleOutline />,
    F: <IoMdCloseCircle />,
    E: <IoMdCloseCircle />,
};

interface TaskSummaryCardProps {
    taskDetails: TaskDetails;
    taskId: string | number | undefined;
}

export default function TaskSummaryCard({
    taskDetails,
    taskId: _taskId,
}: TaskSummaryCardProps) {
    const [commandOpen, setCommandOpen] = useState(false);

    const status = taskDetails.task_status;
    const statusColor = getStatusColor(status);
    const statusTextColor = getStatusTextColor(status);
    const statusLabel = getStatusLabel(status);
    const statusIcon = STATUS_ICONS[status] || null;

    const showAttempts =
        taskDetails.max_attempts > 1 ||
        (ERROR_STATUSES as readonly string[]).includes(status);
    const attemptProgress =
        taskDetails.max_attempts > 0
            ? (taskDetails.num_attempts / taskDetails.max_attempts) *
              100
            : 0;

    return (
        <Card
            elevation={0}
            sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderLeft: `4px solid ${statusColor}`,
                borderRadius: 2,
                mb: 2,
                '&:hover': {
                    boxShadow: 2,
                },
            }}
        >
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                {/* Row 1: Icon + Name + Status Chip */}
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 1,
                        mb: 1,
                    }}
                >
                    {statusIcon && (
                        <Box
                            sx={{
                                color: statusColor,
                                fontSize: '1.4rem',
                                display: 'flex',
                                alignItems: 'center',
                            }}
                        >
                            {statusIcon}
                        </Box>
                    )}
                    <Typography
                        variant="h6"
                        sx={{
                            fontWeight: 600,
                            flex: 1,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                        }}
                    >
                        {taskDetails.task_name}
                    </Typography>
                    <Chip
                        label={statusLabel}
                        size="small"
                        sx={{
                            backgroundColor: statusColor,
                            color: statusTextColor,
                            fontWeight: 600,
                        }}
                    />
                </Box>

                {/* Row 2: Metadata line */}
                <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mb: 1 }}
                >
                    Updated:{' '}
                    {formatJobmonDate(taskDetails.task_status_date)}
                </Typography>

                {/* Row 3: Attempts progress (conditional) */}
                {showAttempts && (
                    <Box sx={{ mb: 1 }}>
                        <Box
                            sx={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                mb: 0.5,
                            }}
                        >
                            <Typography
                                variant="caption"
                                color="text.secondary"
                                sx={{
                                    fontWeight: 600,
                                    textTransform: 'uppercase',
                                    letterSpacing: 0.5,
                                }}
                            >
                                Attempts
                            </Typography>
                            <Typography
                                variant="caption"
                                color="text.secondary"
                            >
                                {taskDetails.num_attempts} of{' '}
                                {taskDetails.max_attempts}
                            </Typography>
                        </Box>
                        <LinearProgress
                            variant="determinate"
                            value={attemptProgress}
                            sx={{
                                height: 6,
                                borderRadius: 3,
                                backgroundColor: 'action.hover',
                                '& .MuiLinearProgress-bar': {
                                    backgroundColor: statusColor,
                                    borderRadius: 3,
                                },
                            }}
                        />
                    </Box>
                )}

                {/* Row 4: Collapsible command */}
                <Box
                    sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.5,
                    }}
                >
                    <Typography
                        variant="caption"
                        color="text.secondary"
                        sx={{
                            fontWeight: 600,
                            textTransform: 'uppercase',
                            letterSpacing: 0.5,
                        }}
                    >
                        Command
                    </Typography>
                    <Tooltip
                        title={
                            commandOpen
                                ? 'Hide command'
                                : 'Show command'
                        }
                    >
                        <IconButton
                            size="small"
                            onClick={() =>
                                setCommandOpen(!commandOpen)
                            }
                        >
                            {commandOpen ? (
                                <ExpandLessIcon fontSize="small" />
                            ) : (
                                <ExpandMoreIcon fontSize="small" />
                            )}
                        </IconButton>
                    </Tooltip>
                </Box>
                <Collapse in={commandOpen}>
                    <ScrollableCodeBlock maxheight="150px">
                        {taskDetails.task_command}
                    </ScrollableCodeBlock>
                </Collapse>
            </CardContent>
        </Card>
    );
}
