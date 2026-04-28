import { useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import Chip from '@mui/material/Chip';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';

import {
    formatRequestedResourcesFull,
    formatRequestedResourcesSummary,
} from '@jobmon_gui/utils/requestedResources';
import RequestedResourcesGrid from '@jobmon_gui/components/common/RequestedResourcesGrid';
import { components } from '@jobmon_gui/types/apiSchema';

export type RequestedResourceCluster =
    components['schemas']['RequestedResourceClusterItem'];

export default function RequestedResourceClusterCard({
    cluster,
    defaultOpen = false,
}: {
    cluster: RequestedResourceCluster;
    defaultOpen?: boolean;
}) {
    const [open, setOpen] = useState(defaultOpen);
    const blob = cluster.requested_resources ?? {};
    const summary = formatRequestedResourcesSummary(blob);
    const fullRows = open ? formatRequestedResourcesFull(blob) : [];

    return (
        <Box
            sx={{
                border: '1px solid #e0e0e0',
                borderRadius: 1,
                p: 1,
                mb: 1,
            }}
        >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Chip
                    label={`${cluster.num_tasks} task${
                        cluster.num_tasks === 1 ? '' : 's'
                    }`}
                    size="small"
                    color="primary"
                    variant="outlined"
                />
                <Typography
                    variant="body2"
                    sx={{
                        fontFamily: 'monospace',
                        flex: 1,
                        minWidth: 0,
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                    }}
                >
                    {summary || '(empty)'}
                </Typography>
                <Tooltip title={open ? 'Hide full blob' : 'Show full blob'}>
                    <IconButton size="small" onClick={() => setOpen(v => !v)}>
                        {open ? (
                            <ExpandLessIcon fontSize="small" />
                        ) : (
                            <ExpandMoreIcon fontSize="small" />
                        )}
                    </IconButton>
                </Tooltip>
            </Box>
            {open && (
                <RequestedResourcesGrid
                    rows={fullRows}
                    sx={{ mt: 1 }}
                />
            )}
        </Box>
    );
}
