import React from 'react';
import Box from '@mui/material/Box';
import { SxProps, Theme } from '@mui/material/styles';

import { formatResourceLabel } from '@jobmon_gui/utils/requestedResources';

interface Props {
    rows: { key: string; value: string }[];
    labelSx?: SxProps<Theme>;
    valueSx?: SxProps<Theme>;
    sx?: SxProps<Theme>;
    emptyText?: string;
}

/** Two-column (label: value) grid used by the workflow Resources card
 *  and the Task Template Details task-table tooltip. Labels come from
 *  ``formatResourceLabel`` so casing and overrides stay consistent. */
export default function RequestedResourcesGrid({
    rows,
    labelSx,
    valueSx,
    sx,
    emptyText = 'No resource fields set.',
}: Props) {
    if (rows.length === 0) {
        return (
            <Box sx={{ fontSize: '0.8rem', color: 'text.secondary', ...sx }}>
                {emptyText}
            </Box>
        );
    }
    return (
        <Box
            sx={{
                display: 'grid',
                gridTemplateColumns: 'max-content 1fr',
                gap: '2px 8px',
                fontFamily: 'monospace',
                fontSize: '0.8rem',
                ...sx,
            }}
        >
            {rows.map(({ key, value }) => (
                <React.Fragment key={key}>
                    <Box
                        sx={{
                            color: 'text.secondary',
                            textAlign: 'right',
                            whiteSpace: 'nowrap',
                            ...labelSx,
                        }}
                    >
                        {formatResourceLabel(key)}:
                    </Box>
                    <Box sx={{ wordBreak: 'break-all', ...valueSx }}>
                        {value}
                    </Box>
                </React.Fragment>
            ))}
        </Box>
    );
}
