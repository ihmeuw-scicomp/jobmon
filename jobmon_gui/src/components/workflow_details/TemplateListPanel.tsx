import React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import { TTStatusResponse } from '@jobmon_gui/types/TaskTemplateStatus';
import { TEMPLATE_STATUS_COLORS } from '@jobmon_gui/constants/taskStatus';
import TemplateStatusBar from '@jobmon_gui/components/common/TemplateStatusBar';

const HOVER_BG = '#e3f2fd';

interface TemplateListPanelProps {
    ttData: TTStatusResponse;
    hoveredTemplateName: string | null;
    onTemplateSelect: (name: string) => void;
    onTemplateHover: (name: string | null) => void;
    onPrefetch: (tt: {
        task_template_version_id: string | number;
        name: string;
    }) => void;
}

export default function TemplateListPanel({
    ttData,
    hoveredTemplateName,
    onTemplateSelect,
    onTemplateHover,
    onPrefetch,
}: TemplateListPanelProps) {
    const templates = Object.values(ttData);

    const failingTemplates = templates
        .filter(tt => tt.FATAL > 0)
        .sort((a, b) => b.FATAL - a.FATAL);

    return (
        <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            {/* Needs Attention section */}
            {failingTemplates.length > 0 && (
                <Box sx={{ mb: 2 }}>
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            mb: 1,
                        }}
                    >
                        <Typography variant="subtitle2">
                            Needs Attention
                        </Typography>
                        <Chip
                            label={`${failingTemplates.length}`}
                            size="small"
                            sx={{
                                height: 20,
                                fontSize: '0.75rem',
                                backgroundColor:
                                    TEMPLATE_STATUS_COLORS.FATAL,
                                color: '#fff',
                            }}
                        />
                    </Box>
                    <Box
                        sx={{
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 0.5,
                        }}
                    >
                        {failingTemplates.map(tt => (
                            <Box
                                key={tt.id}
                                onClick={() =>
                                    onTemplateSelect(tt.name)
                                }
                                onMouseEnter={() => {
                                    onTemplateHover(tt.name);
                                    onPrefetch(tt);
                                }}
                                onMouseLeave={() =>
                                    onTemplateHover(null)
                                }
                                sx={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                    p: 1,
                                    borderRadius: 1,
                                    border: '1px solid',
                                    borderColor: 'divider',
                                    cursor: 'pointer',
                                    backgroundColor:
                                        hoveredTemplateName ===
                                        tt.name
                                            ? HOVER_BG
                                            : undefined,
                                    transition:
                                        'background-color 0.15s ease',
                                    '&:hover': {
                                        backgroundColor: HOVER_BG,
                                    },
                                }}
                            >
                                <Typography
                                    variant="body2"
                                    sx={{
                                        fontWeight: 500,
                                        overflow: 'hidden',
                                        textOverflow: 'ellipsis',
                                        whiteSpace: 'nowrap',
                                        flex: 1,
                                        mr: 1,
                                    }}
                                >
                                    {tt.name}
                                </Typography>
                                <Chip
                                    label={`${tt.FATAL} fatal`}
                                    size="small"
                                    sx={{
                                        height: 20,
                                        fontSize: '0.7rem',
                                        fontWeight: 'bold',
                                        backgroundColor:
                                            TEMPLATE_STATUS_COLORS.FATAL,
                                        color: '#fff',
                                        flexShrink: 0,
                                    }}
                                />
                            </Box>
                        ))}
                    </Box>
                </Box>
            )}

            {/* All Templates list with mini status bars */}
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
                All Templates
            </Typography>
            <List sx={{ padding: 0 }}>
                {templates.map(tt => {
                    const isHovered =
                        hoveredTemplateName === tt.name;
                    return (
                        <ListItem key={tt.id} disablePadding>
                            <ListItemButton
                                onClick={() =>
                                    onTemplateSelect(tt.name)
                                }
                                onMouseEnter={() => {
                                    onTemplateHover(tt.name);
                                    onPrefetch(tt);
                                }}
                                onMouseLeave={() =>
                                    onTemplateHover(null)
                                }
                                sx={{
                                    py: 0.75,
                                    px: 1.5,
                                    backgroundColor: isHovered
                                        ? '#e3f2fd !important'
                                        : undefined,
                                    transition:
                                        'background-color 0.15s ease',
                                }}
                            >
                                <Box sx={{ width: '100%' }}>
                                    <Box
                                        sx={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent:
                                                'space-between',
                                            mb: 0.25,
                                        }}
                                    >
                                        <Typography
                                            variant="body2"
                                            sx={{
                                                fontWeight: 500,
                                                overflow: 'hidden',
                                                textOverflow:
                                                    'ellipsis',
                                                whiteSpace: 'nowrap',
                                                flex: 1,
                                                mr: 1,
                                            }}
                                        >
                                            {tt.name}
                                        </Typography>
                                        <Typography
                                            variant="caption"
                                            color="text.secondary"
                                            sx={{ flexShrink: 0 }}
                                        >
                                            {tt.tasks === 0
                                                ? 0
                                                : Math.floor(
                                                      (tt.DONE /
                                                          tt.tasks) *
                                                          100
                                                  )}
                                            %
                                        </Typography>
                                    </Box>
                                    <TemplateStatusBar
                                        counts={tt}
                                        height={4}
                                        borderRadius={0.5}
                                    />
                                </Box>
                            </ListItemButton>
                        </ListItem>
                    );
                })}
            </List>
        </Box>
    );
}
