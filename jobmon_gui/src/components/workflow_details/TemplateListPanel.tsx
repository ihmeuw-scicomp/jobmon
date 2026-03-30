import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import Tooltip from '@mui/material/Tooltip';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import MemoryIcon from '@mui/icons-material/Memory';
import { TTStatus, TTStatusResponse } from '@jobmon_gui/types/TaskTemplateStatus';
import {
    TEMPLATE_STATUS_COLORS,
    RESOURCE_ERROR_COLORS,
} from '@jobmon_gui/constants/taskStatus';
import TemplateStatusBar from '@jobmon_gui/components/common/TemplateStatusBar';

const HOVER_BG = '#e3f2fd';

interface TemplateListPanelProps {
    ttData: TTStatusResponse;
    hoveredTemplateName: string | null;
    onTemplateSelect: (name: string) => void;
    onTemplateHover: (name: string | null) => void;
    onPrefetch: (tt: {
        id: string | number;
        task_template_version_id: string | number;
        name: string;
    }) => void;
}

function TemplateRow({
    tt,
    isHovered,
    onSelect,
    onHover,
    onPrefetch,
}: {
    tt: TTStatus;
    isHovered: boolean;
    onSelect: () => void;
    onHover: (name: string | null) => void;
    onPrefetch: () => void;
}) {
    const pct =
        tt.tasks === 0
            ? 0
            : Math.floor((tt.DONE / tt.tasks) * 100);

    return (
        <ListItem disablePadding>
            <ListItemButton
                onClick={onSelect}
                onMouseEnter={() => {
                    onHover(tt.name);
                    onPrefetch();
                }}
                onMouseLeave={() => onHover(null)}
                sx={{
                    py: 0.75,
                    px: 1.5,
                    backgroundColor: isHovered
                        ? `${HOVER_BG} !important`
                        : undefined,
                    transition: 'background-color 0.15s ease',
                }}
            >
                <Box sx={{ width: '100%' }}>
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            mb: 0.25,
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
                                minWidth: 0,
                            }}
                        >
                            {tt.name}
                        </Typography>
                        {(tt.resource_error_count > 0 ||
                            tt.FATAL > 0) && (
                            <Box
                                sx={{
                                    display: 'flex',
                                    gap: 0.5,
                                    flexShrink: 0,
                                }}
                            >
                                {tt.resource_error_count > 0 && (
                                    <Tooltip
                                        title={`${tt.resource_error_count} task instance${tt.resource_error_count > 1 ? 's' : ''} hit resource limits (memory or runtime exceeded)`}
                                        arrow
                                    >
                                        <Chip
                                            icon={
                                                <MemoryIcon
                                                    sx={{
                                                        fontSize:
                                                            '10px !important',
                                                        color: '#fff !important',
                                                    }}
                                                />
                                            }
                                            label={`${tt.resource_error_count} resource`}
                                            size="small"
                                            sx={{
                                                height: 18,
                                                fontSize: '0.6rem',
                                                fontWeight: 'bold',
                                                backgroundColor:
                                                    RESOURCE_ERROR_COLORS.main,
                                                color: '#fff',
                                                '& .MuiChip-icon':
                                                    {
                                                        ml: 0.5,
                                                        mr: -0.25,
                                                    },
                                            }}
                                        />
                                    </Tooltip>
                                )}
                                {tt.FATAL > 0 && (
                                    <Chip
                                        label={`${tt.FATAL} fatal`}
                                        size="small"
                                        sx={{
                                            height: 18,
                                            fontSize: '0.6rem',
                                            fontWeight: 'bold',
                                            backgroundColor:
                                                TEMPLATE_STATUS_COLORS.FATAL,
                                            color: '#fff',
                                        }}
                                    />
                                )}
                            </Box>
                        )}
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            sx={{ flexShrink: 0 }}
                        >
                            {pct}%
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
}

export default function TemplateListPanel({
    ttData,
    hoveredTemplateName,
    onTemplateSelect,
    onTemplateHover,
    onPrefetch,
}: TemplateListPanelProps) {
    const templates = Object.values(ttData);

    const needsAttention = templates
        .filter(tt => tt.FATAL > 0)
        .sort((a, b) => b.FATAL - a.FATAL);

    const needsAttentionIds = new Set(
        needsAttention.map(tt => tt.id)
    );

    const otherTemplates = templates.filter(
        tt => !needsAttentionIds.has(tt.id)
    );

    return (
        <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            {/* Needs Attention section */}
            {needsAttention.length > 0 && (
                <Box sx={{ mb: 2 }}>
                    <Box
                        sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            mb: 0.5,
                        }}
                    >
                        <Typography variant="subtitle2">
                            Needs Attention
                        </Typography>
                        <Chip
                            label={`${needsAttention.length}`}
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
                    <List sx={{ padding: 0 }}>
                        {needsAttention.map(tt => (
                            <TemplateRow
                                key={tt.id}
                                tt={tt}
                                isHovered={
                                    hoveredTemplateName ===
                                    tt.name
                                }
                                onSelect={() =>
                                    onTemplateSelect(tt.name)
                                }
                                onHover={onTemplateHover}
                                onPrefetch={() =>
                                    onPrefetch(tt)
                                }
                            />
                        ))}
                    </List>
                </Box>
            )}

            {/* Remaining templates */}
            <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                {needsAttention.length > 0
                    ? 'Other Templates'
                    : 'All Templates'}
            </Typography>
            <List sx={{ padding: 0 }}>
                {otherTemplates.map(tt => (
                    <TemplateRow
                        key={tt.id}
                        tt={tt}
                        isHovered={
                            hoveredTemplateName === tt.name
                        }
                        onSelect={() =>
                            onTemplateSelect(tt.name)
                        }
                        onHover={onTemplateHover}
                        onPrefetch={() => onPrefetch(tt)}
                    />
                ))}
            </List>
        </Box>
    );
}
