import { useMemo } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import CircularProgress from '@mui/material/CircularProgress';
import Divider from '@mui/material/Divider';
import Chip from '@mui/material/Chip';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { useQuery } from '@tanstack/react-query';

import {
    getWorkflowRequestedResourcesQueryFn,
    WorkflowRequestedResourcesQueryKey,
    WorkflowRequestedResourcesResponse,
} from '@jobmon_gui/queries/GetWorkflowRequestedResources';
import RequestedResourceClusterCard from '@jobmon_gui/components/common/RequestedResourceClusterCard';

interface WorkflowResourcesPanelProps {
    workflowId: string | number;
    onBack: () => void;
}

type Cluster = WorkflowRequestedResourcesResponse['clusters'][number];
type TemplateGroup = { id: number; name: string; clusters: Cluster[] };

function Header({ onBack }: { onBack: () => void }) {
    return (
        <>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <Tooltip title="Back">
                    <IconButton size="small" onClick={onBack} sx={{ mr: 1 }}>
                        <ArrowBackIcon fontSize="small" />
                    </IconButton>
                </Tooltip>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                    Requested Resources
                </Typography>
            </Box>
            <Divider sx={{ mb: 2 }} />
        </>
    );
}

export default function WorkflowResourcesPanel({
    workflowId,
    onBack,
}: WorkflowResourcesPanelProps) {
    const { data, isLoading, isError } = useQuery({
        queryKey: [
            'workflow_requested_resources',
            workflowId,
        ] as WorkflowRequestedResourcesQueryKey,
        queryFn: getWorkflowRequestedResourcesQueryFn,
        staleTime: 60_000,
    });

    const templateGroups = useMemo<TemplateGroup[]>(() => {
        const byId = new Map<number, TemplateGroup>();
        for (const c of data?.clusters ?? []) {
            let group = byId.get(c.task_template_id);
            if (!group) {
                group = {
                    id: c.task_template_id,
                    name: c.task_template_name,
                    clusters: [],
                };
                byId.set(c.task_template_id, group);
            }
            group.clusters.push(c);
        }
        return Array.from(byId.values());
    }, [data]);

    return (
        <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            <Header onBack={onBack} />
            {isLoading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                    <CircularProgress size={28} />
                </Box>
            ) : isError ? (
                <Typography color="error">
                    Error loading requested resources.
                </Typography>
            ) : templateGroups.length === 0 ? (
                <Typography color="text.secondary" variant="body2">
                    No bound tasks yet — the workflow must be bound
                    (<code>workflow.bind()</code>) before requested
                    resources are visible.
                </Typography>
            ) : (
                templateGroups.map(group => (
                    <Box key={group.id} sx={{ mb: 2 }}>
                        <Typography
                            variant="subtitle2"
                            sx={{
                                fontWeight: 600,
                                mb: 0.5,
                                display: 'flex',
                                alignItems: 'center',
                                gap: 1,
                            }}
                        >
                            {group.name}
                            <Chip
                                size="small"
                                variant="outlined"
                                label={`${group.clusters.length} cluster${
                                    group.clusters.length === 1 ? '' : 's'
                                }`}
                            />
                        </Typography>
                        {group.clusters.map(c => (
                            <RequestedResourceClusterCard
                                key={`${c.task_template_version_id}-${c.task_resources_id}`}
                                cluster={c}
                                defaultOpen
                            />
                        ))}
                    </Box>
                ))
            )}
        </Box>
    );
}
