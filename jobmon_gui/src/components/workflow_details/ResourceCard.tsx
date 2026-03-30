import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import CircularProgress from '@mui/material/CircularProgress';
import humanizeDuration from 'humanize-duration';

function fmtRuntime(seconds: number | null | undefined): string {
    if (seconds == null) return 'N/A';
    return humanizeDuration(seconds * 1000, {
        largest: 1,
        round: true,
    });
}

function fmtMemory(bytes: number | null | undefined): string {
    if (bytes == null) return 'N/A';
    const gib = bytes / 1073741824;
    return `${gib.toFixed(2)} GiB`;
}

interface ResourceCardProps {
    usageData: {
        median_runtime?: number | null;
        min_runtime?: number | null;
        max_runtime?: number | null;
        median_mem?: number | null;
        min_mem?: number | null;
        max_mem?: number | null;
    } | null;
    usageLoading: boolean;
}

function StatColumn({ label, value }: { label: string; value: string }) {
    return (
        <Grid item xs={12} sm={4}>
            <Typography
                variant="caption"
                color="text.secondary"
                fontWeight="medium"
                sx={{
                    textTransform: 'uppercase',
                    letterSpacing: 0.5,
                    fontSize: '0.6rem',
                }}
            >
                {label}
            </Typography>
            <Typography variant="body2" fontWeight="bold" sx={{ mt: 0.25 }}>
                {value}
            </Typography>
        </Grid>
    );
}

export default function ResourceCard({
    usageData,
    usageLoading,
}: ResourceCardProps) {
    const hasUsage =
        usageData &&
        (usageData.median_runtime != null || usageData.median_mem != null);

    if (usageLoading) {
        return (
            <Box
                sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                    p: 1,
                }}
            >
                <CircularProgress size={16} />
                <Typography variant="caption" color="text.secondary">
                    Loading resources...
                </Typography>
            </Box>
        );
    }

    if (!hasUsage) return null;

    return (
        <Box
            sx={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: 1,
            }}
        >
            {/* Runtime card */}
            {usageData!.median_runtime != null && (
                <Card
                    elevation={0}
                    sx={{
                        flex: '1 1 0',
                        minWidth: 0,
                        border: '1px solid',
                        borderColor: 'divider',
                        borderLeft: '3px solid',
                        borderLeftColor: 'primary.main',
                        borderRadius: 2,
                    }}
                >
                    <CardContent
                        sx={{
                            p: 1.5,
                            '&:last-child': { pb: 1.5 },
                        }}
                    >
                        <Typography
                            variant="caption"
                            color="primary.main"
                            fontWeight="bold"
                            sx={{
                                textTransform: 'uppercase',
                                letterSpacing: 0.5,
                                mb: 0.5,
                                display: 'block',
                            }}
                        >
                            Runtime
                        </Typography>
                        <Grid container spacing={0.5}>
                            <StatColumn
                                label="Minimum"
                                value={fmtRuntime(usageData!.min_runtime)}
                            />
                            <StatColumn
                                label="Maximum"
                                value={fmtRuntime(usageData!.max_runtime)}
                            />
                            <StatColumn
                                label="Median"
                                value={fmtRuntime(usageData!.median_runtime)}
                            />
                        </Grid>
                    </CardContent>
                </Card>
            )}

            {/* Memory card */}
            {usageData!.median_mem != null && (
                <Card
                    elevation={0}
                    sx={{
                        flex: '1 1 0',
                        minWidth: 0,
                        border: '1px solid',
                        borderColor: 'divider',
                        borderLeft: '3px solid',
                        borderLeftColor: 'secondary.main',
                        borderRadius: 2,
                    }}
                >
                    <CardContent
                        sx={{
                            p: 1.5,
                            '&:last-child': { pb: 1.5 },
                        }}
                    >
                        <Typography
                            variant="caption"
                            color="secondary.main"
                            fontWeight="bold"
                            sx={{
                                textTransform: 'uppercase',
                                letterSpacing: 0.5,
                                mb: 0.5,
                                display: 'block',
                            }}
                        >
                            Memory
                        </Typography>
                        <Grid container spacing={0.5}>
                            <StatColumn
                                label="Minimum"
                                value={fmtMemory(usageData!.min_mem)}
                            />
                            <StatColumn
                                label="Maximum"
                                value={fmtMemory(usageData!.max_mem)}
                            />
                            <StatColumn
                                label="Median"
                                value={fmtMemory(usageData!.median_mem)}
                            />
                        </Grid>
                    </CardContent>
                </Card>
            )}
        </Box>
    );
}
