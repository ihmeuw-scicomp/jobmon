import React from 'react';
import {
    Box,
    Button,
    Card,
    CardContent,
    Skeleton,
    Typography,
} from '@mui/material';
import { ErrorOutline as ErrorOutlineIcon } from '@mui/icons-material';

interface ErrorSummaryCardProps {
    errorClusterCount: number;
    totalFailures: number;
    topErrorPreview?: string;
    isLoading: boolean;
    onViewDetails: () => void;
}

const ErrorSummaryCard: React.FC<ErrorSummaryCardProps> = ({
    errorClusterCount,
    totalFailures,
    topErrorPreview,
    isLoading,
    onViewDetails,
}) => {
    if (isLoading) {
        return <Skeleton variant="rectangular" height={200} />;
    }

    const hasErrors = errorClusterCount > 0 || totalFailures > 0;

    return (
        <Card
            elevation={0}
            sx={{
                height: '100%',
                border: '1px solid',
                borderColor: hasErrors
                    ? 'error.light'
                    : 'divider',
                borderRadius: 2,
                transition: 'all 0.2s ease-in-out',
                '&:hover': {
                    boxShadow: 3,
                    borderColor: hasErrors
                        ? 'error.main'
                        : 'divider',
                },
            }}
        >
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Box display="flex" alignItems="center" mb={1.5}>
                    <Box
                        sx={{
                            bgcolor: hasErrors
                                ? 'error.50'
                                : 'grey.100',
                            color: hasErrors
                                ? 'error.main'
                                : 'text.secondary',
                            p: 0.75,
                            borderRadius: 1,
                            mr: 1.5,
                        }}
                    >
                        <ErrorOutlineIcon sx={{ fontSize: 18 }} />
                    </Box>
                    <Typography
                        variant="subtitle1"
                        component="div"
                        fontWeight="bold"
                    >
                        Error Summary
                    </Typography>
                </Box>

                <Box
                    display="flex"
                    gap={2}
                    mb={1.5}
                >
                    <Box>
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            fontWeight="medium"
                            sx={{
                                textTransform: 'uppercase',
                                letterSpacing: 0.5,
                            }}
                        >
                            Error Clusters
                        </Typography>
                        <Typography
                            variant="h6"
                            fontWeight="bold"
                            color={
                                hasErrors
                                    ? 'error.main'
                                    : 'text.primary'
                            }
                            sx={{ mt: 0.5 }}
                        >
                            {errorClusterCount}
                        </Typography>
                    </Box>
                    <Box>
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            fontWeight="medium"
                            sx={{
                                textTransform: 'uppercase',
                                letterSpacing: 0.5,
                            }}
                        >
                            Total Failures
                        </Typography>
                        <Typography
                            variant="h6"
                            fontWeight="bold"
                            color={
                                hasErrors
                                    ? 'error.main'
                                    : 'text.primary'
                            }
                            sx={{ mt: 0.5 }}
                        >
                            {totalFailures}
                        </Typography>
                    </Box>
                </Box>

                {topErrorPreview && (
                    <Box
                        sx={{
                            bgcolor: 'grey.50',
                            p: 1,
                            borderRadius: 1,
                            border: '1px solid',
                            borderColor: 'grey.200',
                            mb: 1.5,
                        }}
                    >
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            fontWeight="medium"
                            sx={{
                                textTransform: 'uppercase',
                                letterSpacing: 0.5,
                                display: 'block',
                                mb: 0.25,
                            }}
                        >
                            Top Error
                        </Typography>
                        <Typography
                            variant="caption"
                            color="text.secondary"
                            sx={{
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                display: 'block',
                            }}
                        >
                            {topErrorPreview}
                        </Typography>
                    </Box>
                )}

                {hasErrors && (
                    <Button
                        variant="outlined"
                        size="small"
                        color="error"
                        onClick={onViewDetails}
                        fullWidth
                    >
                        View Details
                    </Button>
                )}

                {!hasErrors && (
                    <Box
                        sx={{
                            bgcolor: 'success.50',
                            p: 1,
                            borderRadius: 1,
                            border: '1px solid',
                            borderColor: 'success.200',
                        }}
                    >
                        <Typography
                            variant="body2"
                            color="success.main"
                            fontWeight="medium"
                        >
                            No errors detected
                        </Typography>
                    </Box>
                )}
            </CardContent>
        </Card>
    );
};

export default ErrorSummaryCard;
