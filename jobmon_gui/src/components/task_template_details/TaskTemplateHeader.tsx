import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';

type TaskTemplateHeaderProps = {
    taskTemplateId: number | string;
    taskTemplateName: string;
};

export default function TaskTemplateHeader({
    taskTemplateId,
    taskTemplateName,
}: TaskTemplateHeaderProps) {
    return (
        <Box className="App-header">
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <span>
                    <Typography variant="h5" component="span" sx={{ pl: 1 }}>
                        {taskTemplateId}{' '}
                        {taskTemplateName ? `- ${taskTemplateName}` : ''}
                    </Typography>
                </span>
            </Box>
        </Box>
    );
}
