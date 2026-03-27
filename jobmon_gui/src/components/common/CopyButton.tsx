import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';

interface CopyButtonProps {
    text: string;
    tooltip?: string;
    size?: number;
}

export default function CopyButton({
    text,
    tooltip = 'Copy',
    size = 16,
}: CopyButtonProps) {
    return (
        <Tooltip title={tooltip}>
            <IconButton
                size="small"
                onClick={() =>
                    navigator.clipboard
                        .writeText(text)
                        .catch(() => {})
                }
            >
                <ContentCopyIcon sx={{ fontSize: size }} />
            </IconButton>
        </Tooltip>
    );
}
