import Paper from '@mui/material/Paper';
import { ReactNode } from 'react';
import { SxProps } from '@mui/system';
import { Theme } from '@mui/material/styles';

type ScrollableTextAreaProps = {
    children: ReactNode;
    elevation?: number;
    maxheight?: string;

    sx?: SxProps<Theme>;
};

export const ScrollableTextArea = ({
    children,
    elevation = 0,
    maxheight = '50vh',
    sx,
}: ScrollableTextAreaProps) => {
    return (
        <Paper
            elevation={elevation}
            sx={{
                ...sx,
                maxHeight: maxheight,
                overflow: 'auto',
            }}
        >
            {children}
        </Paper>
    );
};

export const ScrollableCodeBlock = ({
    children,
    elevation = 0,
    maxheight = '50vh',
    sx,
}: ScrollableTextAreaProps) => {
    const ScrollableCodeBlockStyles = {
        fontFamily: 'Roboto Mono Variable',
        backgroundColor: '#eee',
        pl: 2,
        pr: 2,
        pt: 2,
        pb: 2,
    };

    return (
        <ScrollableTextArea
            sx={{ ...sx, ...ScrollableCodeBlockStyles }}
            children={children}
            maxheight={maxheight}
            elevation={elevation}
        />
    );
};
export default ScrollableTextArea;
