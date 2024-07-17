import Paper from "@mui/material/Paper";
import {ReactNode} from "react";
import {SxProps} from "@mui/system";
import {Theme} from "@mui/material/styles";

type ScrollableTextAreaProps = {
    children: ReactNode
    elevation?: number
    maxheight?: string

    sx?: SxProps<Theme>
}

export const ScrollableTextArea = ({
                                       children,
                                       elevation = 0,
                                       maxheight = '50vh',
                                       sx
                                   }: ScrollableTextAreaProps) => {
    return (
        <Paper elevation={elevation} sx={{...sx, maxHeight: maxheight, overflow: 'auto',
        }}>
            {children}
        </Paper>
    )
}
export default ScrollableTextArea
