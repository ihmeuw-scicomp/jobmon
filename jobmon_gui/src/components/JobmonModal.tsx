import Modal from "@mui/material/Modal";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import React, {ReactNode} from "react";
import IconButton from "@mui/material/IconButton";
import CloseIcon from '@mui/icons-material/Close';
import Divider from "@mui/material/Divider";
import ScrollableTextArea from "@jobmon_gui/components/ScrollableTextArea";

type JobmonModalProps = {
    title?: string,
    open: boolean
    onClose?: {
        // `Record<never, never>` see: https://github.com/supabase/postgres-meta/issues/332
        bivarianceHack(event: Record<never, never>, reason: 'backdropClick' | 'escapeKeyDown' | 'closeButtonClick'): void;
    }['bivarianceHack'];
    children: ReactNode
    width: number | string
    minHeight?: number | string,

}


export const JobmonModal = ({open, onClose, children, width, title, minHeight}: JobmonModalProps) => {
    if (!onClose) return (<></>)
    // MUI Modal Component with default style
    return (<Modal
        open={open}
        onClose={onClose} disableRestoreFocus={true}>
        <Box sx={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            width: width,
            bgcolor: 'background.paper',
            border: '2px solid #000',
            boxShadow: 24,
            minHeight: minHeight ? minHeight : "50%",
            p: 4,
        }}>
            <Box display="flex" alignItems="center">
                <Box flexGrow={1}>{title ? (<Typography variant={"h4"}>{title}</Typography>) : ""}</Box>
                <Box>
                    <IconButton id={"modal_close_button"} onClick={(event) => {
                        onClose(event, "closeButtonClick")
                    }}>
                        <CloseIcon/>
                    </IconButton>
                </Box>
            </Box>
            {title ? (<Divider sx={{mb:3}}/>): <></>}
            <ScrollableTextArea children={children} elevation={0} maxheight="80vh" />
        </Box>
    </Modal>)
}

export default JobmonModalProps
