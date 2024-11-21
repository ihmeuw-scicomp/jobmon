import {ListItemIcon, ListItemText, Menu, MenuItem, Typography, Link} from '@mui/material'
import Logout from '@mui/icons-material/Logout';
import React from 'react'
import {Box} from '@mui/system';
import {logoutURL} from "@jobmon_gui/configs/ApiUrls.ts";

type UserMenuProps = {
    username: string
    anchorEl: null | HTMLElement
    setAnchorEl: (element: null | HTMLElement) => void
}

const UserMenu = ({username, anchorEl, setAnchorEl}: UserMenuProps) => {
    const open = Boolean(anchorEl);
    const handleClose = () => {
        setAnchorEl(null);
    };

    return (
        <>
            <Menu
                anchorEl={anchorEl}
                open={open}
                onClose={handleClose}
                // onClick={handleClose}
                sx={{minWidth: 300}}
            >
                <Box sx={{minWidth: 200, my: 1, mx: 2}}>
                    <Typography>Welcome, {username}</Typography>
                </Box>
                <MenuItem id="menu-item-logout" component={Link} href={logoutURL}>
                    <ListItemIcon>
                        <Logout/>
                    </ListItemIcon>
                    <ListItemText>
                        Logout
                    </ListItemText>
                </MenuItem>
            </Menu>
        </>
    )
}

export default UserMenu