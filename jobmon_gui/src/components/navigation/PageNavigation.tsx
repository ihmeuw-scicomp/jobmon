import React, { useContext } from 'react';

import { styled, useTheme, Theme, CSSObject } from '@mui/material/styles';
import Box from '@mui/material/Box';
import MuiDrawer from '@mui/material/Drawer';
import MuiAppBar, { AppBarProps as MuiAppBarProps } from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import List from '@mui/material/List';
import CssBaseline from '@mui/material/CssBaseline';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import SubjectIcon from '@mui/icons-material/Subject';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import { PropsWithChildren, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { IhmeIcon } from '@jobmon_gui/assets/logo/logo';

import FeedIcon from '@mui/icons-material/Feed';
import Typography from '@mui/material/Typography';

import MediationIcon from '@mui/icons-material/Mediation';
import BifrostLinks from '@jobmon_gui/components/navigation/BifrostLinks';
import BatchPredictionIcon from '@mui/icons-material/BatchPrediction';
import { Tooltip } from '@mui/material';
import bifrostEnabled from '@jobmon_gui/components/navigation/Bifrost';
import '@fontsource/archivo';
import { readTheDocsUrl } from '@jobmon_gui/configs/ExternalUrls';
import AuthContext from '@jobmon_gui/contexts/AuthContext.tsx';
import UserAvatarButton from '@jobmon_gui/components/navigation/UserAvatarButton';
import UserMenu from '@jobmon_gui/components/navigation/UserMenu.tsx';

const drawerWidth = 260;

const openedMixin = (theme: Theme): CSSObject => ({
    width: drawerWidth,
    transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.enteringScreen,
    }),
    overflowX: 'hidden',
});

const closedMixin = (theme: Theme): CSSObject => ({
    transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    overflowX: 'hidden',
    width: `calc(${theme.spacing(7)} + 1px)`,
    [theme.breakpoints.up('sm')]: {
        width: `calc(${theme.spacing(8)} + 1px)`,
    },
});

const DrawerHeader = styled('div')(({ theme }) => ({
    display: 'flex',
    alignItems: 'center',
    padding: theme.spacing(1, 1),
}));

interface AppBarProps extends MuiAppBarProps {
    open?: boolean;
}

const AppBar = styled(MuiAppBar, {
    shouldForwardProp: prop => prop !== 'open',
})<AppBarProps>(({ theme, open }) => ({
    zIndex: theme.zIndex.drawer + 1,
    transition: theme.transitions.create(['width', 'margin'], {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    ...(open && {
        marginLeft: drawerWidth,
        width: `calc(100% - ${drawerWidth}px)`,
        transition: theme.transitions.create(['width', 'margin'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
        }),
    }),
}));

const Drawer = styled(MuiDrawer, {
    shouldForwardProp: prop => prop !== 'open',
})(({ theme, open }) => ({
    width: drawerWidth,
    flexShrink: 0,
    whiteSpace: 'nowrap',
    boxSizing: 'border-box',
    ...(open && {
        ...openedMixin(theme),
        '& .MuiDrawer-paper': openedMixin(theme),
    }),
    ...(!open && {
        ...closedMixin(theme),
        '& .MuiDrawer-paper': closedMixin(theme),
    }),
}));

export default function PageNavigation({ children }: PropsWithChildren) {
    const theme = useTheme();
    const navigate = useNavigate();
    const [drawerOpen, setDrawerOpen] = useState(false);
    const { user } = useContext(AuthContext);
    const [userMenuAnchorEl, setUserMenuAnchorEl] =
        useState<null | HTMLElement>(null);

    const handleDrawerOpen = () => {
        setDrawerOpen(true);
    };

    const handleDrawerClose = () => {
        setDrawerOpen(false);
    };

    return (
        <Box sx={{ display: 'flex' }} id="AppBarBox">
            <CssBaseline />
            <AppBar
                position="fixed"
                sx={{ backgroundColor: '#17B9CF' }}
                id="AppBar"
            >
                <Toolbar>
                    <Box
                        sx={{
                            display: 'flex',
                            flexGrow: 1,
                            alignItems: 'center',
                        }}
                    >
                        <img
                            src={IhmeIcon}
                            style={{ padding: 5 }}
                            height={50}
                            alt=""
                        />
                        <Typography
                            variant="banner"
                            sx={{
                                boxShadow: 'none',
                                userSelect: 'none',
                                display: 'flex',
                                alignItems: 'center',
                                '&:hover': {
                                    textDecoration: 'none',
                                },
                                '&:visited': {
                                    color: 'inherit',
                                },
                            }}
                            component={Link}
                            id="homeNavbarLink"
                            to="/"
                        >
                            Jobmon
                        </Typography>
                    </Box>
                    <Box>
                        <UserAvatarButton
                            userFullName={
                                user.given_name || user.family_name
                                    ? `${user.given_name} ${user.family_name}`
                                    : ''
                            }
                            onClickHandler={setUserMenuAnchorEl}
                        />
                        <UserMenu
                            username={user.email || ''}
                            anchorEl={userMenuAnchorEl}
                            setAnchorEl={setUserMenuAnchorEl}
                        />
                    </Box>
                </Toolbar>
            </AppBar>
            <Drawer
                id="Drawer"
                variant="permanent"
                open={drawerOpen}
                sx={{
                    maxHeight: 'calc(100vh - 200px)',
                    mr: 2,
                }}
            >
                <Toolbar />
                <DrawerHeader id="DrawerHeader">
                    {drawerOpen ? (
                        <IconButton
                            color="inherit"
                            aria-label="close drawer"
                            onClick={handleDrawerClose}
                        >
                            {theme.direction === 'ltr' ? (
                                <ChevronLeftIcon />
                            ) : (
                                <ChevronRightIcon />
                            )}
                        </IconButton>
                    ) : (
                        <IconButton
                            color="inherit"
                            aria-label="open drawer"
                            onClick={handleDrawerOpen}
                        >
                            {theme.direction === 'ltr' ? (
                                <ChevronRightIcon />
                            ) : (
                                <ChevronLeftIcon />
                            )}
                        </IconButton>
                    )}
                </DrawerHeader>
                <Divider />
                <List>
                    <ListItem key={'drawerJobmonGUI'} disablePadding>
                        <Tooltip
                            title={drawerOpen ? '' : 'Jobmon GUI'}
                            placement="right"
                        >
                            <ListItemButton
                                onClick={() => {
                                    navigate('/');
                                }}
                            >
                                <ListItemIcon>
                                    <MediationIcon />
                                </ListItemIcon>
                                <ListItemText primary={'Jobmon GUI'} />
                            </ListItemButton>
                        </Tooltip>
                    </ListItem>
                    <ListItem key={'drawerJobmonAtIHME'} disablePadding>
                        <Tooltip
                            title={drawerOpen ? '' : 'Jobmon at IHME'}
                            placement="right"
                        >
                            <ListItemButton
                                onClick={() => {
                                    navigate('/jobmon_at_ihme');
                                }}
                            >
                                <ListItemIcon>
                                    <FeedIcon />
                                </ListItemIcon>
                                <ListItemText primary={'Jobmon at IHME'} />
                            </ListItemButton>
                        </Tooltip>
                    </ListItem>
                </List>
                <ListItem key={'drawerHelp'} disablePadding>
                    <Tooltip title={drawerOpen ? '' : 'Help'} placement="right">
                        <ListItemButton
                            onClick={() => {
                                navigate('/help');
                            }}
                        >
                            <ListItemIcon>
                                <BatchPredictionIcon />
                            </ListItemIcon>
                            <ListItemText primary={'Help'} />
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
                <ListItem key={'drawerDocumentation'} disablePadding>
                    <Tooltip
                        title={drawerOpen ? '' : 'Documentation'}
                        placement="right"
                    >
                        <ListItemButton href={readTheDocsUrl} target={'_blank'}>
                            <ListItemIcon>
                                <SubjectIcon />
                            </ListItemIcon>
                            <ListItemText primary={'Documentation'} />
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
                <BifrostLinks enabled={bifrostEnabled()} open={drawerOpen} />
                <Box
                    sx={{
                        height: '100%',
                        display: 'flex',
                        alignItems: 'flex-end',
                    }}
                ></Box>
                <Typography color={'#ccc'} align={'center'}>
                    v: {import.meta.env.VITE_APP_VERSION}
                </Typography>
            </Drawer>
            <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
                <Toolbar />
                {children}
            </Box>
        </Box>
    );
}
