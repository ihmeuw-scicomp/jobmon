import React from 'react';

import {styled, useTheme, Theme, CSSObject} from "@mui/material/styles";
import Box from "@mui/material/Box";
import MuiDrawer from "@mui/material/Drawer";
import MuiAppBar, {AppBarProps as MuiAppBarProps} from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import List from "@mui/material/List";
import CssBaseline from "@mui/material/CssBaseline";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import MenuIcon from "@mui/icons-material/Menu";
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import {PropsWithChildren, useState} from "react";
import {Link, useNavigate} from "react-router-dom";
import {IhmeIcon} from "../../assets/logo/logo";

import FeedIcon from "@mui/icons-material/Feed";
import Typography from "@mui/material/Typography";

import MediationIcon from '@mui/icons-material/Mediation';
import BifrostLinks from "./BifrostLinks";
import BatchPredictionIcon from '@mui/icons-material/BatchPrediction';
import {Tooltip} from "@mui/material";
import bifrostEnabled from "./Bifrost";
import "@fontsource/archivo"

const drawerWidth = 260;

const openedMixin = (theme: Theme): CSSObject => ({
    width: drawerWidth,
    transition: theme.transitions.create("width", {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.enteringScreen,
    }),
    overflowX: "hidden",
});

const closedMixin = (theme: Theme): CSSObject => ({
    transition: theme.transitions.create("width", {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    overflowX: "hidden",
    width: `calc(${theme.spacing(7)} + 1px)`,
    [theme.breakpoints.up("sm")]: {
        width: `calc(${theme.spacing(8)} + 1px)`,
    },
});

const DrawerHeader = styled("div")(({theme}) => ({
    display: "flex",
    alignItems: "center",
    justifyContent: "flex-end",
    padding: theme.spacing(0, 1),
    // necessary for content to be below app bar
    ...theme.mixins.toolbar,
}));

interface AppBarProps extends MuiAppBarProps {
    open?: boolean;
}

const AppBar = styled(MuiAppBar, {
    shouldForwardProp: (prop) => prop !== "open",
})<AppBarProps>(({theme, open}) => ({
    zIndex: theme.zIndex.drawer + 1,
    transition: theme.transitions.create(["width", "margin"], {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
    }),
    ...(open && {
        marginLeft: drawerWidth,
        width: `calc(100% - ${drawerWidth}px)`,
        transition: theme.transitions.create(["width", "margin"], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
        }),
    }),
}));

const Drawer = styled(MuiDrawer, {shouldForwardProp: (prop) => prop !== "open"})(
    ({theme, open}) => ({
        width: drawerWidth,
        flexShrink: 0,
        whiteSpace: "nowrap",
        boxSizing: "border-box",
        ...(open && {
            ...openedMixin(theme),
            "& .MuiDrawer-paper": openedMixin(theme),
        }),
        ...(!open && {
            ...closedMixin(theme),
            "& .MuiDrawer-paper": closedMixin(theme),
        }),
    })
);

export default function PageNavigation({children}: PropsWithChildren) {
    const theme = useTheme();
    const [open, setOpen] = useState(false);
    const navigate = useNavigate();

    const handleDrawerOpen = () => {
        setOpen(true);
    };

    const handleDrawerClose = () => {
        setOpen(false);
    };

    return (
        <Box sx={{display: "flex"}}>
            <CssBaseline/>
            <AppBar position="fixed" open={open} sx={{ backgroundColor: "#17B9CF" }}>
                <Toolbar>
                    <IconButton
                        color="inherit"
                        aria-label="open drawer"
                        onClick={handleDrawerOpen}
                        edge="start"
                        sx={{
                            marginRight: 5,
                            ...(open && {display: "none"}),
                        }}>
                        <MenuIcon/>
                    </IconButton>
                        <Box sx={{ display: "flex", alignItems: "center" }}>
                          <img src={IhmeIcon} style={{ padding: 5 }} height={50} alt="" />
                          <Typography
                            sx={{
                              fontFamily: "'Archivo', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue', 'sans-serif'",
                              fontWeight: 300,
                              fontSize: "3rem",
                              lineHeight: 1.167,
                              letterSpacing: "0em",
                              color: "white",
                              textDecoration: "none",
                              boxShadow: "none",
                              userSelect: "none",
                              display: "flex",
                              alignItems: "center",
                            }}
                          >
                            Jobmon GUI
                          </Typography>
                        </Box>
                </Toolbar>
            </AppBar>
            <Drawer variant="permanent" open={open}>
                <DrawerHeader>
                    <IconButton onClick={handleDrawerClose}>
                        {theme.direction === "ltr" ? <ChevronLeftIcon/> : <ChevronRightIcon/>}
                    </IconButton>
                </DrawerHeader>
                <Divider/>
                <List>
                    <ListItem key={"drawerJobmonGUI"} disablePadding>
                        <Tooltip title={open ? "" : "Jobmon GUI"} placement="right">
                            <ListItemButton
                                onClick={() => {
                                    navigate("/");
                                }}>
                                <ListItemIcon>
                                    <MediationIcon/>
                                </ListItemIcon>
                                <ListItemText primary={"Jobmon GUI"}/>
                            </ListItemButton>
                        </Tooltip>
                    </ListItem>
                    <ListItem key={"drawerJobmonAtIHME"} disablePadding>
                        <Tooltip title={open ? "" : "Jobmon at IHME"} placement="right">
                            <ListItemButton
                                onClick={() => {
                                    navigate("/jobmon_at_ihme");
                                }}>
                                <ListItemIcon>
                                    <FeedIcon/>
                                </ListItemIcon>
                                <ListItemText primary={"Jobmon at IHME"}/>
                            </ListItemButton>
                        </Tooltip>
                    </ListItem>
                </List>
                <ListItem key={"drawerHelp"} disablePadding>
                    <Tooltip title={open ? "" : "Help"} placement="right">
                        <ListItemButton onClick={() => {
                            navigate("/help")
                        }}>

                            <ListItemIcon>
                                <BatchPredictionIcon/>
                            </ListItemIcon>

                            <ListItemText primary={"Help"}/>
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
                <BifrostLinks enabled={bifrostEnabled()} open={open}/>
            </Drawer>
            <Box component="main" sx={{flexGrow: 1, p: 3}}>
                <DrawerHeader/>
                {children}
            </Box>
        </Box>
    );
}
