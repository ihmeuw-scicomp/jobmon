import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import HubIcon from "@mui/icons-material/Hub";
import PsychologyIcon from '@mui/icons-material/Psychology';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import {Tooltip} from "@mui/material";
import {imageLauncherUrl, metricMindUrl, squidUrl} from "@jobmon_gui/configs/ExternalUrls";


type BifrostLinksProps = {
    enabled: boolean,
    open: boolean
}

export const BifrostLinks = ({enabled, open}: BifrostLinksProps) => {

    if (!enabled) {
        return <></>
    }

    return (
        <>
            <Divider/>
            <List>
                <ListItem key={"drawerExternalMetricMind"} disablePadding>
                    <Tooltip title={open ? "" : "MetricMind"} placement="right">
                        <ListItemButton href={metricMindUrl} target={"_blank"}>
                            <ListItemIcon>
                                <PsychologyIcon/>
                            </ListItemIcon>
                            <ListItemText primary={"MetricMind"}/>
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
                <ListItem key={"drawerExternalImageLauncher"} disablePadding>
                    <Tooltip title={open ? "" : "Image Launcher"} placement="right">
                        <ListItemButton href={imageLauncherUrl} target={"_blank"}>
                            <ListItemIcon>
                                <RocketLaunchIcon/>
                            </ListItemIcon>
                            <ListItemText primary={"Image Launcher"}/>
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
                <ListItem key={"drawerExternalSquid"} disablePadding>
                    <Tooltip title={open ? "" : "Squid"} placement="right">
                        <ListItemButton href={squidUrl} target={"_blank"}>
                            <ListItemIcon>
                                <HubIcon/>
                            </ListItemIcon>
                            <ListItemText primary={"Squid"}/>
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
            </List>
        </>
    )
        ;
};

export default BifrostLinks
