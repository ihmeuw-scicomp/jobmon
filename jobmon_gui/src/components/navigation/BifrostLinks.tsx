import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import StorageIcon from '@mui/icons-material/Storage';
import PsychologyIcon from '@mui/icons-material/Psychology';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import {Tooltip} from "@mui/material";
import {imageLauncherUrl, metricMindUrl, jobInfoUrl} from "@jobmon_gui/configs/ExternalUrls";


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
                <ListItem key={"drawerExternalJobInfo"} disablePadding>
                    <Tooltip title={open ? "" : "Job Info"} placement="right">
                        <ListItemButton href={jobInfoUrl} target={"_blank"}>
                            <ListItemIcon>
                                <StorageIcon/>
                            </ListItemIcon>
                            <ListItemText primary={"Job Info"}/>
                        </ListItemButton>
                    </Tooltip>
                </ListItem>
            </List>
        </>
    )
        ;
};

export default BifrostLinks
