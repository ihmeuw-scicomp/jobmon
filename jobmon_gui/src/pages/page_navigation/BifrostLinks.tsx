import Divider from "@mui/material/Divider";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import MediationIcon from "@mui/icons-material/Mediation";
import ListItemText from "@mui/material/ListItemText";
import HubIcon from "@mui/icons-material/Hub";
import {Tooltip} from "@mui/material";

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
                <ListItem key={"drawerExternalSquid"} disablePadding>
                    <Tooltip title={open ? "" : "Squid"} placement="right">
                        <ListItemButton href="https://squid.ihme.washington.edu" target={"_blank"}>
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
