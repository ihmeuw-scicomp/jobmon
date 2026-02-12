import {
    ListItemIcon,
    ListItemText,
    Menu,
    MenuItem,
    Typography,
    Link,
    Select,
    Grid,
    Button,
} from '@mui/material';
import Logout from '@mui/icons-material/Logout';
import { useState } from 'react';
import { Box } from '@mui/system';
import { logoutURL } from '@jobmon_gui/configs/ApiUrls.ts';
import TuneIcon from '@mui/icons-material/Tune';
import { JobmonModal } from '@jobmon_gui/components/JobmonModal.tsx';
import {
    useDisplayTimeFormatStore,
    useDisplayTimezoneStore,
} from '@jobmon_gui/stores/DateTime.ts';
import InfoIcon from '@mui/icons-material/Info';
import IconButton from '@mui/material/IconButton';
import HtmlTooltip from '@jobmon_gui/components/HtmlToolTip.tsx';

type UserMenuProps = {
    username: string;
    anchorEl: null | HTMLElement;
    setAnchorEl: (element: null | HTMLElement) => void;
};

const UserMenu = ({ username, anchorEl, setAnchorEl }: UserMenuProps) => {
    const open = Boolean(anchorEl);
    const [settingsOpen, setSettingsOpen] = useState(false);
    const timezoneStore = useDisplayTimezoneStore();
    const timeFormatStore = useDisplayTimeFormatStore();

    return (
        <>
            <Menu
                anchorEl={anchorEl}
                open={open}
                onClose={() => setAnchorEl(null)}
                sx={{ minWidth: 300 }}
            >
                <Box sx={{ minWidth: 200, my: 1, mx: 2 }}>
                    <Typography>Welcome, {username}</Typography>
                </Box>
                <MenuItem
                    id="menu-item-logout"
                    onClick={() => setSettingsOpen(true)}
                >
                    <TuneIcon /> Settings
                </MenuItem>
                <MenuItem
                    id="menu-item-logout"
                    component={Link}
                    href={logoutURL}
                >
                    <ListItemIcon>
                        <Logout />
                    </ListItemIcon>
                    <ListItemText>Logout</ListItemText>
                </MenuItem>
            </Menu>
            <JobmonModal
                title={'Settings'}
                open={settingsOpen}
                onClose={() => setSettingsOpen(false)}
                width={'80%'}
            >
                <Grid container spacing={4} sx={{ pl: 4 }}>
                    <Grid item xs={3}>
                        <Box
                            sx={{ display: 'flex', justifyContent: 'flex-end' }}
                        >
                            <Typography
                                fontWeight={'bold'}
                                fontSize={'1.2em'}
                                sx={{ pt: 1 }}
                            >
                                Timezone:
                                <HtmlTooltip
                                    title={
                                        'Jobmon stores all dates in UTC. This setting allows you to set the timezone that will be displayed in the web ui.'
                                    }
                                >
                                    <IconButton>
                                        <InfoIcon />
                                    </IconButton>
                                </HtmlTooltip>
                            </Typography>
                        </Box>
                    </Grid>
                    <Grid item xs={9}>
                        <Select
                            value={timezoneStore.get()}
                            onChange={e => timezoneStore.set(e.target.value)}
                        >
                            <MenuItem
                                value={timezoneStore.getBrowserDefaultTimezone()}
                            >
                                {timezoneStore.getBrowserDefaultTimezone()}
                            </MenuItem>
                            {timezoneStore.getSupportedTimezones().map(t => (
                                <MenuItem value={t}>{t}</MenuItem>
                            ))}
                        </Select>
                    </Grid>
                    <Grid item xs={3}>
                        <Box
                            sx={{ display: 'flex', justifyContent: 'flex-end' }}
                        >
                            <Typography
                                fontWeight={'bold'}
                                fontSize={'1.2em'}
                                sx={{ pt: 1 }}
                            >
                                Time Format:
                                <HtmlTooltip
                                    title={
                                        'This setting allows you to customize the displayed time format.'
                                    }
                                >
                                    <IconButton>
                                        <InfoIcon />
                                    </IconButton>
                                </HtmlTooltip>
                            </Typography>
                        </Box>
                    </Grid>
                    <Grid item xs={4}>
                        <Select
                            value={timeFormatStore.get()}
                            onChange={e => timeFormatStore.set(e.target.value)}
                        >
                            {timeFormatStore.getTimeFormatList().map(t => (
                                <MenuItem value={t}>{t}</MenuItem>
                            ))}
                        </Select>
                    </Grid>
                    <Grid item xs={5}>
                        <Button
                            sx={{ mt: 1 }}
                            variant={'outlined'}
                            onClick={() =>
                                window
                                    .open(
                                        'https://day.js.org/docs/en/display/format',
                                        '_blank'
                                    )
                                    .focus()
                            }
                        >
                            Time format code Reference
                        </Button>
                    </Grid>
                </Grid>
            </JobmonModal>
        </>
    );
};

export default UserMenu;
