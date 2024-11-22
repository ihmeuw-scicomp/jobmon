import {loginURL} from "@jobmon_gui/configs/ApiUrls.ts";
import {Button, Grid} from "@mui/material";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import React from "react";
import Paper from "@mui/material/Paper";
import {IhmeIcon} from "@jobmon_gui/assets/logo/logo.ts";
import {loginButtonText} from "@jobmon_gui/configs/Login.ts";

const LoginScreen = () => {
    return (
        <Box
            display="flex"
            justifyContent="center"
            alignItems="center"
            minHeight="100vh"
            sx={{backgroundColor: "#eee"}}
        >
            <Paper sx={{borderRadius: 5}}>
                <Box sx={{p: 4}}>
                    <Grid container>
                        <Grid item xs={12}>
                            <Box sx={{display: "flex", alignItems: "center", mb: 2}}>
                                <img src={IhmeIcon} style={{padding: 5}} height={50} alt=""/>
                                <Typography
                                    variant="h4"
                                    color={"primary"}
                                    sx={{
                                        fontFamily: '-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans","Liberation Sans",sans-serif,"Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol","Noto Color Emoji"',
                                        boxShadow: "none",
                                        userSelect: "none",
                                        alignItems: "center",
                                        '&:hover': {
                                            textDecoration: "none",
                                        },
                                        '&:visited': {
                                            color: "inherit",
                                        },
                                    }}>
                                    Jobmon
                                </Typography>
                            </Box>
                            <Typography variant="h4" sx={{mb: 4}}>Login</Typography>
                        </Grid>
                        <Grid item xs={12}></Grid>
                        <Grid item xs={12}><Button onClick={() => window.location.replace(loginURL)} variant={"contained"}
                                                   color={"primary"}>{loginButtonText}</Button></Grid>
                    </Grid>
                </Box>
            </Paper>
        </Box>
    )
}
export default LoginScreen