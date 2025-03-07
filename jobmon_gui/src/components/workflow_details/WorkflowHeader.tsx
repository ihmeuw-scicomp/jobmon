import "@jobmon_gui/styles/jobmon_gui.css";
import {BiRun} from "react-icons/bi";
import {IoMdCloseCircle, IoMdCloseCircleOutline} from "react-icons/io";
import {AiFillSchedule, AiFillCheckCircle} from "react-icons/ai";
import {TbHandStop} from "react-icons/tb";
import {HiRocketLaunch} from "react-icons/hi2";
import React, {useContext, useState, useEffect} from "react";
import {JobmonModal} from "@jobmon_gui/components/JobmonModal.tsx";
import {
    CircularProgress,
    Grid,
    TextField,
    Table,
    TableCell,
    TableRow,
    FormControlLabel,
    Checkbox,
    Tooltip,
    InputLabel,
    Select,
    MenuItem,
    FormControl,
    Divider
} from "@mui/material";
import {Box} from "@mui/system";
import {useMutation, useQuery} from "@tanstack/react-query";
import Typography from "@mui/material/Typography";
import humanizeDuration from 'humanize-duration';
import {formatJobmonDate} from "@jobmon_gui/utils/DayTime.ts";
import {compare} from 'compare-versions';
import AllInclusiveIcon from '@mui/icons-material/AllInclusive';
import {
    update_task_status_url,
    task_table_url
} from "@jobmon_gui/configs/ApiUrls.ts";

type WorkflowHeaderProps = {
    wf_id: number | string
    task_template_info: { tt_version_id: any; name: any; }[];
    onTechnicalPanelClose: () => void;
}

interface WorkflowData {
    max_concurrently_running: number;
}

interface TaskTemplateData {
    max_concurrently_running: number;
}

import IconButton from "@mui/material/IconButton";
import {HtmlTooltip} from "@jobmon_gui/components/HtmlToolTip";
import InfoIcon from '@mui/icons-material/Info';
import AuthContext from "@jobmon_gui/contexts/AuthContext.tsx";
import StopWorkflowButton from "@jobmon_gui/components/StopWorkflow.tsx";
import BuildIcon from '@mui/icons-material/Build';
import {getWorkflowDetailsQueryFn} from "@jobmon_gui/queries/GetWorkflowDetails.ts";
import axios from "axios";
import {
    get_task_template_concurrency_url,
    get_workflow_concurrency_url,
    set_task_template_concurrency_url,
    set_wf_concurrency_url
} from "@jobmon_gui/configs/ApiUrls.ts";
import {jobmonAxiosConfig} from "@jobmon_gui/configs/Axios.ts";
import {Label} from "@mui/icons-material";

interface WorkflowResponse {
  tasks: any[]; // Replace `any` with the correct type of tasks
  // other properties
}

export default function WorkflowHeader({
                                           wf_id,
                                           task_template_info,
                                           onTechnicalPanelClose,
                                       }: WorkflowHeaderProps) {
    const {user} = useContext(AuthContext)
    const user_name = user?.preferred_username ? user?.preferred_username.split("@")[0] : "unknown"
    const [recursive, setRecursive] = React.useState(true);
    const [wfFieldValues, setWfFieldValues] = useState(null)
    const wfDetails = useQuery({
        queryKey: ["workflow_details", "details", wf_id],
        queryFn: getWorkflowDetailsQueryFn,
        staleTime: 60000, // 60 seconds
        refetchOnMount: true,
    })
    const [statusUpdateMsgDict, setStatusUpdateMsgDict] = useState({})
    const [statusSelectionDict, setStatusSelectionDict] = useState({})
    const [wfTaskStatusUpdateMsg, setWfTaskStatusUpdateMsg] = useState("")

    axios.get<WorkflowData>(get_workflow_concurrency_url(wf_id), {
        ...jobmonAxiosConfig,
        data: null,
    }).then((r) => {
        setWfFieldValues(r.data.max_concurrently_running)
    });

    const [showWFInfo, setShowWFInfo] = useState(false)
    const [showTechnicalPanel, setShowTechnicalPanel] = useState(false)

    const [fieldValues, setFieldValues] = useState(
        task_template_info.reduce((acc, template) => {
            acc[template.tt_version_id] = 50;
            return acc;
        }, {})
    );

    const updateTaskTemplateConcurrency = useMutation({
        mutationFn: async ({task_template_version_id, max_tasks}: {
            task_template_version_id: string;
            max_tasks: string
        }) => {
            return axios.put(set_task_template_concurrency_url(wf_id), {
                task_template_version_id: task_template_version_id,
                max_tasks: max_tasks
            }, jobmonAxiosConfig)
        },
    })

    const handleUpdateStatus = (status: string, template) => {
        // set the message to "Updating..."
        setStatusUpdateMsgDict(prevValues => ({
            ...prevValues,
            [template.tt_version_id]: "Updating...",
        }));
        // update the selected status
        setStatusSelectionDict(prevValues => ({
            ...prevValues,
            [template.tt_version_id]: status,
        }));
        // call the get_task_template_url post request with date {tool_version_id: template.tt_version_id, task_template_name: template.name}
        // then use teh task_template_id to call getTaskTemplateDetails
        // then update the task status
        const tt_name = template.name
        const url = task_table_url + wf_id
        axios
              .get(url, {
                params: { tt_name: tt_name },
                ...jobmonAxiosConfig,
              })
              .then(response => {
                const tasks = (response.data as WorkflowResponse).tasks;
                const task_id_list = tasks.map(task => task.task_id);
                // If task_id_list length > 10000 and recursive, update the message and exit early.
                if (task_id_list.length > 10000 && recursive) {
                      setStatusUpdateMsgDict(prevValues => ({
                        ...prevValues,
                        [template.tt_version_id]: "Error: Too many tasks to update recursively. Please use non-recursive update.",
                      }));
                      // remove recursive checkbox
                      setRecursive(false)
                      // clean the status selection
                        setStatusSelectionDict(prevValues => ({
                            ...prevValues,
                            [template.tt_version_id]: "",
                        }));
                      return;
                }


                // Update the status of the tasks by returning the PUT request promise
                return axios.put(
                  update_task_status_url,
                  {
                      workflow_id: wf_id,
                      task_ids: task_id_list,
                      new_status: status,
                      recursive: recursive,
                  },
                  jobmonAxiosConfig
                );
              })
              .then(response => {
                  // if the response is successful and the message is not Updating,
                  // update the message to "Success"
                  if (response && statusUpdateMsgDict[template.tt_version_id] !== "Updating...") {
                      setStatusUpdateMsgDict(prevValues => ({
                          ...prevValues,
                          [template.tt_version_id]: "Success",
                      }));
                  }
              })
              .catch(error => {
                setStatusUpdateMsgDict(prevValues => ({
                  ...prevValues,
                  [template.tt_version_id]: "Error: " + (error.message || error.toString()),
                }));
              });
        }

     const handleUpdateStatusAll = (status: string) => {
         setWfTaskStatusUpdateMsg("Updating...");
            axios
                .put(
                    update_task_status_url,
                    {
                        workflow_id: wf_id,
                        new_status: status,
                        recursive: false,
                        task_ids: "all"
                    },
                    jobmonAxiosConfig
                )
                .then(response => {
                    setWfTaskStatusUpdateMsg("Success");
                })
                .catch(error => {
                    setWfTaskStatusUpdateMsg("Error: " + (error.message || error.toString()));
                });
     }

    useEffect(() => {
        setWfTaskStatusUpdateMsg("") // reset status update message
        setStatusUpdateMsgDict({}) // reset status update message
        task_template_info.map(template => {
            const url = get_task_template_concurrency_url(wf_id, template.tt_version_id);
            return axios.get<TaskTemplateData>(url, jobmonAxiosConfig)
                .then((r) => {
                    setFieldValues(prevValues => ({
                        ...prevValues,
                        [template.tt_version_id]: r.data.max_concurrently_running
                    }));
                    setStatusUpdateMsgDict((prevValues) => ({
                        ...prevValues,
                        [template.tt_version_id]: ""
                    }));
                    setStatusSelectionDict((prevValues) => ({
                        ...prevValues,
                        [template.tt_version_id]: ""
                    }));
                })
        });
    }, [task_template_info]);

    const updateWfConcurrency = useMutation({
        mutationFn: async ({max_tasks}: {
            max_tasks: string
        }) => {
            return axios.put(set_wf_concurrency_url(wf_id), {
                max_tasks: max_tasks
            }, jobmonAxiosConfig)
        },
    })

    const handleInputChange = (id) => (event) => {
        const value = event.target.value === "" ? "" : Number(event.target.value);
        if (value === "" || (value >= 0 && value <= 2147483647)) {
            setFieldValues((prevValues) => ({
                ...prevValues,
                [id]: value,
            }));
        }
        updateTaskTemplateConcurrency.mutate({
            task_template_version_id: id,
            max_tasks: value.toString(),
        });

    };

    const handleWfInputChange = () => (event) => {
        const value = event.target.value === "" ? "" : Number(event.target.value);
        if (value === "" || (value >= 0 && value <= 2147483647)) {
            setWfFieldValues(value === "" ? 0 : value);
        }
        updateWfConcurrency.mutate({
            max_tasks: value.toString(),
        });

    };

    const statusIcons = {
        A: {icon: <IoMdCloseCircleOutline/>, className: 'icon-aa'},
        D: {icon: <AiFillCheckCircle/>, className: 'icon-dd'},
        F: {icon: <IoMdCloseCircle/>, className: 'icon-ff'},
        G: {icon: <AiFillSchedule/>, className: 'icon-pp'},
        H: {icon: <TbHandStop/>, className: 'icon-aa'},
        I: {icon: <AiFillSchedule/>, className: 'icon-pp'},
        O: {icon: <HiRocketLaunch/>, className: 'icon-ss'},
        Q: {icon: <AiFillSchedule/>, className: 'icon-pp'},
        R: {icon: <BiRun/>, className: 'icon-rr'},
    };


    const gridHeaderStyles = {fontWeight: "bold"}

    if (wfDetails.isLoading) {
        return <CircularProgress/>
    }
    if (wfDetails.isError) {
        return <Typography>Error loading workflow details. Please refresh and try again.</Typography>
    }
    const wf_status = wfDetails?.data?.wf_status
    const wf_status_desc = wfDetails?.data?.wf_status_desc
    const wf_tool = wfDetails?.data?.tool_name
    const wf_name = wfDetails?.data?.wf_name
    const wf_args = wfDetails?.data?.wf_args
    const wf_submitted_date = formatJobmonDate(wfDetails?.data?.wf_created_date)
    const wfr_heartbeat_date = formatJobmonDate(wfDetails?.data?.wfr_heartbeat_date)
    const wf_elapsed_time = humanizeDuration(new Date(wfDetails?.data?.wfr_heartbeat_date).getTime() - new Date(wfDetails?.data?.wf_created_date).getTime())
    const jobmon_version = wfDetails?.data?.wfr_jobmon_version
    const wfr_user = wfDetails?.data?.wfr_user

    function normalizeVersion(version: string): string {
        return version
            .replace(/\.dev/, '-dev')
            .replace(/(\d+)rc(\d+)/, '$1-rc.$2')
    }

    const normalizedVersion = normalizeVersion(jobmon_version);
    const disabled = !compare(normalizedVersion, "3.3", '>')

    const {icon, className} = statusIcons[wf_status] || {};

    const handleClose = () => {
        setShowTechnicalPanel(false);
        if (onTechnicalPanelClose) {
            onTechnicalPanelClose();
        }
    }

    return (
        <Box className="App-header">
            <Box sx={{display: 'flex', alignItems: 'center'}}>
                <span>
                    {icon && <span className={className}>{icon}</span>}
                    <Typography variant="h5" component="span" sx={{pl: 1}}>
                        {wf_id} {wf_name ? `- ${wf_name}` : ''}
                    </Typography>
                </span>
                <span style={{transform: 'translateY(-5px)', paddingLeft: '10px'}}>
                    <HtmlTooltip
                        title="Workflow Information"
                        arrow={true}
                        placement={"bottom"}
                        sx={{pl: 1}}
                    >
                        <IconButton
                            color="inherit"
                            onClick={() => setShowWFInfo(true)}
                        >
                            <InfoIcon fontSize={"large"}/>
                        </IconButton>
                    </HtmlTooltip>
                    <HtmlTooltip
                        title="Technical Panel"
                        arrow={true}
                        placement={"bottom"}
                    >
                        <IconButton
                            color="inherit"
                            onClick={() => setShowTechnicalPanel(true)}
                        >
                            <BuildIcon/>
                        </IconButton>
                    </HtmlTooltip>
                </span>
            </Box>
            <Box>
                <JobmonModal
                    title={"Workflow Information"}
                    children={
                        <Grid container spacing={2}>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Status:</Grid>
                            <Grid item xs={9}>{wf_status}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Status Description:</Grid>
                            <Grid item xs={9}>{wf_status_desc}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Tool:</Grid>
                            <Grid item xs={9}>{wf_tool}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Name:</Grid>
                            <Grid item xs={9}>{wf_name}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Args:</Grid>
                            <Grid item xs={9}>{wf_args}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Submitted Date:</Grid>
                            <Grid item xs={9}>{wf_submitted_date}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>WorkflowRun Heartbeat Date:</Grid>
                            <Grid item xs={9}>{wfr_heartbeat_date}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow User:</Grid>
                            <Grid item xs={9}>{wfr_user}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Workflow Elapsed Time:</Grid>
                            <Grid item xs={9}>{wf_elapsed_time}</Grid>
                            <Grid item xs={3} sx={gridHeaderStyles}>Jobmon Version:</Grid>
                            <Grid item xs={9}>{jobmon_version}</Grid>
                        </Grid>
                    }
                    open={showWFInfo}
                    onClose={() => setShowWFInfo(false)}
                    width="80%"/>
            </Box>
            <Box>
                <JobmonModal
                    title={`Workflow ${wf_id} Technical Panel`}
                    children={
                        <Grid container spacing={2}>
                            <Grid item xs={10}>
                                <StopWorkflowButton wf_id={wf_id} disabled={disabled}/>
                            </Grid>
                            <Grid item xs={10}>
                                <Typography variant="h5">Workflow</Typography>
                            </Grid>
                            <Grid item xs={9}>
                                <Table>
                                    <TableRow>
                                        <TableCell align="center">Update Concurrency Limit</TableCell>
                                        <TableCell align="center">
                                            <Tooltip title={"Update the status of all tasks in the workflow"}>
                                                <span>Update Task Status</span>
                                            </Tooltip>
                                        </TableCell>
                                    </TableRow>
                                    <TableRow>
                                        <TableCell align="center">
                                            <TextField
                                                value={wfFieldValues}
                                                onChange={handleWfInputChange()}
                                                inputProps={{
                                                    step: 1,
                                                    min: 0,
                                                    max: 2147483647,
                                                    type: "number",
                                                    "aria-labelledby": `workflow-input-number`,
                                                }}
                                                variant="outlined"
                                                size="small"
                                                fullWidth
                                                disabled={disabled}
                                            />
                                        </TableCell>
                                        <TableCell align="center">
                                            <FormControl variant="outlined" size="small">
                                                <InputLabel id="new-status-label">New Status</InputLabel>
                                                <Select
                                                    labelId="new-status-label"
                                                    id="new-status-select"
                                                    label="New Status"
                                                    onChange={(e) => handleUpdateStatusAll(e.target.value as string)}
                                                    style={{minWidth: 80}}
                                                >
                                                    <MenuItem value="D">D</MenuItem>
                                                    <MenuItem value="G">G</MenuItem>
                                                </Select>
                                                {wfTaskStatusUpdateMsg !== "" &&
                                                <Typography variant="caption" color="error">
                                                    {wfTaskStatusUpdateMsg}
                                                </Typography>
                                                }
                                            </FormControl>
                                        </TableCell>
                                    </TableRow>
                                </Table>
                            </Grid>
                            <br/>
                            <Grid item xs={10}>
                                <Typography variant="h5">Task Templates</Typography>
                                <Typography>
                                    <Tooltip title="If recursive update is enabled, all related task statuses will be modified; if disabled, only the tasks in the selected template will be updated. For large workflows, it's recommended to disable recursion for better performance.">
                                      <FormControlLabel
                                        control={
                                          <Checkbox
                                            checked={recursive}
                                            onChange={(e) => setRecursive(e.target.checked)}
                                            color="primary"
                                          />
                                        }
                                        label="Recursive"
                                      />
                                   </Tooltip>
                                </Typography>
                            </Grid>
                            <Table>
                                    <TableRow>
                                        <TableCell align="center">Task Template Name</TableCell>
                                        <TableCell align="center">Update Concurrency Limit</TableCell>
                                        <TableCell align="center">
                                            {recursive ?
                                                <Tooltip title="Update all tasks in the template recursively">
                                                    <span>Update Task Status &nbsp; <AllInclusiveIcon/></span>
                                                </Tooltip> :
                                                <Tooltip title="Update only the tasks in the selected template">
                                                    <span>Update Task Status</span>
                                                </Tooltip>
                                            }
                                        </TableCell>
                                        <TableCell align="center"></TableCell>
                                    </TableRow>
                                {task_template_info?.map((template) => (

                                    <TableRow>
                                        <TableCell align={"center"}><Typography sx={gridHeaderStyles}>{template.name}</Typography></TableCell>
                                        <TableCell align="center">
                                            <TextField
                                                value={fieldValues[template.tt_version_id]}
                                                onChange={handleInputChange(template.tt_version_id)}
                                                inputProps={{
                                                    step: 1,
                                                    min: 0,
                                                    max: 2147483647,
                                                    type: "number",
                                                    "aria-labelledby": `input-number-${template.tt_version_id}`,
                                                }}
                                                variant="outlined"
                                                size="small"
                                                disabled={disabled}
                                            />
                                        </TableCell>
                                        <TableCell align="center">
                                              <FormControl variant="outlined" size="small">
                                                <InputLabel id="new-status-label">New Status</InputLabel>
                                                <Select
                                                  labelId="new-status-label"
                                                  id="new-status-select-{template.tt_id}"
                                                  label="New Status"
                                                  value={statusSelectionDict[template.tt_version_id] || ""}
                                                  onChange={(e) => handleUpdateStatus(e.target.value as string, template)}
                                                  style={{ minWidth: 80 }}
                                                >
                                                  <MenuItem value="D">D</MenuItem>
                                                  <MenuItem value="G">G</MenuItem>
                                                </Select>
                                                  {statusUpdateMsgDict[template.tt_version_id] !== "" &&
                                                      <Typography variant="caption" color="error">
                                                          {statusUpdateMsgDict[template.tt_version_id]}
                                                        </Typography>
                                                  }
                                              </FormControl>
                                        </TableCell>
                                    </TableRow>

                                  ))}
                            </Table>
                        </Grid>
                    }
                    open={showTechnicalPanel}
                    onClose={handleClose}
                    width="80%"/>
            </Box>
        </Box>

    )

}