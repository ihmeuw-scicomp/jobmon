import {
  Button,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Select,
  TextField,
} from '@mui/material';
import { DatePicker, LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import dayjs from 'dayjs';
import React, { useEffect } from 'react';
import { useWorkflowSearchSettings } from '@jobmon_gui/stores/workflow_settings';
import { useQueryClient } from '@tanstack/react-query';
import Box from '@mui/material/Box';

export default function WorkflowFilters() {
  const queryClient = useQueryClient();
  const workflowSettings = useWorkflowSearchSettings();

  useEffect(() => {
    workflowSettings.loadValuesFromSearchParams(
      new URLSearchParams(window.location.search)
    );
  }, []);

  const handleClear = () => {
    workflowSettings.clear();
    workflowSettings.triggerDataRefresh();
    void queryClient.invalidateQueries({
      queryKey: ['workflow_overview', 'workflows'],
    });
  };

  const handleSubmit = event => {
    event.preventDefault();
    workflowSettings.applyPendingSettings();
    workflowSettings.triggerDataRefresh();
    void queryClient.invalidateQueries({
      queryKey: ['workflow_overview', 'workflows'],
    });
  };

  const handleInputChange = (key, value) => {
    workflowSettings.setPendingSetting(key, value);
  };

  return (
    <Box className="div-level-2">
      <form onSubmit={handleSubmit}>
        <Grid container spacing={2}>
          <Grid item xs={3}>
            <TextField
              label="Username"
              fullWidth={true}
              value={workflowSettings.getPending().user}
              onChange={e => handleInputChange('user', e.target.value)}
            />
          </Grid>
          <Grid item xs={3}>
            <TextField
              label="Workflow Args"
              fullWidth={true}
              value={workflowSettings.getPending().wf_args}
              onChange={e => handleInputChange('wf_args', e.target.value)}
            />
          </Grid>
          <Grid item xs={1.5}>
            <TextField
              label="WF Attribute Key"
              fullWidth={true}
              value={workflowSettings.getPending().wf_attribute_key}
              onChange={e =>
                handleInputChange('wf_attribute_key', e.target.value)
              }
            />
          </Grid>
          <Grid item xs={1.5}>
            <TextField
              label="WF Attribute Value"
              fullWidth={true}
              value={workflowSettings.getPending().wf_attribute_value}
              onChange={e =>
                handleInputChange('wf_attribute_value', e.target.value)
              }
            />
          </Grid>
          <Grid item xs={3}>
            <TextField
              label="Tool"
              fullWidth={true}
              value={workflowSettings.getPending().tool}
              onChange={e => handleInputChange('tool', e.target.value)}
            />
          </Grid>
          <Grid item xs={1.5}>
            <LocalizationProvider dateAdapter={AdapterDayjs}>
              <DatePicker
                label={'Submitted Date Start'}
                value={
                  workflowSettings.getPending().date_submitted
                    ? dayjs(workflowSettings.getPending().date_submitted)
                    : dayjs().subtract(2, 'weeks')
                }
                onChange={value =>
                  handleInputChange('date_submitted', dayjs(value))
                }
                sx={{ width: '100%' }}
              />
            </LocalizationProvider>
          </Grid>
          <Grid item xs={1.5}>
            <LocalizationProvider dateAdapter={AdapterDayjs}>
              <DatePicker
                label={'Submitted Date End'}
                value={
                  workflowSettings.getPending().date_submitted_end
                    ? dayjs(workflowSettings.getPending().date_submitted_end)
                    : dayjs()
                }
                onChange={value =>
                  handleInputChange('date_submitted_end', dayjs(value))
                }
                sx={{ width: '100%' }}
              />
            </LocalizationProvider>
          </Grid>
          <Grid item xs={3}>
            <TextField
              label="Workflow Name"
              fullWidth={true}
              value={workflowSettings.getPending().wf_name}
              onChange={e => handleInputChange('wf_name', e.target.value)}
            />
          </Grid>
          <Grid item xs={3}>
            <FormControl fullWidth={true}>
              <InputLabel id="LABEL-workflow-status">
                Workflow Status
              </InputLabel>
              <Select
                labelId="LABEL-workflow-status"
                label="Workflow Status"
                id={'SELECT-workflow-status'}
                onChange={e => handleInputChange('status', e.target.value)}
                value={workflowSettings.getPending().status}
                fullWidth={true}
              >
                <MenuItem>{undefined}</MenuItem>
                <MenuItem value="A">Aborted</MenuItem>
                <MenuItem value="D">Done</MenuItem>
                <MenuItem value="F">Failed</MenuItem>
                <MenuItem value="G">Registering</MenuItem>
                <MenuItem value="H">Halted</MenuItem>
                <MenuItem value="I">Instantiating</MenuItem>
                <MenuItem value="O">Launched</MenuItem>
                <MenuItem value="Q">Queued</MenuItem>
                <MenuItem value="R">Running</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={3}>
            <TextField
              label="Workflow ID"
              fullWidth={true}
              value={workflowSettings.getPending().wf_id}
              onChange={e => handleInputChange('wf_id', e.target.value)}
            />
          </Grid>
          <Grid item xs={12}>
            <Grid container spacing={2}>
              <Grid item xs={4} />
              <Grid item xs={2}>
                <Button variant="contained" type="submit">
                  Submit
                </Button>
              </Grid>
              <Grid item xs={2}>
                <Button variant="contained" onClick={handleClear}>
                  Clear All
                </Button>
              </Grid>
              <Grid item xs={4} />
            </Grid>
          </Grid>
        </Grid>
      </form>
    </Box>
  );
}
