# Jobmon GUI

A GUI to visualize Jobmon Workflows.

## Overview

This application uses React, FastApi, and Bootstrap.

## Roadmap

You can see the roadmap of the upcoming deployments here: https://hub.ihme.washington.edu/display/DataScience/Jobmon+GUI+Design+and+Initial+Roadmap

## Testing Locally

### Deploying the FastApi Server App Locally

To deploy the FastApi app locally:

1. Open a terminal
2. Make a conda environment and activate it
3. Install `nox` by running `conda install conda-forge::nox`
4. Navigate to the top of the Jobmon repository
5. Run `nox -s launch_gui_test_server`
6. Run `conda activate ./nox/launch_gui_test_server`
6. Run `python jobmon_gui/local_testing/jobmon_gui/testing_servers/functionaltest_server.py`
    - This command will spin up a local version of the Flask backend, running on 127.0.0.1:8070 by default. You can then configure the React app to point to this URL for testing purposes.

### Deploying the React App Locally

To deploy the React app locally:

1. Open a new terminal
2. Install bun
3. Navigate to the jobmon_gui subdirectory
4. Run `bun install`
5. Run `bun start`

You can then access the site at: http://localhost:3000

## Deploying to Kubernetes

The Jobmon GUI is deployed via the primary Jobmon Jenkins deployment pipelines i.e. you cannot deploy the Jobmon GUI with out deploying Jobmon and vice versa.
The build follows the dev (dev k8s cluster) -> stage (dev k8s cluster) -> prod deployment (prod k8s cluster) process:

- Development Jenkins pipeline: https://jenkins.scicomp.ihme.washington.edu/job/jobmon/job/release/job/jobmon.dev.deploy/
- Stage Jenkins pipeline: https://jenkins.scicomp.ihme.washington.edu/job/jobmon/job/release/job/jobmon.stage.deploy/
- Prod Jenkins pipeline: https://jenkins.scicomp.ihme.washington.edu/job/jobmon/job/release/job/jobmon.prod.deploy/

If the pipelines were successful you should be able to see the webpage and the pods spun up in Rancher.
