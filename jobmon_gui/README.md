# Jobmon GUI

A GUI to visualize Jobmon Workflows.

## Overview

This application uses React, FastApi, and Bootstrap.

## Testing Locally (docker-compose)

### Spin up the backend and frontend servers

From the root of the repository run.

1. `docker-compose build jobmon_server jobmon_frontend`
2. `docker-compose up`

### Then run test workflows

From the root of the repository activate a python environment of your choice and then run.

1. `pip install -e ./jobmon_core ./jobmon_client`
2. `python jobmon_gui/local_testing/create_wfs.py`

## Testing Locally (bare metal)

### Deploying the Jobmon Server Backend Locally

To deploy the Jobmon Server Backend locally:

1. Open a terminal
2. Make a conda environment and activate it
3. Install `nox` by running `conda install conda-forge::nox`
4. Navigate to the top of the Jobmon repository
5. Run `nox -s build_gui_test_env`
6. Run `conda activate ./.nox/build_gui_test_env`
7. Run `python jobmon_gui/local_testing/main.py`
    - This command will spin up a local version of the Jobmon Server, running on 127.0.0.1:8070 by default. You can then configure the React app to point to this URL for testing purposes.
8. Run `python jobmon_gui/local_testing/create_wfs.py`

### Deploying the Jobmon GUI Frontend Locally

To deploy the Jobmon GUI Frontend locally:

1. Open a new terminal
2. Install bun
3. Navigate to the jobmon_gui subdirectory
4. Run `bun install`
5. Run `bun start`

You can then access the site at: http://localhost:3000
