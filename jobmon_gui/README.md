# Jobmon GUI

A GUI to visualize Jobmon Workflows.

## Overview

This application uses React, FastApi, and Bootstrap.

## Testing Locally

### Deploying the Jobmon Server Backend Locally

To deploy the Jobmon Server Backend locally:

1. Open a terminal
2. Install `nox` by running `conda install conda-forge::nox`
3. Navigate to the top of the Jobmon repository
4. Run `nox -s build_gui_test_env`
5. Run `conda activate ./.nox/build_gui_test_env`
6. Run `python jobmon_gui/local_testing/main.py`
    - This command will spin up a local version of the Jobmon Server, running on 127.0.0.1:8070 by default. You can then configure the React app to point to this URL for testing purposes.
7. Run `python jobmon_gui/local_testing/create_wfs.py`

**NOTE**: If you're running against the Jobmon production database you need to make sure that mysqlclient is installed 
via conda, and not installed via pip. You will see auth errors if this is incorrectly installed. Do the following:

1. `pip uninstall mysqlclient`
2. `conda install mysqlclient`

### Deploying the Jobmon GUI Frontend Locally

To deploy the Jobmon GUI Frontend locally:

1. Open a new terminal
2. Install bun
3. Navigate to the jobmon_gui subdirectory
4. Run `bun install`
5. Run `bun start`

You can then access the site at: http://localhost:3000
