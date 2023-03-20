# Jobmon GUI
A GUI to visualize Jobmon Workflows.

## Overview
This application uses React, Flask, and Bootstrap.

The Jobmon GUI was originally created by create-react-app.

## Roadmap
You can see the roadmap of the upcoming deployments here: https://hub.ihme.washington.edu/display/DataScience/Jobmon+GUI+Design+and+Initial+Roadmap

## Testing Locally

### Deploying the Flask Server App Locally
To deploy the Flask app locally:

1. Open a terminal
2. Make a conda environment and activate it
3. Navigate to the top of the Jobmon repository
4. Run `nox -s launch_gui_test_server`
5. Run `python jobmon_gui/local_testing/jobmon_gui/testing_servers/functionaltest_server.py`
    - This command will spin up a local version of the Flask backend, running on 127.0.0.1:8070 by default. You can then configure the React app to point to this URL for testing purposes.

### Deploying the React App Locally
To deploy the React app locally:

1. Open a new terminal
2. Install npm
3. Navigate to the jobmon_gui subdirectory
4. Run `npm install`
5. Run `npm start`

Note: When running locally the React app uses the Webpack Dev Server to serve its assets.

You can then access the site at: http://localhost:3000

## Deploying to Kubernetes
To deploy the Jobmon GUI:

1. Build the images with the `build_gui_image` Jenkins pipeline: https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_TAD/job/build_gui_image/
2. Deploy the images with the `deploy_gui` Jenkins pipeline: https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_TAD/job/deploy_gui/
3. If a production deployment tag the commit in Bitbucket.
4. If the pipelines were successful you should be able to 1. see the webpage and 2. the pods spun up in the provided namespace in Rancher.

Note: Please deploy to development and have the team look at the GUI changes before deploying to production.