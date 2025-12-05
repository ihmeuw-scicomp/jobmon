**********************
Continuous Integration
**********************

.. _jobmon-continuous-integration-label:

Jobmon uses Jenkins for its continuous integration. Below you can see all of the pipelines
that are in place to test and deploy Jobmon and the plugins.

Metallb Deploy
**************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline needs to be run every time that a change is made to the scicomp-cluster-metallb.yml
       | This means that it needs to run anytime a new IP address is added to address pools.
   * - Jenkinsfile Path
     - Scicomp/metallb-scicomp/Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/metallb-deploy/
   * - Build Parameters
     - There are no build parameters in this pipeline.

Jobmon UGE Pipelines (Deprecated)
*********************************

.. warning::
    The UGE plugin and its CI pipelines are deprecated and no longer in active use.
    The sections below are preserved for historical reference only.

Jobmon UGE PR Opened
^^^^^^^^^^^^^^^^^^^^
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline was automatically kicked off when a PR was opened in the Jobmon UGE Plugin.
       | It ran the test suite, lints, typechecks and built the docs.
   * - Jenkinsfile Path
     - Scicomp/jobmon_uge/ci/pr_opened.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_uge_pr_opened/

Jobmon UGE PR Merged
^^^^^^^^^^^^^^^^^^^^
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline was automatically kicked off when a PR was merged in the Jobmon UGE Plugin.
   * - Jenkinsfile Path
     - Scicomp/jobmon_uge/ci/pr_merged.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_uge_pr_merged/

Jobmon Slurm PR Opened
**********************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline is automatically kicked off when a PR is opened in the Jobmon Slurm Plugin.
       | It runs the test suite, lints, typechecks and builds the docs.
       | A developer is not allowed to merge their PR unless this build passes.
   * - Jenkinsfile Path
     - Scicomp/jobmon_slurm/ci/pr_opened.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_slurm_pr_opened/
   * - Build Parameters
     - There are no build parameters in this pipeline.

Jobmon Slurm PR Merged
**********************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline is automatically kicked off when a PR is merged in the Jobmon Slurm Plugin.
       | It runs the test suite, lints, typechecks and builds the docs on the branch the PR was just merged to.
   * - Jenkinsfile Path
     - Scicomp/jobmon_slurm/ci/pr_merged.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_slurm_pr_merged/
   * - Build Parameters
     - * BRANCH_TO_BUILD - A drop down menu of different branches that are built.
       * DEPLOY_TO_PYPI - Whether or not the pipeline should deploy this version of Jobmon to pypi.

Jobmon PR Opened
****************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline is automatically kicked off when a PR is opened in Jobmon Core.
       | It runs the test suite, lints, typechecks and builds the docs.
       | A developer is not allowed to merge their PR unless this build passes.
   * - Jenkinsfile Path
     - Scicomp/jobmon/ci/pr_opened.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_pr_opened/
   * - Build Parameters
     - There are no build parameters in this pipeline.

Jobmon PR Merged
****************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline is automatically kicked off when a PR is merged in Jobmon Core.
       | It runs the test suite, lints, typechecks and builds the docs on the branch the PR was just merged to.
   * - Jenkinsfile Path
     - Scicomp/jobmon/ci/pr_merged.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_pr_merged/
   * - Build Parameters
     - * BRANCH_TO_BUILD - A drop down menu of different branches that are built.
       * DEPLOY_TO_PYPI - Whether or not the pipeline should deploy this version of Jobmon to pypi.

Jobmon Deploy Server
********************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline is run whenever SciComp wants to release a new version of Jobmon.
       | It builds the Jobmon server containers and deploys them to Kubernetes.
   * - Jenkinsfile Path
     - Scicomp/jobmon/ci/deploy_server.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_deploy_server/
   * - Build Parameters
     - * JOBMON_VERSION - The version of Jobmon to deploy (git tag) e.g. 3.0.3.
       * K8S_NAMESPACE - Kubernetes Namespace to deploy to e.g jobmon-prod-3-0-3.
       * K8S_REAPER_NAMESPACE - Kubernetes Namespace to deploy to e.g. jobmon-reapers.
       * METALLB_IP_POOL - Name of the MetalLB IP Pool you wish to get IPs from (see metallb-scicomp repository)
       * RANCHER_DB_SECRET - Name of rancher secret to use for database variables.
       * USE_LOGSTASH - Whether to forward event logs to Logstash or not.
       * RANCHER_SLACK_SECRET - Name of rancher secret to use for Slack variables.
       * RANCHER_QPID_SECRET - Name of rancher secret to use for QPID variables.
       * RANCHER_PROJECT_ID - Rancher project must be created in the rancher web UI before running this job. Get this from the URL after you select the project in the rancher UI. Variable shouldn't change often.
       * DEPLOY_JOBMON - Whether or not you want to deploy Jobmon.
       * DEPLOY_ELK - Whether or not you want to deploy the ELK stack.
       * LOG_ROTATION - Whether or not you want to config log rotation for ElasticSearch.

Jobmon Deploy Conda
*******************
.. list-table::
   :widths: 25 75

   * - Overview
     - | This pipeline is run whenever SciComp wants to release a new version of Jobmon.
       | It builds the Jobmon Conda distribution and the IHME distribution and and uploads them.
   * - Jenkinsfile Path
     - Scicomp/jobmon/ci/deploy_conda.Jenkinsfile
   * - Jenkins URL
     - https://jenkins.scicomp.ihme.washington.edu/job/scicomp/job/jobmon_deploy_conda/
   * - Build Parameters
     - * CONDA_CLIENT_VERSION - The version to be associated with this conda client release.
       * JOBMON_VERSION - The version of Jobmon Core you wish to deploy.
       * JOBMON_UGE_VERSION - The version of Jobmon UGE you wish to deploy.
       * JOBMON_SLURM_VERSION - The version of Jobmon Slurm you wish to deploy.
       * SLURM_REST_VERSION - The version of Slurm Rest.
       * K8S_NAMESPACE - To Kubernetes Namespace to deploy to.
