# Installation at IHME

## Plugins
Jobmon runs jobs on IHME's Slurm cluster.
Jobmon can also execute Tasks locally on a single machine using either
sequential execution or multiprocessing, although these operating modes are really
only suitable for testing or proof of concept.

To use either of the clusters with Jobmon users need to install their Jobmon plugin. If a user
wants to use Slurm with Jobmon, they would need to have the core Jobmon software and the
Jobmon Slurm plugin installed.

Users can either:
1. install Jobmon core and the plugins individually using "pip," or
2. install Jobmon core and the Slurm plugin together with a single conda command.

## Conda install
To install core Jobmon and both plugins using conda:
```shell
    conda install ihme_jobmon -k --channel https://artifactory.ihme.washington.edu/artifactory/api/conda/conda-scicomp --channel conda-forge
```
##  pip install
To install just core jobmon (no cluster plugins) via pip:
```shell
    pip install jobmon
```
To install the preconfigured Slurm plugin:
```shell
    pip install jobmon_installer_ihme
``` 
To install both at once via pip:
```shell
    pip install jobmon[ihme]
```
Then issue a "jobmon_config update" command to configure the web service and port, as described on
the hub at [Jobmon Conda Versions](https://hub.ihme.washington.edu/display/DataScience/Jobmon+Conda+Versions)


> **_NOTE:_**
    If you get the error **"Could not find a version that satisfies the requirement jobmon (from version: )"** then create (or append) the following to your ``~/.pip/pip.conf``:
    
        [global]
        extra-index-url = https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
        trusted-host = artifactory.ihme.washington.edu

## Running on the Slurm Cluster
When running your Jobmon Workflow on the production Slurm cluster (gen-slurm-slogin-p01.cluster.ihme.washington.edu) 
please make sure that you set your ``cluster_name`` to ``slurm`` in Jobmon.

When running your Jobmon Workflow on the test Slurm cluster (gen-slurm-slogin-s01.cluster.ihme.washington.edu) please 
make sure  that you set your ``cluster_name`` to ``slurm_stage`` in Jobmon.

You can set your queue to all.q, long.q, or d.q on both the production and test slurm clusters.

## Jobmon Database
The Jobmon database is hosted in Azure, the database information (read-only) is as follows:
```
Host: scicomp-mysql-mysql-p01.database.azure.com
Username: read_only
Password: docker
Database: docker
Port: 3306
```

Please note to access the Jobmon database you need to switch your VPN to "All Internet Traffic" in your Big-IP Edge Client.

## Jobmon Learning
For a deeper dive in to Jobmon, check out some of our courses:
1. [About Jobmon](https://hub.ihme.washington.edu/pages/viewpage.action?pageId=74531156)
2. [Learn Jobmon](https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062050)
3. [Jobmon Retry](https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062056)

Check [IHME Learn](https://ihme.brightspace.com>) to see if there are any upcoming trainings.

## Jobmon GUI
You can view all your workflows, monitor their progress, and dive into the details
of their tasks using the [Jobmon GUI](https://jobmon-gui.ihme.washington.edu)