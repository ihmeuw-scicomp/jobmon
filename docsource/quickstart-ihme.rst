Installation at IHME
####################

Plugins
*******
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

Conda install
*************
To install core Jobmon and both plugins using conda::

    conda install ihme_jobmon -k --channel https://artifactory.ihme.washington.edu/artifactory/api/conda/conda-scicomp --channel conda-forge

Pip install
***********
To install just core jobmon (no cluster plugins) via pip::

    pip install jobmon

To install the preconfigured Slurm plugin::

    pip install jobmon_installer_ihme

To install both at once via pip::

    pip install jobmon[ihme]


See versions at:
`Jobmon Conda Versions <https://hub.ihme.washington.edu/display/DataScience/Jobmon+Conda+Versions>`_


.. note::
    If you get the error **"Could not find a version that satisfies the requirement jobmon (from version: )"** then create (or append) the following to your ``~/.pip/pip.conf``::

        [global]
        extra-index-url = https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
        trusted-host = artifactory.ihme.washington.edu


New Releases
************
New Jobmon releases, including updates to jobmon_installer_ihme, will be announced in the #jobmon_users Slack channel.
Release details- such as installer version, dates and included packages, are also available on the HUB at
`Jobmon Conda Versions. <https://hub.ihme.washington.edu/display/DataScience/Jobmon+Conda+Versions>`_

Jobmon Learning
###############
For a deeper dive in to Jobmon, check out some of our courses:
    1. `About Jobmon <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=74531156>`_
    2. `Learn Jobmon <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062050>`_
    3. `Jobmon Retry <https://hub.ihme.washington.edu/pages/viewpage.action?pageId=78062056>`_

Check `IHME Learn <https://ihme.brightspace.com>`_ to see if there are any
upcoming trainings.

Jobmon GUI
##########
You can view all your workflows, monitor their progress, and dive into the details
of their tasks using the `Jobmon GUI <https://jobmon-gui.ihme.washington.edu>`_.