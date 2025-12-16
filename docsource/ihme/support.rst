************
IHME Support
************

Getting Help
============

If you need help with Jobmon at IHME, here are your options:

Slack
-----

The primary channel for Jobmon support is Slack:

- **Channel**: ``#jobmon-users``
- **Response time**: Usually within a few hours during business hours

When asking for help, please include:

1. Your workflow ID (if applicable)
2. The error message you're seeing
3. What you were trying to do
4. Any relevant code snippets

Jobmon GUI
----------

The Jobmon GUI is often the fastest way to diagnose issues:

- **URL**: https://jobmon-gui.ihme.washington.edu
- View workflow status and task details
- See error messages and logs
- Check resource usage

Documentation
-------------

- **This documentation**: https://jobmon.readthedocs.io
- **GitHub**: https://github.com/ihmeuw-scicomp/jobmon

Common Issues
=============

"DistributorNotAlive" Error
---------------------------

This usually means you're running from a login node instead of a submit node:

.. code-block:: bash

   # Run srun first
   srun --pty bash
   
   # Then run your workflow
   python my_workflow.py

Connection Errors
-----------------

If you can't connect to the Jobmon server:

1. Make sure you're on the IHME network (or VPN)
2. Check if the server is up: https://jobmon-gui.ihme.washington.edu
3. Verify your configuration file

Resource Errors
---------------

If jobs are failing due to resource limits:

1. Check the actual resource usage in the GUI
2. Increase memory or runtime as needed
3. Consider using a fallback queue for large jobs

For more troubleshooting tips, see :doc:`/advanced/troubleshooting`.

Office Hours
============

Scientific Computing holds regular office hours. Check the IHME intranet 
or Slack for the current schedule.

Reporting Bugs
==============

If you find a bug in Jobmon:

1. Check if it's already reported: https://github.com/ihmeuw-scicomp/jobmon/issues
2. If not, create a new issue with:
   - Steps to reproduce
   - Expected behavior
   - Actual behavior
   - Jobmon version (``python -c "import jobmon.client; print(jobmon.client.__version__)"``)

