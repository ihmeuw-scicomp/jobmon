:orphan:

************************
Monitoring and Debugging
************************

Graphical User Interface (GUI)
##############################
The Jobmon GUI allows users to see the status of their workflows.
In general any status query in the CLI is also available in the GUI
and should be easier to find.

The GUI is at: `<https://jobmon-gui.ihme.washington.edu>`_

.. _status-commands-label:

Jobmon Command Line Interface (CLI) Status Commands
###################################################
The Jobmon status commands allow you to check that status of your Workflows and Tasks from the
command line.

To use the status commands:
    1. Open a new terminal window
    2. SSH in to the cluster slogin node
    3. srun
    4. Activate the same conda environment that your tasks are running in

There are currently three supported commands:

workflow_status
***************
    Entering ``jobmon workflow_status`` at the command line will show you
    a table of the number of tasks that are in each state within that workflow. You
    can specify the workflow by user using the -u flag. For example:
    ``jobmon workflow_status -u {user}``. You can also specify the workflow
    using the -w flag. For example: ``jobmon workflow_status -w 9876``.
    You can also use the -w flag to specify multiple workflows at the same
    time. For example, if you have one workflow named 9876 and one
    workflow named 1234 you would enter ``jobmon workflow_status -w 9876 1234``.

workflow_tasks
**************
    Entering ``jobmon workflow_tasks`` in to the command line will show you
    the status of specific tasks in a given workflow. You can specify which
    workflow with the -w flag. For example: ``jobmon workflow_tasks -w 9876``.
    You can also add a -s flag to only query tasks that are in a certain
    state. For example: ``jobmon workflow_tasks -w 9876 -s PENDING`` will query all
    tasks within workflow 9876 that have the pending status. You may also query by multiple
    statuses. For example: ``jobmon workflow_tasks -w 9876 -s PENDING RUNNING``

.. _task_status-commands-label:

task_status
***********
    Entering ``jobmon task_status`` in to the command line will show you the
    state of each task instance for a certain task. You may specify the task
    by adding a -t flag. For example: ``jobmon task_status -t 1234``. You may also filter by
    multiple task ids and statuses. The -s flag will allow you to filter upon a specific status.
    For example, if you wanted to query all task instances in the Done state for task 1234 and
    task 7652 you would do the following ``jobmon task_status -t 1234 7652 -s done``

JSON Flag
*********
    A new flag has been added to the Jobmon CLI to allow users to return their workflow and
    task statuses in JSON format. To use this feature add a ``-n`` flag to any of the Jobmon
    CLI commands. For example: ``jobmon task_status -t 1234 7652 -s done -n``

Possible states: PENDING, RUNNING, DONE, FATAL

task_dependencies
*****************
    Entering ``jobmon task_dependencies`` in to the command line will show users what tasks
    are upstream and downstream of the task provided. Users will specify the task by adding a
    -t flag. For example: ``jobmon task_dependencies -t 1672``. The ouput returned will look
    like::

        Upstream Tasks:

           Task ID         Status
           1               D

        Downstream Tasks:

           Task ID         Status
           3               D
           4               D

get_filepaths
*************

Due to the introduction of array task submission in Jobmon 3.1, sometimes the structure of the output filepaths isn't
intuitive to human operators. Jobmon provides a ``jobmon get_filepaths`` CLI tool to retrieve a more useful representation
of the location of your log files based on the provided compute_resources.

You can filter the results by workflow ID, array name, or job name using the -w, -a, -j flags respectively.
By default the query returns 5 results but you can always increase the limit using the -l flag.

Jobmon Database
###############

Jobmon Database API
*******************

This API allows users to query the database for detailed resource usage information. There are currently two routes:
    1. **/wf_resource_usage** - which allows users to query resource usage by Workflow ID.
    2. **/tool_resource_usage** - which allows users to query resource usage by Tool name.

Base URL
--------

All API requests should be made to the base URL provided on the **"Jobmon at IHME"** page, accessible through
the Jobmon GUI.

Authentication
--------------

Currently, the Database API does not require authentication. In future releases, this may be updated, so check this
section for any changes.

Resource Usage by Workflow ID
-----------------------------
**Endpoint:** `GET <base_url>/workflow/<workflow_id>/wf_resource_usage`

**Parameters:**
    - Workflow ID

**Description**: Gets the resource usage for all TaskInstances associated with a provided workflow ID.

**Example Response**
    - **Status Code:** `200 OK`
    - **Content-Type:** `application/json`
    - **Body:**

      .. code-block::

          [
              {
                  "task_id": 6213741,
                  "task_name": "random_task_name",
                  "task_status": "D",
                  "task_num_attempts": 2,
                  "task_instance_id": 91823412,
                  "ti_usage_str": "wallclock=8 cpu=1, mem=7 GBs, io=5 GB",
                  "ti_wallclock": 8,
                  "ti_maxrss": 7,
                  "ti_maxpss": 5,
                  "ti_cpu": 1,
                  "ti_io": 5,
                  "ti_status": "D"
              }
          ]



Resource Usage by Tool Name
-----------------------------------
**Endpoint:** `GET <base_url>/tool/<tool_name>/tool_resource_usage?start_date=<YYYY-MM-DD>&end_date=<YYYY-MM-DD>`

**Parameters:**
    - Tool name
    - Start date
    - End date

**Description**: Pass a tool name to the route and it will return the requested resources, utilized resources and
node_args for TaskInstances associated with that tool. NOTE: it is required to pass a start and end date.

**Example Response**
    - **Status Code:** `200 OK`
    - **Content-Type:** `application/json`
    - **Body:**

      .. code-block::

        [
            {
                "node_arg_val": "--provenance True",
                "ti_id": 12345677,
                "ti_maxrss": 50844672,
                "ti_requested_resources": {"runtime": 21600, "memory": 10},
                "ti_wallclock": 20
            },
            {
                "node_arg_val": "--intrinsic False",
                "ti_id": 12345678,
                "ti_maxrss": 43960320,
                "ti_requested_resources": {"runtime": 21600, "memory": 10},
                "ti_wallclock": 22
            }
        ]

Additional Routes
-----------------
If there are any other routes or features you would like to see in the API, please let the SciComp team know.

Common Errors
*************

TaskInstance NO_DISTRIBUTOR_ID
------------------------------
A NO_DISTRIBUTOR_ID error typically indicates an issue when Jobmon calls the underlying Slurm API. For more details,
check the error in the Standard Error column of the TaskInstances table on the Task Details page in the Jobmon GUI.
Common causes include insufficient permissions to submit jobs to a particular partition or exceeding resource limits
for a specific queue.

DistributorNotAlive Error
-------------------------
This issue usually occurs when a user runs their command from a login node instead of a submit node. Try calling `srun`
and see if that resolves the error.

Running Queries
***************
If the command line status commands do not provide the information you need,
you can query the Jobmon database.

You can view the Jobmon database connection information here: `<https://jobmon-gui.ihme.washington.edu/#/jobmon_at_ihme>`_

.. include:: database-ihme.rst

.. note::
    Jobmon has a persistent database. This means any time the client side of Jobmon is updated
    it will continue to use the same database. The database credentials will only change when
    database changes are implemented.

You can query the Jobmon database to see the status of a whole Workflow, or any set of tasks.
Open a SQL browser (e.g. Sequel Pro) and connect to the database defined above.

Useful Jobmon Queries
*********************
If you wanted the current status of all Tasks in workflow 191:
    | SELECT status, count(*)
    | FROM task
    | WHERE workflow_id = <workflow_id>
    | GROUP BY status

To find your Workflow if you know the Workflow name:
    | SELECT *
    | FROM workflow
    | WHERE name="<your workflow name>"

To find all of your Workflows by your username:
    | SELECT *
    | FROM workflow
    | JOIN workflow_run ON workflow.id = workflow_run.workflow_id
    | WHERE workflow_run.user = "<your username>"

To get all of the error logs associated with a given Workflow:
    | SELECT *
    | FROM task t1, task_instance t2, task_instance_error_log t3
    | WHERE t1.id = t2.task_id
    | AND t2.id = t3.task_instance_id
    | AND t1.workflow_id = <workflow id>

To get the error logs for a given WorkflowRun:
    | SELECT *
    | FROM task_instance t1, task_instance_error_log t2
    | WHERE t1.id = t2.task_instance_id
    | AND t1.workflow_run_id = <workflow_run_id>

Database Tables
###############

arg
***
    A list of args that the node_args and task_args use.

arg_type
********
    The different types of arguments (NODE_ARG, TASK_ARG, OP_ARG). For more information on
    argument types see, :ref:`jobmon-arguments-label`.

array
*****
    Groups of tasks that are submitted together as Slurm job arrays. Contains the array name,
    associated task template version, workflow, and concurrency settings.

cluster
*******
    A list of clusters that Jobmon is able to run jobs on (e.g., Slurm).

cluster_type
************
    A list of cluster types that Jobmon can run jobs on. Currently includes dummy, sequential,
    multiprocess, and Slurm.

dag
***
    This table has every entry for every DAG (Directed Acyclic Graph) created, as identified
    by it's ID and hash.

edge
****
    A table that shows the relationship between a specific node and it's upstream and
    downstream nodes.

node
****
    The object representing a Task within a DAG. Table includes the ID of the TaskTemplate
    version and the hash of the node args.

node_arg
********
    Arguments that identify a unique node within the DAG. For more information on node
    arguments see, :ref:`jobmon-node-args-label`.

queue
*****
    A table that lists all of the available queues for a given cluster. It also provides the
    resource bounds (minimum and maximum value for cores, runtime and memory) of a queue and
    the default resources of a queue.

task
****
    A single executable object in the workflow. The table includes the name of the task, the
    command it submitted, and it's task resource ID.

task_arg
********
    A list of args that make a command unique across different workflows, includes task_id,
    arg_id and the associated value. For more information on task arguments see,
    :ref:`jobmon-task-args-label`.

task_attribute
**************
    A table that tracks optional additional attributes of a task. For example, release ID or
    location set version ID.

task_attribute_type
*******************
    Types of task attributes that can be tracked.

task_instance
*************
    Table that tracks the actual runs of tasks. The table includes the workflow_run_id,
    cluster_type_id, and task_id associated with the task instance. It also includes what node
    the task instance ran on.

task_instance_error_log
***********************
    Any errors that are produced by a task instance are logged in this table.

task_instance_status
********************
    Meta-data table that defines the fourteen states of Task Instance. For more information see
    status section below.

task_resources
**************
    The resources that were requested for a Task. Resources include: memory, cores, runtime,
    queue, and project.

task_resources_type
*******************
    This table is used mostly for internal Jobmon functionality. There are three types of task
    resources: original (the resources requested by the user), validated (the requested
    resources that have been validated against the provided queue), adjusted (resources that
    have been scaled after a task instance failed due to a resource error).

task_status
***********
    Meta-data table that defines the nine states of Task. For more information, see the status
    section below.

task_template
*************
    This table has every TaskTemplate, paired with it's tool_version_id.

task_template_version
*********************
    A table listing the different versions a TaskTemplate can have.

template_arg_map
****************
    A table that maps TaskTemplate versions to argument IDs.

tool
****
    A table that shows the list of Tools that can be associated with your Workflow and
    TaskTemplates.

tool_version
************
    A table listing the different versions a Tool has.

workflow
********
    This table has every Workflow created. It includes the name of the workflow, the tool
    version it's associated with, and the DAG that it's associated with.

workflow_attribute
******************
    A table that lists optional additional attributes that are being tracked for a given
    Workflow.

workflow_attribute_type
***********************
    The types of attributes that can be tracked for Workflows.

workflow_run
************
    This table has every run of a workflow, paired with it's workflow, as identified by
    workflow_id. It also includes what user ran the workflow and the run status.

workflow_run_status
*******************
    Meta-data table that defines the thirteen states of Workflow Run. For more information see
    the status section below.

workflow_status
***************
    Meta-data table that defines nine states of Workflow.

Jobmon Statuses
###############

Workflow Statuses
*****************
.. list-table::
   :widths: 10 35 50
   :header-rows: 1

   * - ID
     - Label
     - Description
   * - A
     - ABORTED
     - Workflow encountered an error before a WorkflowRun was created.
   * - D
     - DONE
     - Workflow has completed, it finished successfully.
   * - F
     - FAILED
     - Workflow unsuccessful in one or more WorkflowRuns, no runs finished successfully as DONE.
   * - G
     - REGISTERING
     - Workflow is being validated.
   * - H
     - HALTED
     - Resume was set and Workflow is shut down or the controller died and therefore Workflow was reaped.
   * - I
     - INSTANTIATING
     - Jobmon Scheduler is creating a Workflow on the distributor.
   * - O
     - LAUNCHED
     - Workflow has been created. Distributor is now controlling tasks, or waiting for scheduling loop.
   * - Q
     - QUEUED
     - Jobmon client has updated the Jobmon database, and signalled Scheduler to create Workflow.
   * - R
     - RUNNING
     - Workflow has a WorkflowRun that is running.

WorkflowRun Statuses
********************
.. list-table::
   :widths: 10 35 50
   :header-rows: 1

   * - ID
     - Label
     - Description
   * - A
     - ABORTED
     - WorkflowRun encountered problems while binding so it stopped.
   * - B
     - BOUND
     - WorkflowRun has been bound to the database.
   * - C
     - COLD_RESUME
     - WorkflowRun is set to resume as soon all existing tasks are killed.
   * - D
     - DONE
     - WorkflowRun is Done, it successfully completed.
   * - E
     - ERROR
     - WorkflowRun did not complete successfully, either some Tasks failed or (rarely) an internal Jobmon error.
   * - G
     - REGISTERED
     - WorkflowRun has been validated and registered.
   * - H
     - HOT_RESUME
     - WorkflowRun was set to hot-resume while tasks are still running, they will continue running.
   * - I
     - INSTANTIATED
     - Scheduler is instantiating a WorkflowRun on the distributor.
   * - L
     - LINKING
     - WorkflowRun is linking tasks to the workflow.
   * - O
     - LAUNCHED
     - Instantiation complete. Distributor is controlling Tasks or waiting for scheduling loop.
   * - R
     - RUNNING
     - WorkflowRun is currently running.
   * - S
     - STOPPED
     - WorkflowRun was deliberately stopped, probably due to keyboard interrupt from user.
   * - T
     - TERMINATED
     - This WorkflowRun is being replaced by a new WorkflowRun created to pick up remaining Tasks, this WorkflowRun is terminating.

Task Statuses
*************
.. list-table::
   :widths: 10 35 50
   :header-rows: 1

   * - ID
     - Label
     - Description
   * - A
     - ADJUSTING_RESOURCES
     - Task errored with a resource error, the resources will be adjusted before retrying.
   * - D
     - DONE
     - Task is Done, it ran successfully to completion; it has a TaskInstance that successfully completed.
   * - E
     - ERROR_RECOVERABLE
     - Task has errored out but has more attempts so it will be retried.
   * - F
     - ERROR_FATAL
     - Task errored out and has used all of the attempts, therefore has failed for this WorkflowRun. It can be resumed in a new WorkflowRun.
   * - G
     - REGISTERING
     - Task is being registered and bound to the database.
   * - I
     - INSTANTIATING
     - Task is being instantiated within Jobmon.
   * - O
     - LAUNCHED
     - Task has been launched and is waiting to run.
   * - Q
     - QUEUED
     - Task's dependencies have successfully completed, task is queued for instantiation.
   * - R
     - RUNNING
     - Task is running on the specified distributor.

TaskInstance Statuses
*********************
.. list-table::
   :widths: 10 35 50
   :header-rows: 1

   * - ID
     - Label
     - Description
   * - B
     - SUBMITTED_TO_BATCH_DISTRIBUTOR
     - TaskInstance registered in the Jobmon database.
   * - D
     - DONE
     - TaskInstance finished successfully.
   * - E
     - ERROR
     - TaskInstance stopped with an application error (non-zero return code).
   * - F
     - ERROR_FATAL
     - TaskInstance killed itself as part of a cold workflow resume, and cannot be retried.
   * - I
     - INSTANTIATED
     - TaskInstance is created within Jobmon, but not queued for submission to the cluster.
   * - K
     - KILL_SELF
     - TaskInstance has been ordered to kill itself if it is still alive, as part of a cold workflow resume.
   * - O
     - LAUNCHED
     - TaskInstance submitted to the cluster normally, part of a Job Array.
   * - Q
     - QUEUED
     - TaskInstance is queued for submission to the cluster.
   * - R
     - RUNNING
     - TaskInstance has started running normally.
   * - T
     - TRIAGING
     - TaskInstance has errored, Jobmon is determining the category of error.
   * - U
     - UNKNOWN_ERROR
     - TaskInstance stopped reporting that it was alive for an unknown reason.
   * - W
     - NO_DISTRIBUTOR_ID
     - TaskInstance submission within Jobmon failed – did not receive a distributor_id from the cluster.
   * - X
     - NO_HEARTBEAT
     - TaskInstance stopped sending heartbeat signals and is presumed dead.
   * - Z
     - RESOURCE_ERROR
     - TaskInstance died because of insufficient resource request, e.g. insufficient memory or runtime.

