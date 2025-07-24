**************
Advanced Usage
**************

Arrays
######
Jobs are launched on the Slurm cluster are launched as Job Arrays (or Array Jobs on UGE).
The effect is that Jobmon uses one sbatch command to launch all the jobs in one TaskTemplate,
rather than one sbatch command to launch a single job. This allows Jobmon to launch jobs
faster. In a comparison load test, 3.0.5 took 1045.10 seconds (17.418 minutes) to submit
10,000 tasks to the cluster, while 3.1.0 took 32.10 seconds to submit the same 10,000 tasks
to IHME's cluster.

Usage
*****
.. tabs::

    .. group-tab:: python
        .. literalinclude:: ./create_tasks_example.py
               :language: python

    .. group-tab:: R
        .. literalinclude:: ./create_tasks_example.R
               :language: R


Array Inference
***************
As mentioned above, Tasks are launched using Slurm Job Arrays (including tasks that were created using
create_task() instead of create_array()). Tasks that share the same task_template and
compute_resources are grouped into arrays during workflow.run().
To prevent overloading the Slurm cluster there is a maximum size for each array.
Therefore an enormous TaskTemplate might launch as several Job Arrays.
Jobmon only adds Tasks to a JobArray when that Task is ready to run, i.e. that its upstreams
have all successfully completed.
This means that workflow with multiple phases then the task in each phase should
belong to different task_templates.
If a TaskInstance fails and the task needs to ve relaunched, then Jobmon adds that TaskInstance to
a new Slurm Job Array.

Jobmon waits a short amount of time for more requests to arrive then submits
the Slurm Job Array.


Slurm Job Arrays
****************
For more info about job arrays on a Slurm cluster, see here: https://slurm.schedmd.com/job_array.html

Retries
#######

Ordinary Retry
**************
By default a Task will be retried up to three times if it fails. This helps to
reduce the chance that random events on the cluster or landing on a bad node
will cause your entire Task and Workflow to fail. If a TaskInstance fails, then Jobmon will
run an exact copy as long as the max number of attempts hasn't be reached. The new TaskInstance
will be created with the same resources and configurations as the first TaskInstance.

In order to configure the number of times a Task can be retried, configure the
max_attempts parameter in the Task that you create. If you are still debugging
your code, set the number of retries to zero so that it does not retry
code with a bug multiple times. When the code is debugged, and you are ready
to run in production, set the retries to a non-zero value.

The following example shows a configuration in which the user wants their Task
to be retried four times and it will fail up until the fourth time.::

    import getpass
    from jobmon.client.tool import Tool

    user = getpass.getuser()

    tool = Tool(name=ordinary_resume_tutorial)

    wf = tool.create_workflow(name=ordinary_resume_wf, workflow_args="wf_with_many_retries")

    retry_tt = tool.get_task_template(
        template_name="retry_tutorial_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )
    output_file_name = f"/home/{user}/retry_output"
    this_file = os.path.dirname(__file__)
    remote_sleep_and_write = os.path.abspath(
        os.path.expanduser(f"{this_file}/../tests/_scripts/remote_sleep_and_write.py")
    )
    retry_task = retry_tt.create_task(
        name="retry_task",
        max_attempts=4,
        compute_resources={
            'cores': 1,
            'runtime': '100s',
            'memory': '1Gb',
            'queue': 'all.q',
            'project': 'proj_scicomp',
        },
        cluster_name="slurm",
        arg=f"python {remote_sleep_and_write} --sleep_secs 4 --output_file_path {output_file_name} --name "retry_task" --fail-count 3"

    )

    wf.add_task(retry_task)

    # 3 TaskInstances will fail before ultimately succeeding
    workflow_run_status = wf.run()



Resource Retry
**************
Sometimes you may not be able to accurately predict the runtime or memory usage
of a task. Jobmon will detect when the task fails due to resource constraints and
then retry that task with with more resources. The default resource
scaling factor is 50% for memory and runtime.
For example if your
runtime for a task was set to 100 seconds and fails, Jobmon will automatically
retry the Task with a max runtime set to 150 seconds. You can specify the percentage
scaling factor.
The scaling factor is applied each time, cumulatively.
For example, if Jobmon is configured to increase memory 50% then when jobmon retries due to
insufficient memory it increase by 50% over the last requested memory request.
If 40GiB is the original request then the memory increases as 40 -> 60 -> 90.

For example::

    import getpass
    from jobmon.client.tool import Tool

    # The Task will time out and get killed by the cluster. After a few minutes Jobmon
    # will notice that it has disappeared and ask Slurm for an exit status. Slurm will
    # show a resource kill. Jobmon will scale the memory and runtime by the default 50% and
    # retry the job at which point it will succeed.

    user = getpass.getuser()

    tool = Tool(name=resource_resume_tutorial)

    wf = tool.create_workflow(name=resource_resume_wf, workflow_args="wf_with_resource_retries")

    retry_tt = tool.get_task_template(
        template_name="resource_retry_tutorial_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )

    retry_task = retry_tt.create_task(
                        arg="sleep 110"
                        name="retry_task",
                        # job should succeed on second try. The runtime will 135 seconds on the retry
                        max_attempts=2,
                        compute_resources={
                            'cores': 1,
                            'runtime': '90s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        cluster_name="slurm"
                    )

    wf.add_task(retry_task)

    my_wf.run()

Custom Resource Scales
**********************

The most basic version of resource scaling is cumulative multiplication by a scaling factor,
but you can also use some more bespoke resource scalers. You can pass a Callable that will be
applied to the existing resource value, or an Iterator that yields numeric values. Any
Callable should take a single numeric value as its sole argument and return only a single
numeric value. Any Iterable can be easily converted to an Iterator by using the iter()
built-in (e.g. iter([80, 160, 190])).

For example::

    import getpass
    from jobmon.client.tool import Tool

    # The Task will time out and get killed by the cluster. After a few minutes Jobmon
    # will notice that it has disappeared and ask Slurm for an exit status. Slurm will
    # show a resource kill. Jobmon will use the runtime values passed in the
    # resource_scales dictionary, and on the third attempt will run for 120s.

    user = getpass.getuser()

    tool = Tool(name=resource_resume_tutorial)

    wf = tool.create_workflow(name=resource_resume_wf, workflow_args="wf_with_resource_retries")

    retry_tt = tool.get_task_template(
        template_name="resource_retry_tutorial_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[]
    )

    retry_task = retry_tt.create_task(
                        arg="sleep 110"
                        name="retry_task",
                        # job should succeed on third try. The runtime will be 120 seconds on the retry
                        max_attempts=3,
                        compute_resources={
                            'cores': 1,
                            'runtime': '90s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        resource_scales={"runtime": iter([100, 120])},
                        cluster_name="slurm"
                    )

    wf.add_task(retry_task)

    my_wf.run()

.. _jobmon-resume-label:

Resuming a Workflow
###################

A Workflow tacks how many times a DAG was run, who ran them, and when.
With a Workflow you can:

#. Stop a set of Tasks mid-run and resume it (either intentionally because you need to
   fix a bug, a result of an unfortunate cluster event)
#. Re-attempt a set of Tasks that may have ERROR'd out in the middle (assuming you
   identified and fixed the source of the error)

When a workflow is resumed, Jobmon examines the Workflow from the beginning and skips over
any tasks that are already Done. It will restart jobs that were in Error (maybe you fixed
that bug!) or are Registered. As always it only starts a job when all its upstreams are Done.
In other words, it starts from first failure. Jobmon creates a new workflow run for an existing workflow.

**Note**: There is a distinction between "restart" and "resume."
Jobmon itself will *restart* individual *Tasks,* whereas a human operator can *resume* the
entire *Workflow.*

There are two ways to resume a Workflow – by ID or by recreating the workflow with the same workflow args.

Resume by ID
************

To resume by ID, you can either use the CLI function:

``jobmon workflow_resume -w <workflow_id> -c <cluster_name>``

The "workflow_resume" CLI has two optional flags:

* ``--reset-running-jobs`` - Whether to kill currently running jobs or let them finish. Setting this flag means that it
  will be a cold resume i.e. currently running jobs are also terminated and rerun. Default is set to False.
* ``--timeout`` or ``-t`` - Allows users to set the Workflow timeout. Default is 180.


Important caveat: if resuming a workflow by ID, you will not have the ability to change certain parameters that have been
bound to the database, such as the workflow attributes or the compute resources. To run a workflow with updated resources,
use the other resume path (recreate the workflow with the same workflow args). 

Resume By Recreating the Workflow
*********************************

To resume a Workflow programmatically, make sure that your previous workflow
run process is dead (kill it using the Slurm scancel command).

When creating a resumed workflow, the
workflow_args provided to Tool.create_workflow() match the workflow they are attempting to resume. Additionally,
users need to add a resume parameter to the run() function to resume their Workflow.::

    workflow = Tool.create_workflow(workflow_args='previous_workflow_args')
    workflow.run(resume=True)

That's it. If you don't set "resume=True", Jobmon will raise an error saying that the user is
trying to create a Workflow that already exists.

For more examples, take a look at the `resume tests <https://github.com/ihmeuw-scicomp/jobmon/blob/release/3.2/tests/pytest/end_to_end/test_workflow_resume.py>`_.

.. note::

    Remember, a Workflow is defined by its WorkflowArgs and its Tasks. If you
    want to resume a previously stopped run, make sure you haven't changed the
    values of WorkflowArgs or added/removed any Tasks to it. If either of these change,
    you will end up creating a brand new Workflow.

.. note::

    Resuming a previously stopped Workflow will create a new
    :term:`WorkflowRun`. This is generally an internal detail that you won't
    need to worry about, but the concept may be helpful in debugging failures.
    (SEE DEBUGGING TODO).

As soon as you change any of the values of your WorkflowArgs or modify its Tasks,
you'll cause a new Workflow entry to be created in the Jobmon
database. When calling run() on this new Workflow, any progress through the
Tasks that may have been made in previous Workflows will be ignored.

For further configuration there are two types of resumes:

Cold Resume
***********
All Tasks are stopped and you are ok with resetting all running Tasks and killing any running
TaskInstances before restarting (the default option).

Hot Resume
**********
Any Tasks that are currently running will not be reset, and
any TaskInstance that are currently running on the cluster will not be killed

Fail Fast
#########

In “normal” mode, Jobmon will execute as many of the jobs as it can in the workflow.
As jobs succeed, their downstreams are launched. If a job fails, then its downstreams
are not launched, but other paths through the graph continue.
That mode will do as much work as possible before Jobmon and exits with an error.
This is the correct mode if your code is well debugged.
You can fix what is probably a data error and resume from where it stopped.

In “fail-fast” mode, Jobmon will stop launching jobs as soon as one job fails
(but it won’t kill jobs that are currently running).
This mode is suitable if your code is not well-debugged.
A failure probably means you have a bug and therefore need to fix it,
and start again from the beginning.
A Workflow will **not** fail fast
if a Task fails because of a resource error (e.g. over runtime or over memory).

For example::

    workflow = tool.create_workflow(name="test_fail_fast", workflow_args="testing")
    task = task_template.create_task(name="fail_fast_task",
                                     compute_resources={runtime: "100s"},
                                     arg="sleep 1")
    workflow.add_tasks([task])

    # This line makes the workflow fail fast
    wfr_status = workflow.run(fail_fast=True)


Fallback Queues
###############

A "Fallback Queue" is a second queue that Jobmon will use if a Task is rescaled by
resource retries such that it no longer fits on its original queue.
Suppose that you have a Task that fails due
to a resource error. Jobmon then scales that Tasks resources, but the newly scaled resources
exceed the resources of the queue the Task is on. If you had specified
a fallback queue then Jobmon would run the Task with scaled
resources onto the next specified queue. If you do not specify a fallback queue, the
resources will only scale to the maximum values of their originally specified queue.

To set fallback queues, simply pass a list of queues to the  create_task() method. For example::

    # In this example Jobmon will run the Task on all.q. Hypothetically, if it scaled the resources
    # past the all.q limits, it would then try to run the Task on long.q. If that also failed,
    # it would then try to run the Task on d.q.

    workflow = tool.create_workflow(name="test_fallback_queue", workflow_args="fallback")
    fallback_task = fallback_tt.create_task(
                        arg="sleep 110"
                        name="fallback_task",
                        compute_resources={
                            'cores': 1,
                            'runtime': '90s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        cluster_name="slurm",
                        fallback_queues=["long.q", "d.q"]
                    )
    workflow.add_tasks([task])

    # This line makes the workflow fail fast
    wfr_status = workflow.run(fail_fast=True)

Dynamic Task Resources
######################

You can dynamically configure the resources needed to run a
given task. For example, if an upstream Task can better inform the resources
that a downstream Task needs, the resources will not be checked and bound until
the downstream is about to run and all of it's upstream dependencies
have completed. To do this, you must provide a function that will be called
at runtime and return a ComputeResources object with the resources needed.

For example ::

    import sys
    from jobmon.client.tool import Tool

    def assign_resources(*args, **kwargs):
        """ Callable to be evaluated when the task is ready to be scheduled
        to run"""
        fp = f'/home/{user}/jobmon/resources.txt'
        with open(fp, "r") as file:
            resources = file.read()
            resource_dict = ast.literal_eval(resources)
        memory = resource_dict['memory']
        runtime = int(resource_dict['runtime'])
        cores = int(resource_dict['cores'])
        queue = resource_dict['queue']

        compute_resources = {"memory": memory, "runtime": runtime, "cores": cores,
                            "queue": queue}
        return compute_resources

    tool = Tool(name="dynamic_tool")

    dynamic_tt = tool.get_task_template(
                template_name="random_template",
                command_template="{python} {script}",
                node_args=[],
                task_args=[],
                op_args=["python", "script"],
                default_cluster_name='slurm')

    # task with static resources that assigns the resources for the 2nd task
    # when it runs
    workflow = tool.create_workflow(name="dynamic_tasks", workflow_args="dynamic")
    task1 = dynamic_tt.create_task(
                        name="task_to_assign_resources",
                        python=sys.executable,
                        script="/assign_resources.py"
                        compute_resources={
                            'cores': 1,
                            'runtime': '200s',
                            'memory': '1Gb',
                            'queue': 'all.q',
                            'project': 'proj_scicomp'},
                        max_attempts=1
                        cluster_name="slurm"
                    )
    # tt is a simple task template that makes arg the command
    task2 = tt.create_task(
                name="dynamic_resource_task",
                arg="echo hello",
                max_attempts=2,
                compute_resouces=assign_resources
            )
    task2.add_upstream(task1) # make task2 dependent on task 1

    wf.add_task(task1, task2)
    wfr_status = wf.run()

Advanced Task Dependencies
##########################
For this example, we'll use a slightly simplified version of the Burdenator which has five
"phases": most-detailed, pct-change, loc-agg, cleanup, and upload. To reduce runtime,
we want to link up each job only to the previous jobs that it requires, not to every job
in that phase. The parallelization strategies for each phase are a little different,
complicating the dependency scheme.

1. Most-detailed jobs are parallelized by location, year;
2. Loc-agg jobs are parallelized by measure, year, rei, and sex;
3. Cleanup jobs are parallelized by location, measure, year
4. Pct-change jobs are parallelized by location_id, measure, start_year, end_year; For most-detailed locations, this can run immediately after the most-detailed phase. But for aggregate locations, this has to be run after both loc-agg and cleanup
5. Upload jobs are parallelized by measure

To begin, we create an empty dictionary for each phase and when we build each task, we add the
task to its dictionary. Then the task in the following phase can find its upstream task using
the upstream dictionary. The only dictionary not needed is one for the upload jobs, since no
downstream tasks depend on these jobs.

.. code::

    # python 3
    import sys
    from jobmon.client.tool import Tool
    from jobmon.client.task_template import TaskTemplate

    from my_app.utils import split_locs_by_loc_set

    class NatorJobSwarm(object):
        def __init__(self, year_ids, start_years, end_years, location_set_id,
                     measure_ids, rei_ids, sex_ids, version):
            self.year_ids = year_ids
            self.start_year_ids = start_years
            self.end_year_ids = end_years
            self.most_detailed_location_ids, self.aggregate_location_ids, \
                self.all_location_ids = split_locs_by_loc_set(location_set_id)
            self.measure_ids = measure_ids
            self.rei_ids = rei_ids
            self.sex_ids = sex_ids
            self.version = version

            self.tool = Tool(name="Burdenator")
            self.most_detailed_jobs_by_command = {}
            self.pct_change_jobs_by_command = {}
            self.loc_agg_jobs_by_command = {}
            self.cleanup_jobs_by_command = {}

            self.python = sys.executable

        def create_workflow(self):
            """ Instantiate the workflow """

            self.workflow = self.tool.create_workflow(
                workflow_args = f'burdenator_v{self.version}',
                name = f'burdenator run {self.version}'
            )

        def create_task_templates(self):
            """ Create the task template metadata objects """

            self.most_detailed_tt = self.tool.get_task_template(
                template_name = "run_burdenator_most_detailed",
                command_template = "{python} {script} --location_id {location_id} --year {year}",
                node_args = ["location_id", "year"],
                op_args = ["python", "script"])

            self.loc_agg_tt = self.tool.get_task_template(
                template_name = "location_aggregation",
                command_template = "{python} {script} --measure {measure} --year {year} --sex {sex} --rei {rei}",
                node_args = ["measure", "year", "sex", "rei"],
                op_args = ["python", "script"])

            self.cleanup_jobs_tt = self.tool.get_task_template(
                template_name = "cleanup_jobs",
                command_template = "{python} {script} --measure {measure} --loc {loc} --year {year}",
                node_args = ["measure", "loc", "year"],
                op_args = ["python", "script"])

            self.pct_change_tt = self.tool.get_task_template(
                template_name = "pct_change",
                command_template = ("{python} {script} --measure {measure} --loc {loc} --start_year {start_year}"
                                    " --end_year {end_year}"),
                node_args = ["measure", "loc", "start_year", "end_year"],
                op_args = ["python", "script"])

            self.upload_tt = self.tool.get_task_template(
                template_name = "upload_jobs",
                command_template = "{python} {script} --measure {measure}"
                node_args = ["measure"],
                op_args = ["python", "script"])


        def create_most_detailed_jobs(self):
            """First set of tasks, thus no upstream tasks"""

            for loc in self.most_detailed_location_ids:
                for year in self.year_ids:
                    task = self.most_detailed_tt.create_task(
                                      compute_resources={"cores": 40, "memory": "20Gb", "runtime": "360s"},
                                      cluster_name="slurm",
                                      max_attempts=5,
                                      name='most_detailed_{}_{}'.format(loc, year),
                                      python=self.python,
                                      script='run_burdenator_most_detailed',
                                      loc=loc,
                                      year=year)
                    self.workflow.add_task(task)
                    self.most_detailed_jobs_by_command[task.name] = task

        def create_loc_agg_jobs(self):
            """Depends on most detailed jobs"""

            for year in self.year_ids:
                for sex in self.sex_ids:
                    for measure in self.measure_ids:
                        for rei in self.rei_ids:
                            task = self.loc_agg_tt.create_task(
                                compute_resources={"cores": 20, "memory": "40Gb", "runtime": "540s"},
                                cluster_name="slurm,
                                max_attempts=11,
                                name='loc_agg_{}_{}_{}_{}'.format(measure, year, sex, rei),
                                python=self.python,
                                script='run_loc_agg',
                                measure=measure,
                                year=year,
                                sex=sex,
                                rei=rei)

                            for loc in self.most_detailed_location_ids:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                            self.workflow.add_task(task)
                            self.loc_agg_jobs_by_command[task.name] = task

        def create_cleanup_jobs(self):
            """Depends on aggregate locations coming out of loc agg jobs"""

            for measure in self.measure_ids:
                for loc in self.aggregate_location_ids:
                    for year in self.year_ids:
                        task = self.cleanup_jobs_tt.create_task(
                                          compute_resources={"cores": 25, "memory": "50Gb", "runtime": "360s"},
                                          cluster_name="slurm",
                                          max_attempts=11,
                                          name='cleanup_{}_{}_{}'.format(measure, loc, year),
                                          python=self.python,
                                          script='run_cleanup',
                                          measure=measure,
                                          loc=loc,
                                          year=year)

                        for sex in self.sex_ids:
                            for rei in self.rei_ids:
                                task.add_upstream(
                                    self.loc_agg_jobs_by_command['loc_agg_{}_{}_{}_{}'
                                                                 .format(measure, year,
                                                                         sex, rei)])
                        self.workflow.add_task
                        self.cleanup_jobs_by_command[task.name] = task

        def create_pct_change_jobs(self):
            """For aggregate locations, depends on cleanup jobs.
            But for most_detailed locations, depends only on most_detailed jobs"""

            for measure in self.measure_ids:
                for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                    for loc in self.location_ids:
                        if loc in self.aggregate_location_ids:
                            is_aggregate = True
                        else:
                            is_aggregate = False
                        task = self.pct_change_tt.create_task(
                                          compute_resources={"cores": 45, "memory": "90Gb", "runtime": "540s"},
                                          cluster_name="slurm",
                                          max_attempts=11,
                                          name=('pct_change_{}_{}_{}_{}'
                                                .format(measure, loc, start_year, end_year),
                                          python=self.python,
                                          script='run_pct_change',
                                          measure=measure,
                                          loc=loc,
                                          start_year=start_year,
                                          end_year=end_year)

                        for year in [start_year, end_year]:
                            if is_aggregate:
                                task.add_upstream(
                                    self.cleanup_jobs_by_command['cleanup_{}_{}_{}'
                                                                 .format(measure, loc, year)]
                            else:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                        self.workflow.add_task(task)
                        self.pct_change_jobs_by_command[task.name] = task

        def create_upload_jobs(self):
            """Depends on pct-change jobs"""

            for measure in self.measure_ids:
                task = self.upload_tt.create_task(
                                  compute_resources={"cores": 20, "memory": "40Gb", "runtime": "720s"},
                                  cluster_name="slurm",
                                  max_attempts=3,
                                  name='upload_{}'.format(measure)
                                  script='run_pct_change',
                                  measure=measure)

                for location_id in self.all_location_ids:
                    for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                        task.add_upstream(
                            self.pct_change_jobs_by_command['pct_change_{}_{}_{}_{}'
                                                            .format(measure, location,
                                                                    start_year, end_year])
                self.workflow.add_task(task)

        def run():
            success = self.workflow.run()
            if success:
                print("You win at life")
            else:
                print("Failure")


Concurrency Limiting
####################
You can set the maximum number of tasks per workflow that are running at one time.
The value can be set statically (in the Jobmon code), or dynamically via the Jobmon CLI.
One of the main use cases for concurrency limit is if an user needs to "throttle down" a
workflow to make space on the cluster without killing their workflow. By default, Jobmon sets
the limit to 10,000 tasks. If the concurrency limit is reduced while the Workflow is running,
Jobmon will let existing jobs finish but will not launch any more until the number
running falls below the limit.
Jobmon will not kill jobs to reduce the number running to the concurrency limit.

To statically set concurrency limit, simply set the ``max_concurrently_running`` flag on the
``create_workflow()`` method.

.. code-block:: python

  tool = Tool(name="example_tool")
  workflow = tool.create_workflow(
      name=f"template_workflow",
      max_concurrently_running=2000
  )

To dynamically set the concurrency limit, see :ref:`concurrency-limit-label`.

Users are also able to set concurrency limit at the TaskTemplate level. By default, Jobmon sets
this limit to 10,000 tasks.

To set concurrency limit on a TaskTemplate, simply call the ``set_task_template_max_concurrency_limit``
method.

.. code-block:: python

  tool = Tool(name="example_concurrency_tt_tool")

  task_template = tool.get_task_template(
        template_name="concurrency_limit_task_template",
        command_template="{arg}",
        node_args=["arg"],
        task_args=[],
        op_args=[],
  )
  workflow = tool.create_workflow(
      name=f"template_workflow",
  )
  tasks = []
  for i in range(20):
        task = task_template.create_task(arg=f"sleep {i}")
        tasks.append(task)
  workflow.add_tasks(tasks)
  # Setting the concurrency limit it of the "concurrency_limit_task_template" to 2
  workflow.set_task_template_max_concurrency_limit(task_template_name=task_template.template_name,
                                                   limit=2)


Jobmon Self-Service Commands
############################
Jobmon has a suite of commands to not only visualize task statuses from the database, but to
allow the users to modify the states of their workflows. These self-service commands can be
invoked from the command line in the same way as the status commands, see :ref:`status-commands-label`.

.. _concurrency-limit-label:

concurrency_limit
*****************
    Upon initiating a workflow, users have the capability to set an upper limit on the number of tasks that can run
    concurrently. This feature is particularly beneficial in preventing a resource-intensive workflow from overloading
    the cluster.  Users are able to dynamically change this value as their workflow is running via the CLI.

   To modify this value, use the following command:
    ``jobmon concurrency_limit --workflow_id [workflow_id] --max_tasks [maximum number of concurrently running tasks]``

workflow_reset
**************
    Entering ``jobmon workflow_reset`` resets a Workflow to G state (REGISTERED). When a
    Workflow is reset, all of the Tasks associated with the Workflow are also transitioned to
    G state. The usage of this command is ``jobmon workflow_reset -w [workflow_id]``.

    To use this command the last WorkflowRun of the specified Workflow must be in E (ERROR) state.
    The last WorkflowRun must also have been started by the same user that is attempting to reset
    the Workflow.

workflow_resume
*****************

    Jobmon's CLI allows you to resume a workflow you've already started running, but has since failed. The CLI
    entrypoint is ``jobmon workflow_resume``. The following arguments are supported:

        * ``-w``, ``--workflow_id`` - required, the workflow ID to resume.
        * ``-c``, ``--cluster_name`` - required, the cluster name you'd like to resume on.
        * ``--reset-running-jobs`` - default False. Whether to kill currently running jobs or let them finish
        * ``--timeout`` or ``-t`` - Allows users to set the Workflow timeout. Default is 180.

    Example usages:
        * ``jobmon workflow_resume -w 123 -c slurm`` - resume workflow ID 123 on the "slurm" cluster in the database.
        * ``jobmon workflow_resume -w 123 -c dummy --reset-running-jobs`` - resume workflow ID 123 on the dummy cluster. Specify a cold resume so that currently running jobs are also terminated and therefore rerun.

update_task_status
******************
    Entering ``jobmon update_task_status`` sets the status of tasks in a
    workflow. This is helpful for either rerunning portions of a workflow that have already
    completed, or allowing a workflow to progress past a blocking error. The usage is
    ``jobmon update_task_status -t [task_ids] -w [workflow_id] -s [status]``

    There are 2 allowed statuses: "D" - DONE and "G" - REGISTERED.

    Specifying status "D" will mark only the listed task_ids as "D", and leave the rest of the
    DAG unchanged. When the workflow is resumed, the DAG executes as if the listed task_ids
    have finished successfully.

    If status "G" is specified, the listed task IDs will be set to "G" as well as all
    downstream dependents of those tasks. TaskInstances will be set to "K". When the workflow
    is resumed, the specified tasks will be rerun and subsequently their downstream tasks as
    well. If the workflow has successfully completed, and is marked with status "D", the
    workflow status will be amended to status "E" in order to allow a resume.

    .. note::
        1. All status changes are propagated to the database.
        2. Only inactive workflows can have task statuses updated
        3. The updating user must have at least 1 workflow run associated with the requested workflow.
        4. The requested tasks must all belong to the specified workflow ID

TaskTemplate Resource Prediction to YAML
****************************************
    Entering ``jobmon task_template_resources`` generates a task template
    compute resources YAML file that can be used in Jobmon 3.0 and later.

    As an example, ``jobmon task_template_resources -w 1 -p f ~/temp/resource.yaml`` generates
    a YAML file for all task templates used in workflow 1 and saves it to ~/temp/resource.yaml.
    It will also print the generated compute resources to standard out.

    An example output:

    .. code-block:: yaml

       your_task_template_1:
            slurm:
              cores: 1
              memory: "400B"
              runtime: 10
              queue: "all.q"
            buster:
              num_cores: 1
              m_mem_free: "400B"
              max_runtime_seconds: 10
              queue: "all.q"
        your_task_template_2:
            slurm:
              cores: 1
              memory: "600B"
              runtime: 20
              queue: "long.q"
            buster:
              num_cores: 1
              m_mem_free: "600B"
              max_runtime_seconds: 20
              queue: "long.q"

update_config
*************
    The ``jobmon update_config`` command allows users to update configuration values in their 
    local defaults.yaml file using dot notation. This is useful for modifying configuration 
    settings without manually editing YAML files.

    **Usage:**
        ``jobmon update_config <key> <value> [--config-file <path>]``

    **Arguments:**
        * ``key`` - Configuration key in dot notation (e.g., 'http.retries_attempts', 'distributor.poll_interval')
        * ``value`` - New value to set
        * ``--config-file`` - Optional path to specific config file to update (defaults to system config)

    **Examples:**
        * ``jobmon update_config http.retries_attempts 15`` - Update HTTP retry attempts to 15
        * ``jobmon update_config distributor.poll_interval 5`` - Set distributor polling interval to 5 seconds
        * ``jobmon update_config telemetry.tracing.requester_enabled true`` - Enable OTLP tracing for requests
        * ``jobmon update_config db.pool.size 20`` - Update database connection pool size to 20
        * ``jobmon update_config http.service_url "http://new-server.com" --config-file /path/to/config.yaml`` - Update service URL in specific config file

    .. note::
        * Only keys that already exist in the configuration can be updated

Resource Usage
##############
Task Resource Usage
*******************
    The ``task.resource_usage()`` method returns the resource usage for that Task. This
    method must be called after ``workflow.run()``. To use it simply call the method on your
    predefined Task object, ``task.resource_usage()``. This method will return a dictionary
    that includes: the memory usage (in bytes), the name of the node the task was run on, the
    number of attempts, and the runtime. This method will only return resource usage data for
    Tasks that had a successful TaskInstance (in DONE state).

TaskTemplate Resource Usage
***************************
    Jobmon can aggregate the resource usage at the TaskTemplate level. Jobmon will return a
    dictionary that includes: number of Tasks used to calculate the usage, the minimum,
    maximum, and mean memory used (in bytes), and the minimum, maximum and mean runtime. It
    only includes Tasks in the calculation that are associated with a specified
    TaskTemplateVersion.

    You can access this in two ways: via a method on TaskTemplate or the Jobmon command line
    interface.

    To access it via the TaskTemplate object, simply call the method on your predefined
    TaskTemplate, ``task_template.resource_usage()``. This method has two *optional*
    arguments: workflows (a list of workflow IDs) and node_args (a dictionary of node
    arguments). This allows users to have more exact resource usage data. For example, a
    user can call ``resources = task_template.resource_usage(workflows=[123, 456],
    node_args={"location_id":[101, 102], "sex":[1]})`` This command will find all of the
    Tasks associated with that version of the TaskTemplate, that are associated with either
    workflow 123 or 456, that also has a location_id that is either 102 or 102, and has a
    sex ID of 1. Jobmon will then calculate the resource usage values based on those queried
    Tasks.

    To use this functionality via the CLI, call ``jobmon task_template_resources -t
    <task_template_version_id>`` The CLI has two optional flags: -w to specify workflow IDs
    and -a to query by specific node_args. For example, ``jobmon task_template_resources -t
    12 -w 101 102 -a '{"location_id":[101,102], "sex":[1]}'``.

Error Logs
##########
    There is a method on the Workflow object called ``get_errors`` that will return all of the
    task instance error logs associated with a Workflow. To use it simply call the method on
    your predefined Workflow object: ``workflow.get_errors()``. This method will return a
    dictionary; the key will be the ID of the task and the key will be the error message.
    By default this method will return the last 1,000 error messages. Users can specify the
    limit by utilizing the parameter ``limit``. For example if a user wanted to only see the
    errors for the ten most recent tasks they would call ``workflow.get_errors(limit=10)``.

    .. note::
        To see the error log for a specific task users can call the ``task_status`` CLI
        command. For more information see :ref:`task_status-commands-label`.

Python Logging
##############
Jobmon provides a flexible logging configuration system with template-based configurations
and user override capabilities.

**Basic Usage:**

To configure Jobmon's client logging with default settings::

    from jobmon.client.logging import configure_client_logging
    
    configure_client_logging()

This automatically configures all Jobmon client loggers (workflow, task, tool, etc.) with 
console output and INFO level logging.

**Advanced Configuration:**

You can customize logging behavior using configuration overrides in your ``~/.jobmon.yaml`` file:

.. code-block:: yaml

    logging:
      client:
        # Add file logging
        formatters:
          file_formatter:
            format: "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
            datefmt: "%Y-%m-%d %H:%M:%S"
        handlers:
          file:
            class: logging.FileHandler
            filename: "/var/log/jobmon_client.log"
            formatter: file_formatter
            level: INFO
        loggers:
          jobmon.client.workflow:
            handlers: [console, file]
            level: DEBUG

You can also specify a completely custom logging configuration file::

.. code-block:: yaml

    logging:
      client_logconfig_file: "/path/to/custom_client_logging.yaml"

For more details on logging configuration options, see the configuration documentation.


