*******************************
Jobmon's Finite State Machines
*******************************

The Finite State Machine (FSM) of a Jobmon domain object is the set of states an object can
inhabit on each deployment unit. It also shows the allowed transitions between states. The
goal of this document is to propose a simplified system of mapping FSM states onto agent roles
and the deployment units that execute them.

A workflow for one particular domain object (e.g. Task) is often spread across multiple
deployment units. As the object transitions through the FSM its "home" moves from one
deployment unit to another.

Agent Roles
###########

Each deployment unit can act in one of four roles during the progression of a domain object
through its finite state machine.

- DEFINER: the agent that decides **which** compute instructions will execute.
- CONTROLLER: the agent that decides **when** the defined compute instructions will execute.
- DISTRIBUTOR: the agent that decides **where** the defined compute instructions will execute.
- OPERATOR: the agent that **executes** the defined compute instructions.

Unification of Task and Workflow FSMs
#####################################

The finite state machines for Task and Workflow are linked and need to be understood as a pair.
The transitions in each are driven by the progression of associated instance finite state
machines. Each deployment unit has a sub-FSM for each type of domain object that it progresses
through before entering a terminal state for that deployment unit. The terminal state is 1. a
signal for the next deployment unit to begin working, or 2. that an unrecoverable error
has been encountered. The deployment unit flow is below; the signal state is listed on the
arrow.

In this diagram the round-cornered rectangles are deployment units, or rather the FSM for
this domain object in that particular deployment unit.

.. image:: diagrams/deployment_unit_fsm.svg

Universal Finite State Machine
##############################

The deployment unit FSM can be generalized into a universal FSM for progression of tasks and
workflows through the deployment units by adding a processing state for each deployment unit.
When a deployment unit has control over the progression of the FSM it is specified by
an **-ing** suffix unique to that deployment unit (REGISTERING, INSTANTIATING, RUNNING). When
the deployment unit wants to pass off control it signals using an **-ed** word to signal to
the next deployment unit that it can claim control. When an error is encountered in the
scheduler or executor the web server decides which deployment unit will get control next.
Alternatively, if all retries are used then the error is fatal and the FSM terminates. If
the client encounters an error it is automatically considered fatal because the object will
have insufficient compute instructions to continue execution. This failure mode is designated
via the special aborted error. It is appropriate to think of each deployment unit as a while
loop waiting for a signal state to begin work. The work is executed in a try/except block that
either errors and posts an error signal or finishes and posts a proceed signal. The states
below describe the universal finite state machine.

- REGISTERING (G) = Client is adding the requisite metadata to the database, and receives an ID back.
- QUEUED (Q) = Client has added all necessary metadata; signals to scheduler to instantiate.
- ABORTED (A) = Client failed to do all necessary work; scheduler cannot begin instantiation. This is fatal.
- INSTANTIATING (I) = Scheduler is instantiating an instance on the executor.
- LAUNCHED (L) = Instantiation is complete. Executor in control for Tasks. Waiting for first scheduling loop for Workflows.
- RUNNING (R) = Actively executing.
- DONE (D) = All work has finished successfully.
- TRIAGING (T) = The cluster Job finished with some kind of error. Figure out which agent gets control and which state the object should move to.
- FAILED (F) = Encountered a fatal error or have reached the maximum number of attempts.
- HALTED (H) = Execution was stopped mid-run.

.. image:: diagrams/shared_fsm.svg

Detailed Task FSM
*****************

Each **-ing** state on a stateful deployment unit has a sub-machine. Filling in the
sub-machine for TaskInstance gives the figure below.

.. image:: diagrams/task_instance_fsm.svg

Of note are the myriad of error states that can occur on the scheduler and worker node. Each
results in a Triaging state in the universal FSM. Each unique state is driven by a different
agent: client, scheduler, worker node.

The enumerated roles for each deployment unit in the Task FSM shows potential design issues
with the client.

- CLIENT -> DEFINER + CONTROLLER
- SCHEDULER -> DISTRIBUTOR
- WORKER NODE -> OPERATOR

In the current implementation of the task instance FSM the client acts as a definer and a
controller since the swarm is inside the client whereas the ``_adjust_resources_and_queue()`` method
is in the swarm. A better solution would be for the swarm to run independently on a worker
node as if it were a task. This is preferable because it would increase resiliency since the
workflow can be retried from the database. It would also allow the client to disconnect after
is fully defines a workflow. If the client api were more robust, and included task defaults,
we could even have workflows be started via a CLI.


Resource Retries
================

Jobs may die due to cluster enforcement if they have under-requested resources.
In order to help jobs complete without user intervention every time,
Jobmon now has resource adjustment. If Jobmon detects that a job has died due to
resource enforcing, the resources are increased and the job will be retried
if it has not exceeded the maximum attempts.

A record of the resources requested can be found in the executor parameter set
table.
Each job has the original parameters requested and the
validated resources.
In addition it has a row added each time that a resource error occurs
and the resources are be increased. If resource retires do occur, the user should
reconfigure their job to use the resources that ultimately succeeded so that
they do not waste cluster resources in the future.

Jobmon deals with a Task Instance failing due
to resource enforcement as follows:

1. The cluster operating system kills the task and returns a resource-killed error code
#. The Jobmon wrapper around the Cluster Task identifies the error and contacts the server with a state update to Z
#. The jobmon-server updates the state in the database
#. The reconciler inside the Python client wakes up and pings the server for a list of Task Instances
   that have changed state since the last time the Reconciler ran.
#. The server finds the Task Instance with recent change to state Z. and returns it (along with other
   Task Instance state changes).
#. The reconciler in the Python client moves the Task Instance
   into state A (Adjusting Resources) if the Task Instance has not reached its retry limit.
#. **NEED HELP HERE** The Task Instance FSM retrieve Tasks queued for instantiation and
   jobs marked for Adjusting Resources. It adds a new row with adjusted
   resources to the executor parameters set table for that job, and mark
   those as the active resources for that job to use, then it will queue it
   for instantiation using those resources
#. A new Task instance is created, referring to the new
   adjusted resource values

The query to retrieve all resource entries for all jobs in a dag is::

    SELECT EPS.*
    FROM executor_parameter_set EPS
    JOIN job J on(J.job_id=EPS.job_id)
    WHERE J.dag_id=42;


Detailed Workflow FSM
*********************

Filling in the sub-machine for Workflow Run give the figure below.

.. image:: diagrams/workflow_run_fsm.svg

The key difference between the Workflow Run FSM and the Task Instance FSM is that the Workflow
Run FSM mandates that the worker node signals back that the process has successfully halted
before a new instance can be created. **Future Question:** Should this pattern also be
adopted in the Task Instance FSM as well?

The enumerated roles for each deployment unit in the Workflow FSM show an opportunity for
improvement.

- CLIENT -> DEFINER + CONTROLLER + DISTRIBUTER + OPERATOR
- SCHEDULER -> N/A
- WORKER NODE -> N/A

A better solution would be to have the workflow run be run on a Worker Node. The new roles
would be the following

- CLIENT -> DEFINER + CONTROLLER
- SCHEDULER -> DISTRIBUTER
- WORKER NODE -> OPERATOR

In a future world would could have the workflow reaper be the controller as well, so the
client only defines the computation.

