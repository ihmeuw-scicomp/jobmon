*****************
Glossary of terms
*****************

For users
#########

.. note::
    The glossary provides an overview of key Jobmon terms, for more in-depth explanations see
    :ref:`jobmon-core-label`

.. glossary::

    Tool
        The project (like CODem, DisMod, etc.) to associate your Workflow and Task Templates
        with.

    Workflow
        The object that encompasses all of your Tasks and their dependencies that will be
        executed.

    WorkflowArgs
        A set of arguments that are used to determine the "uniqueness" of the Workflow. They
        decide whether a Workflow can be resumed.

    WorkflowRun
        A single attempt of a Workflow.

    DAG
        Directed Acyclic Graph. The graph of Tasks that will be traversed upon execution of a
        WorkflowRun.

    Node
        The object representing a Task within a DAG.

    Edge
        The relationship between an upstream and a downstream Node.

    TaskTemplate
        The Task Template outlines the structure of a Task to give it more context within the
        DAG and over multiple executions of the DAG..

    Task
        A single executable object in the workflow, a command that will be run.

    TaskAttribute
        Additional attributes of the task that can be tracked.

    TaskInstance
        The actual instance of execution of a Task command.

    Nodes
        Nodes are the object representing a Task within a DAG.

    Distributor
        Where the Tasks will be run. At IHME you will usually run on the Slurm Distributor.
        However, jobs can be run locally using Multiprocessing Distributor or Sequential
        Distributor. If the user wants to set up the Jobmon Workflow and test it without
        risking actually running the commands, they can use the Dummy Distributor which
        imitates job submission.

    Workflow Attributes
        Additional attributes that are being tracked for a given Workflow.
