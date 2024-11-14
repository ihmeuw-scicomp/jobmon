
Task Generator
#######################
The Task Generator is a new feature in Jobmon, currently available only in Python,
that enables users to generate tasks from an existing method. This feature utilizes
serialization to convert method parameters into strings, forming a CLI command for
submission to the cluster. Once on the cluster node,
Jobmon deserializes the strings back into their original parameters,
allowing the tasks to execute as intended.

*Import*

The task generator can be imported from jobmon.core:
    from jobmon.core import task_generator
or
    from jobmon.core.task_generator import task_generator

*Parameters*

.. table::
    :widths: auto
    :align: left

    ===================  =================== ==========================================================================================
    Parameter             Type                Description
    ===================  =================== ==========================================================================================
    serializers          Dict                 A dict of {type: callable} to serialize the input parameters.
    naming_args          Optional[List[str]]  The args used to name the task. Does not apply to array tasks.
    max_attempts         Optional[int]        The maximum number of attempts to run the task.
    module_source_path   Optional[str]        The path to the module source code if the module is not in the current conda environment.
    ===================  =================== ==========================================================================================

*Examples*

**Method with simple input parameter type:**

.. code-block:: python

    @task_generator(
        serializers={},
        tool_name="test_tool",
        module_source_path=full_script_path,
        max_attempts=1,
        naming_args=["foo"],
    )
    def simple_function(foo: int, bar: List[str] = []) -> None:
        """Simple task_function."""
        print(f"foo: {foo}")
        print(f"bar: {bar}")

The above code allows the user to use the "simple_function" method as a task generator to generate
a single task or a task array.

To create a single task workflow:

.. code-block:: python

    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}
    task = simple_function.create_task(wf, foo=1, bar=["a", "b"], compute_resources=compute_resources)
    wf.add_task(task)
    wf.run()

The above code creates a workflow with a single task named "simple_function:foo=1" and runs it.

To create a task array workflow:

.. code-block:: python

    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}
    tasks = simple_function.create_tasks(compute_resources=compute_resources, foo=[1, 2], bar=[["a", "b"]])
    wf.add_task_array(task_array)
    wf.run()

The above code creates a workflow with a task array of two tasks with input (foo=1, bar=["a", "b"] and
(foo=2, bar=["a", "b"]) and runs it.

**Method with complex input parameter type:**

.. code-block:: python

    class TestYear:
        """A fake YearRange class for testing"""
        def __init__(self, year: int) -> None:
            self.year = year
        @staticmethod
        def parse_year(year: str):
            """Parse a year range."""
            return TestYear(int(year))
        def __str__(self) -> str:
            return str(self.year)
        def __eq__(self, other):
            return self.year == other.year
    test_year_serializer = {TestYear: (str, TestYear.parse_year)}
    @task_generator.task_generator(
        serializers=test_year_serializer,
        tool_name="test_tool",
        module_source_path=full_script_path,
        max_attempts=1,
        naming_args=["year"],
    )
    def simple_function_with_serializer(year: TestYear) -> None:
        """Simple task_function."""
        print(f"year: {year}")

The above code creates a testing class, TestYear, with a serializer to convert s string to TestYear,
and a task generator, simple_function_with_serializer.

To create a single task workflow:

.. code-block:: python

    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}
    task = simple_function_with_serializer.create_task(wf,
               year=TestYear(2021),
               compute_resources=compute_resources)
    wf.add_task(task)
    wf.run()

The above code creates a workflow with a single task named
"simple_function_with_serializer:year=2021" and runs it.

To create a workflow with function input containing special characters like a single quote:

.. code-block:: python

    import html

    def special_char_encodeing(input: str) -> str:
    """Encode special characters."""
    return html.escape(input)


    def special_char_decoding(input: str) -> str:
        """Decode special characters."""
        return html.unescape(input)


    @task_generator.task_generator(
        serializers={str: (special_char_encodeing, special_char_decoding)},
        tool_name="test_tool",
        module_source_path=full_script_path,
        max_attempts=1,
        naming_args=["foo"],
    )
    def special_chars_function(foo: str) -> None:
        """Simple task_function."""
        print(f"foo: {foo}")


    tool = Tool("test_tool")
    tool.set_default_compute_resources_from_dict(
        cluster_name="slurm", compute_resources={"queue": "all.q"}
    )
    wf = tool.create_workflow()
    compute_resources = {"queue": "all.q", "project": "proj_scicomp"}
    simple_function = task_generator_funcs.special_chars_function
    task = simple_function.create_task(compute_resources=compute_resources, foo=f"\'aaa\'")
    wf.add_task(task)
    wf.run()

The above code creates a workflow with a single task what requests special characters in the input.
Please note that this makes the Jobmon command harder to read and understand; thus, SciComp is not
responsible to debug it.

You can pass your own function to name your task. The function should take two arguments:
    prefix: str - this will be your function name
    kwargs_for_name: Dict[str, Any] - the arguments of the task
    and return a string.

.. code-block:: python

        def custom_naming(prefix: str, kwargs_for_name: Dict[str, Any]) -> str:
            return f"Lala_{kwargs_for_name['foo']}"

        @task_generator(
            serializers={},
            tool_name="test_tool",
            module_source_path=full_script_path,
            max_attempts=1,
            naming_args=["foo"],
            custom_naming=custom_naming,
        )
        def simple_function(foo: int, bar: List[str] = []) -> None:
            """Simple task_function."""
            print(f"foo: {foo}")
            print(f"bar: {bar}")

The above code creates a task generator with a custom naming function. The task will be
named "Lala_1" instead of "simple_function:foo=1".

.. code-block:: python

    @task_generator(
        default_cluster_name="slurm",
        default_compute_resources={"queue": "all.q", "project": "proj_scicomp"},
        serializers={},
        tool_name="test_tool",
        module_source_path=full_script_path,
        max_attempts=1,
        naming_args=["foo"],
    )
    def simple_function(foo: int, bar: List[str] = []) -> None:
        """Simple task_function."""
        print(f"foo: {foo}")
        print(f"bar: {bar}")

The above code creates a task generator with default cluster name and compute resources, so you do not need to
specify them when creating a task.

.. code-block:: python

    @task_generator(
        yaml_file="/tmp/task_generator.yaml",
        serializers={},
        tool_name="test_tool",
        module_source_path=full_script_path,
        max_attempts=1,
        naming_args=["foo"],
    )
    def simple_function(foo: int, bar: List[str] = []) -> None:
        """Simple task_function."""
        print(f"foo: {foo}")
        print(f"bar: {bar}")

The above code creates a task generator with a yaml file that contains the default cluster name and compute resources,