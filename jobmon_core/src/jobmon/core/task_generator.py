import ast
import importlib
import inspect
import logging
import shutil
import traceback
from types import ModuleType
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    get_args,
    Iterable,
    List,
    Optional,
    Type,
    Union,
)

import docstring_parser
from docutils import nodes, statemachine  # type: ignore
from docutils.parsers.rst import Directive  # type: ignore
from sphinx import application  # type: ignore
from sphinx.util import nodes as sphinx_nodes  # type: ignore

from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.workflow import Workflow

SIMPLE_TYPES = {str, int, float, bool}
BUILT_IN_COLLECTIONS = {list, tuple, set}
OPTIONAL_TYPES = {Optional, type(None)}

TASK_RUNNER_NAME = "worker_node_entry_point"
TASK_RUNNER_SUB_COMMAND = "task_generator"
HELP_TEXT_INTRO_FORMAT = "Command Line Documentation for {full_path}"

SERIALIZED_EMPTY_STRING = '""'

logger = logging.getLogger(__name__)


def create_task_name(
    kwargs_for_name: dict,
    prefix: str = "",
    name_func: Optional[Callable] = None,
) -> str:
    """Create a task name from the kwargs."""
    if name_func:
        return name_func(prefix, kwargs_for_name)
    else:
        # use FHS default if no name_func is provided
        # Handle the case where we have an empty string placeholder in the name by making it
        # empty in the name; Jobmon will not accept quotes in the name
        for key, value in kwargs_for_name.items():
            if value == SERIALIZED_EMPTY_STRING:
                kwargs_for_name[key] = ""

        name = prefix
        for item_name, value in kwargs_for_name.items():
            # trim leading and ending single quote added by make_cli_argument_string
            if value[0] == "'" and value[-1] == "'":
                value = value[1:-1]
            # trim leading [ and ending ] when converting a list to string
            if value[0] == "[" and value[-1] == "]":
                value = value[1:-1]
            # remove illegal characters: '/\\'\" ' from name
            value = (
                value.replace("/", "_")
                .replace("\\", "_")
                .replace('"', "_")
                .replace("'", "_")
                .replace(" ", "_")
            )
            name += f":{item_name}={value}"
        return name


def _is_multidimensional_type(type_hint: Any) -> bool:
    # Check if the type hint is a List
    if (
        hasattr(type_hint, "__origin__")
        and type_hint.__origin__ in BUILT_IN_COLLECTIONS  # type: ignore
    ):
        # Get the arguments inside the List
        args = get_args(type_hint)
        # Check if the first argument is also a List
        if (
            args
            and hasattr(args[0], "__origin__")
            and args[0].__origin__ in BUILT_IN_COLLECTIONS  # type: ignore
        ):
            return True
    return False


def _find_executable_path(executable_name: str) -> str:
    path = shutil.which(executable_name)
    if path is not None:
        return path
    else:
        raise FileNotFoundError(
            f"The executable '{executable_name}' was not found in the system PATH."
        )


def _is_unannotated_built_in_collection_type(obj_type: Any) -> bool:
    """Return whether the ``obj_type`` is an unannotated, built-in collection type.

    Ex. ``list`` would return ``True``, ``tuple`` would return ``True``, ``list[str]``
    would return ``False``.
    """
    return obj_type in BUILT_IN_COLLECTIONS


def _get_collection_type(obj_type: Any) -> Optional[Type]:
    """Return the type of collection.

    To check, we extract the ``__origin__`` component of the ``obj_type`` -- this would be
    the type of collection (list, tuple, set).

    Ex. ``list[int]`` would return ``list``, ``tuple[str]`` would return ``tuple``,
    ``float`` would return ``None`` (because it is not a collection).
    """
    # This catches unannotated built-in collection types (``list``, ``tuple``, ``set``)
    if obj_type in BUILT_IN_COLLECTIONS:
        return obj_type

    return getattr(obj_type, "__origin__", None)


def _get_generic_type_parameters(obj_type: Any) -> Any:
    """Return the parameters for a generic type.

    The results are a tuple of types. For example, a ``list[int]`` returns ``(int,)``, a
    ``tuple[int, str]``, returns ``(int, str)``, and a ``Union[str, None]`` returns
    ``(str, NoneType)``.
    """
    return getattr(obj_type, "__args__", None)


def _is_annotated_built_in_collection_type(obj_type: Any) -> bool:
    """Return whether the type is a properly annotated built-in collection type."""
    collection_type = _get_collection_type(obj_type)
    collection_items_type = _get_generic_type_parameters(obj_type)

    return collection_type in BUILT_IN_COLLECTIONS and collection_items_type is not None


def _zip_collection_items_and_types(
    obj: Collection[str], collection_items_type: tuple
) -> Any:
    """Return a list of tuples mapping collection items and their types.

    Ex. ``obj`` is ``[1, 2, 3]`` and ``collection_items_type`` is ``(int,)`` would return
    ``[(1, int), (2, int), (3, int)]``.
    """
    # If the collection has a single internal type annotation, or if the second
    # internal type annotation is the Ellipsis (``...``), match the first (only) internal
    # annotation to all items in the collection
    if len(collection_items_type) == 1 or collection_items_type[1] == Ellipsis:
        return [(item, collection_items_type[0]) for item in obj]

    # If the collection has multiple internal type annotations but the object
    # provided is a collection of a different length, raise an error
    if len(obj) != len(collection_items_type):
        raise TypeError(
            f"Expected a collection of internal type ``{collection_items_type}`` but got "
            f"``{obj}``. Note the mismatched lengths."
        )

    # Otherwise, match each item of the collection to the type of the corresponding internal
    # type annotation
    return list(zip(obj, collection_items_type))


def is_optional_type(obj_type: Any) -> bool:
    """Return whether the ``obj_type`` is an Optional type.

    Ex. ``Optional[str]`` would return ``True``, ``NoneType`` would return ``True``,
    ``str | None`` would return ``True``, ``str`` would return ``False``.
    """
    matches: List[bool] = []

    # This is true if ``obj_type=NoneType``
    exact_type_match = obj_type in OPTIONAL_TYPES
    matches.append(exact_type_match)

    # This is true if ``obj_type=Optional[str]``. The _name attribute is set by
    # ``typing.Optional``
    possible_annotation = getattr(obj_type, "_name", None)
    annotated_type_match = possible_annotation is not None and any(
        possible_annotation in annotation for annotation in map(str, OPTIONAL_TYPES)
    )
    matches.append(annotated_type_match)

    # This is true if ``obj_type=str | None`` (Note that this is represented as a collection
    # so we can access it using ``_get_generic_type_parameters``)
    possible_union_type = _get_generic_type_parameters(obj_type)
    if possible_union_type is not None:
        possible_union_annotation = any(
            annotation in OPTIONAL_TYPES for annotation in possible_union_type
        )
        matches.append(possible_union_annotation)

    return any(matches)


def get_optional_type_parameter(obj_type: Type) -> Type:
    """Return the type inside an optional."""
    # theoretically a tuple with NoneType and the other type
    types = _get_generic_type_parameters(obj_type)
    if types is not None:
        for t in types:
            if t != type(None):  # noqa: E721
                return t

    raise TypeError(
        f"Looks like we couldn't extract a non-None type from optional-type {obj_type}. This "
        "shouldn't have happend."
    )


def _clean_arg_name(arg_name: str) -> str:
    """Remove the ``--`` from the arg name and convert dashes to underscores."""
    return arg_name.replace("--", "").replace("-", "_")


def _get_short_description(task_function_docstring: docstring_parser.Docstring) -> str:
    short_description = ""
    if task_function_docstring.short_description:
        short_description += "\n" + task_function_docstring.short_description
    return short_description


def make_cli_argument_string(arg_value: Union[str, List]) -> str:
    """Make a CLI argument string from an argument name and value.

    For string, return itself.
    For list, say ["a", "b", "c"], return string '[a,b,c]'
    """
    if isinstance(arg_value, list):
        no_space_string = ",".join(arg_value)
        # we can not take single quote in the string, throw an error
        if "'" in no_space_string:
            raise ValueError(
                f"This version of TaskGenertor cannot serialize list with single quote in it: "
                f"{arg_value}."
            )
        return f"'[{no_space_string}]'"

    return f"'{arg_value}'"


class TaskGenerator:
    """Class for auto-generating jobmon tasks from a python function.

    Arguments on the task_function are inspected to generate the task template, and type
    annotations are used for serializing and deserializing arguments to strings passed on the
    command line.

    The TaskGenerator has built in serializers for:

        * str, int, bool, float
        * list, tuple, set of other serializable types
        * Optional types (and types that are equivalent to Optional types) of other
            serializable types

    Users can also supply serializers for custom types by providing a dictionary of type to
    a tuple of serialization and deserialization functions. Serializers must always turn
    objects into a single string. Lists of strings are represented on the command line as
    ``arg_name='[value1,value2]'``.

    Note:
        While lists, tuples and sets (and their ``Optional`` counterparts) can currently
        serialize empty collections, we can't currently serialize custom types as empty lists.
    """

    def __init__(
        self,
        task_function: Callable,
        serializers: Dict,
        tool_name: str,
        naming_args: Optional[List[str]] = None,
        max_attempts: Optional[int] = None,
        module_source_path: Optional[str] = None,
        name_func: Optional[Callable] = None,
        default_cluster_name: str = "",
        default_compute_resources: Optional[Dict[str, Any]] = None,
        default_resource_scales: Optional[Dict[str, float]] = None,
        yaml_file: Optional[str] = None,
    ) -> None:
        """Initialize TaskGenerator.

        Generates and saves the task template.

        Args:
            task_function: The function that the task will run.
            serializers: A dict mapping types to two callables. The first callable is the
                serializer, which converts an object of the type to a string. And the second is
                a deserializer, which converts a string to an object of the type.
            tool_name: A jobmon tool name for generating tasks.
            naming_args: A list of arguments to use in the task name. If not provided (or
                ``None``), it uses all the arguments. If ``[]`` is provided, it uses no naming
                arguments, and the name of the task will just be the name of the task function.
            max_attempts: The max number of attempts jobmon will make on the tasks
            max_attempts: The max number of attempts jobmon will make on the tasks
            module_source_path: The path to the module source code. If not provided,
                                the module is assumed to be installed in the system.
            name_func: A function that takes in the task name prefix and the kwargs to generate
                the task name. If not provided, the FHS default is used.
            default_cluster_name: The default cluster name to use when creating tasks. If not
                provided, an empty string is used.
            default_compute_resources: The default compute resources when creating tasks.
                If not provided, an empty dictionary is used.
            default_resource_scales: The default resource scales to use when creating tasks.
                If not provided, an empty dictionary is used.
            yaml_file: The path to the yaml file that contains the task template. If not
                provided, the default template is used.
        """
        self.task_function = task_function
        self.serializers = serializers
        self.tool = Tool(tool_name)
        self.max_attempts = max_attempts
        self.mod_name = f"{task_function.__module__}"
        self.name = task_function.__name__
        self.full_path = f"{task_function.__module__}:{self.name}"
        self.module_source_path = module_source_path
        self.params = {
            name: details.annotation
            for name, details in inspect.signature(
                self.task_function
            ).parameters.items()
        }
        if naming_args is None:
            logger.warning(
                "You have specified no naming_args on a task_generator, which means that all "
                "task args will be used in the task name. Usually, this results in a name "
                "that's too long for Jobmon, so you might need to specify naming_args. "
                "Include just the arguments that differ from task to task within a workflow."
            )
        self._naming_args = (
            naming_args if naming_args is not None else self.params.keys()
        )

        self._validate_task_function()

        self._task_template = None
        self._default_cluster_name = default_cluster_name
        self._default_compute_resources = default_compute_resources
        self._default_resource_scales = default_resource_scales
        self._yaml_file = yaml_file

        # If the user provides a name_func, use that to generate the task name
        # otherwise, use FHS default
        self.name_func = name_func

    def _validate_task_function(self) -> None:
        """Check that a task can be generated from the task_function.

        Currently checks:
        - All parameters have type annotations
        - All parameters have serializers
        """
        # Verify that all parameters have type annotations
        unannotated_params = [
            name
            for name, annotation in self.params.items()
            if annotation == inspect._empty
        ]
        if unannotated_params:
            raise TypeError(
                f"Can't generate a task template for {self.name} because "
                "the following parameters are missing type annotations: "
                f"{' '.join(unannotated_params)}"
            )

        # Verify that we know how to serialize all of the parameters
        for arg_name, annotation in self.params.items():
            if not self._is_valid_annotation(annotation):
                raise TypeError(
                    f"Unknown annotation on task_function ``{self.name}`` for parameter "
                    f"``{arg_name}: {annotation}``. Please either provide a serializer and "
                    f"deserializer for this type in the ``serializers`` dictionary, or "
                    f"annotate this parameter with one of the known types: "
                    f"{SIMPLE_TYPES | BUILT_IN_COLLECTIONS | OPTIONAL_TYPES}."
                )

    def _is_valid_annotation(self, annotation: Type) -> bool:
        """Returns True if the annotation is valid, False otherwise."""
        # Check for simple types and types that were provided in the serializers dict
        if annotation in SIMPLE_TYPES:
            return True

        if annotation in self.serializers.keys():
            return True

        # Check for built-in collection types. Note that we validate the underlying type
        if _is_annotated_built_in_collection_type(annotation):
            if all(
                # The type can either be a simple type, a type in the serializers dict, or
                # the Ellipses (``...``) type
                t in SIMPLE_TYPES | set(self.serializers.keys()) | {Ellipsis}
                for t in _get_generic_type_parameters(annotation)
            ):
                return True

        # Check for optional types. Note that we validate the underlying type
        if is_optional_type(annotation):
            return self._is_valid_annotation(get_optional_type_parameter(annotation))

        # If we've made it through all the conditions above, return False
        return False

    def _generate_task_template(self) -> None:
        """Generate and store the task template."""
        # args convert to foo=1 bar=2
        args_template = " ".join(
            f"{arg_name}={{{arg_name}}}" for arg_name in self.params
        )
        args_template = " " + args_template
        if self.module_source_path:
            self._task_template = self.tool.get_task_template(
                template_name=self.name,
                command_template="{executable} "
                + TASK_RUNNER_SUB_COMMAND
                + " --module_name "
                + self.mod_name
                + " --func_name "
                + self.name
                + " --module_source_path "
                + self.module_source_path
                + args_template,
                node_args=self.params.keys(),  # type: ignore
                op_args=["executable"],
                default_cluster_name=self._default_cluster_name,
                default_compute_resources=self._default_compute_resources,
                default_resource_scales=self._default_resource_scales,
                yaml_file=self._yaml_file,
            )
        else:
            self._task_template = self.tool.get_task_template(
                template_name=self.name,
                command_template="{executable} "
                + TASK_RUNNER_SUB_COMMAND
                + " --module_name "
                + self.mod_name
                + " --func_name "
                + self.name
                + args_template,
                node_args=self.params.keys(),  # type: ignore
                op_args=["executable"],
                default_cluster_name=self._default_cluster_name,
                default_compute_resources=self._default_compute_resources,
                default_resource_scales=self._default_resource_scales,
                yaml_file=self._yaml_file,
            )

    def _is_valid_type(self, obj: Any, expected_type: Type) -> bool:
        """Check that the obj type matches the expected type."""
        if _get_collection_type(expected_type) in BUILT_IN_COLLECTIONS:
            matches = isinstance(obj, _get_collection_type(expected_type))  # type: ignore
        elif is_optional_type(expected_type):
            if obj is None:
                matches = True
            else:
                matches = self._is_valid_type(
                    obj, get_optional_type_parameter(expected_type)
                )
        else:
            matches = isinstance(obj, expected_type)

        return matches

    def serialize(self, obj: Any, expected_type: Type) -> str:
        """Serialize ``obj``, validating that it is actually of type ``expected_type``."""
        if not self._is_valid_type(obj, expected_type):
            raise TypeError(
                f"Expected something of type ``{expected_type}``, but got ``{obj}`` of "
                f"type ``{type(obj)}``."
            )

        serialized_result: str

        # raise an error if the expected_type is a multi-dimensional collection
        if _is_multidimensional_type(expected_type):
            raise TypeError(
                f"This version of Task Generator cannot "
                f"serialize multi-dimensional collection: {expected_type}."
            )

        if expected_type in self.serializers.keys():
            # The 0'th index of the serializers dict is the serialization function
            serialized_result = self.serializers[expected_type][0](obj)

        elif expected_type in SIMPLE_TYPES:
            # If the object is an empty string use the placeholder, otherwise just stringify it
            if expected_type == str and len(obj) == 0:
                serialized_result = SERIALIZED_EMPTY_STRING
            else:
                serialized_result = str(obj)

        elif is_optional_type(expected_type):
            if obj is None:
                serialized_result = str(None)
            else:
                serialized_result = self.serialize(
                    obj=obj, expected_type=get_optional_type_parameter(expected_type)
                )

        elif _get_collection_type(expected_type) in BUILT_IN_COLLECTIONS:

            internal_type = _get_generic_type_parameters(expected_type)
            if internal_type is None:
                raise TypeError(
                    f"Cannot serialize collection with unknown internal type: {expected_type}."
                )

            serialized_result = [
                self.serialize(obj=item, expected_type=item_type)  # type: ignore
                for item, item_type in _zip_collection_items_and_types(
                    obj, internal_type
                )
            ]
            # convert list to string
            if isinstance(serialized_result, list):
                serialized_result = f"[{','.join(serialized_result)}]"

        else:
            raise TypeError(
                f"Cannot serialize unknown type {expected_type.__name__}. Upon "
                "constructing this TaskGenerator, please provide serialization and "
                "deserialization functions for this type in the ``serializers`` dictionary."
            )

        # error out if the output is not a string
        if not isinstance(serialized_result, str):
            raise TypeError(
                f"Expected a string to serialize, but got {type(serialized_result)}."
            )
        return serialized_result

    def serialize_array(self, obj: Any, expected_type: Type) -> List:
        """Serialize obj into a list of serialized string."""
        if isinstance(obj, list):
            return [self.serialize(item, expected_type) for item in obj]
        else:
            return [self.serialize(obj, expected_type)]

    def deserialize(self, obj: str, obj_type: Type) -> Any:
        """Deserialize ``obj``."""
        deserialized_result: Any
        # error out if the input is not a string
        if not isinstance(obj, str):
            raise TypeError(f"Expected a string to deserialize, but got {type(obj)}.")

        # raise an error if the expected_type is a multi-dimensional collection
        if _is_multidimensional_type(obj_type):
            raise TypeError(
                f"This version of Task Generator cannot "
                f"serialize multi-dimensional collection: {obj_type}."
            )

        if is_optional_type(obj_type):
            if obj == str(None):
                return None
            else:
                obj_type = get_optional_type_parameter(obj_type)

        if obj_type in self.serializers.keys():
            # To support dynamic length list input, the deserializer only takes string
            if not isinstance(obj, str):
                raise TypeError(
                    f"Expected a string to deserialize, but got {type(obj)}."
                )
            # The 1'st index of the serializers dict is the deserialization function
            deserialized_result = self.serializers[obj_type][1](obj)

        elif obj_type in SIMPLE_TYPES:
            # If we simply called something like ``bool("False")`` we would get ``True`` due to
            # the way Python thinks about truthiness. So instead we use ``ast.literal_eval``
            # to evaluate the string as a Python literal.
            if obj_type == bool:
                deserialized_result = ast.literal_eval(obj)  # type: ignore

            # If the object is our serialized empty string, return an empty string
            elif obj_type == str and obj == SERIALIZED_EMPTY_STRING:
                deserialized_result = ""

            # For all other simple types, we can simply call the type on ``obj``
            else:
                deserialized_result = obj_type(obj)

        elif _is_unannotated_built_in_collection_type(obj_type):
            raise TypeError(
                f"The provided type annotation ``{obj_type}`` does not provide enough "
                "information to deserialize.  Please provide a more specific type "
                "annotation like ``list[int]`` or ``tuple[str]``."
            )

        elif _is_annotated_built_in_collection_type(obj_type):
            # input is a string, convert it to a list
            try:
                middle_result = ast.literal_eval(obj)
                # if the input is a single item, convert it to a list
                if type(middle_result) not in BUILT_IN_COLLECTIONS:
                    middle_result = [middle_result]
            except Exception:
                # handle input like "[a,b,c]"
                # remove leading and tailing space
                obj = obj.strip()
                # handle input like "'[a,b,c]'"
                if obj[0] == "'" and obj[-1] == "'":
                    obj = obj[1:-1]
                # remove leading and tailing brackets
                if obj[0] in ["[", "("] and obj[-1] in ["]", ")"]:
                    obj = obj[1:-1]

                middle_result = obj.split(",")

            deserialized_result = []
            for item in middle_result:
                if obj_type.__args__[0] in self.serializers.keys():
                    deserialized_result.append(
                        self.serializers[obj_type.__args__[0]][1](item)
                    )
                else:
                    deserialized_result.append(obj_type.__args__[0](item))

        else:
            raise TypeError(
                f"Cannot deserialize unknown type {obj_type.__name__}. Upon "
                "constructing this TaskGenerator, please provide serialization and "
                "deserialization functions for this type in the ``serializers`` dictionary."
            )

        return deserialized_result

    def _cluster_resource_check(
        self, cluster_name: str, compute_resources: Optional[Dict]
    ) -> str:
        """Make sure a cluster name is available to use, and compute_resource is a dict."""
        # add compute_resources type protection
        if compute_resources:
            if not isinstance(compute_resources, dict):
                raise TypeError(
                    f"Expected a dictionary for compute_resources, "
                    f"but got {type(compute_resources)}."
                )
        # if cluster_name is still an empty string, use the default cluster name
        if cluster_name == "":
            cluster_name = self._default_cluster_name
        # if cluster_name is still an empty string, leave to wf to handle
        logger.info(
            "Cluster name is empty. Will use the wf default if one is provided."
        )
        return cluster_name

    def create_task(
        self,
        cluster_name: str = "",
        compute_resources: Optional[Dict] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Task:
        """Create a task for the task_function with the given kwargs."""
        # if the task template is not generated, generate it
        if self._task_template is None:
            self._generate_task_template()

        # check input and adjust the cluster_name value
        cluster_name = self._cluster_resource_check(cluster_name, compute_resources)

        executable_path = _find_executable_path(executable_name=TASK_RUNNER_NAME)
        # Serialize the kwargs
        serialized_kwargs = {
            name: self.serialize(obj=value, expected_type=self.params[name])
            for name, value in kwargs.items()
        }

        # assign the None value to args in self.params.keys but not in serialized_kwargs.keys
        for name in self.params.keys():
            if name not in serialized_kwargs.keys():
                serialized_kwargs[name] = str(None)

        # Format the kwargs for the task
        kwargs_for_task = {
            name: make_cli_argument_string(arg_value=value)
            for name, value in serialized_kwargs.items()
        }
        # We want a slightly different format to put list kwargs into the name
        kwargs_for_name = {
            name: value
            for name, value in serialized_kwargs.items()
            if name in self._naming_args
        }

        name = create_task_name(
            kwargs_for_name=kwargs_for_name, prefix=self.name, name_func=self.name_func
        )

        # Create the task
        task = self._task_template.create_task(  # type: ignore
            name=name,
            cluster_name=cluster_name,
            compute_resources=compute_resources,
            resource_scales=resource_scales,
            max_attempts=self.max_attempts,
            executable=executable_path,
            **kwargs_for_task,  # type: ignore
        )

        return task

    def create_tasks(
        self,
        cluster_name: str = "",
        compute_resources: Optional[Dict] = None,
        resource_scales: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> List[Task]:
        """Create a task array for the task_function with the given kwargs."""
        # if the task template is not generated, generate it
        if self._task_template is None:
            self._generate_task_template()

        # check input and adjust the cluster_name value
        cluster_name = self._cluster_resource_check(cluster_name, compute_resources)

        executable_path = _find_executable_path(executable_name=TASK_RUNNER_NAME)
        # Serialize the kwargs
        serialized_kwargs = {
            name: self.serialize_array(obj=value, expected_type=self.params[name])
            for name, value in kwargs.items()
        }

        # Format the kwargs for the task
        # Each individual element in the array should be formatted before sending to the cli
        kwargs_for_task = {}
        for name, value in serialized_kwargs.items():
            if isinstance(value, list):
                kwargs_for_task[name] = [
                    make_cli_argument_string(arg_value=item) for item in value
                ]
            else:
                kwargs_for_task[name] = make_cli_argument_string(arg_value=value)

        # name is auto for array

        # Create the task
        tasks = self._task_template.create_tasks(  # type: ignore
            cluster_name=cluster_name,
            compute_resources=compute_resources,
            resource_scales=resource_scales,
            max_attempts=self.max_attempts,
            executable=executable_path,
            **kwargs_for_task,  # type: ignore
        )

        return tasks

    def run(self, args: List[str]) -> Any:
        """Run the task_function with the given args and return any result."""
        # Parse the args
        parsed_arg_value_pairs: Dict[str, Union[str, List[str]]] = dict()
        # args is a list of string like ["arg1=1", "arg2=[2, 3]", "arg1=4"]
        for arg in args:
            arg_name, arg_value = arg.split("=")
            # if the arg_name key, already exists, append the value to the list
            if arg_name in parsed_arg_value_pairs.keys():
                if isinstance(parsed_arg_value_pairs[arg_name], list):
                    parsed_arg_value_pairs[arg_name].append(arg_value)  # type: ignore
                else:
                    parsed_arg_value_pairs[arg_name] = [
                        parsed_arg_value_pairs[arg_name],  # type: ignore
                        arg_value,
                    ]
            else:
                parsed_arg_value_pairs[arg_name] = arg_value  # type: ignore

        # Raise an error if the user did not provide all of the arguments for the task_function
        if parsed_arg_value_pairs.keys() != self.params.keys():
            raise ValueError(
                "Some arguments to the task_function were not provided on the command line. "
                "The following arguments were not provided: "
                f"{set(self.params.keys()) - set(parsed_arg_value_pairs.keys())}"
            )

        # Deserialize the args, catching any errors that may come from the deserialization fn
        deserialized_args = dict()
        for arg_name, arg_value in parsed_arg_value_pairs.items():  # type: ignore
            try:
                deserialized_args[arg_name] = self.deserialize(
                    obj=arg_value, obj_type=self.params[arg_name]
                )
            except Exception:
                raise ValueError(
                    f"Could not deserialize argument ``{arg_name}`` with value "
                    f"``{arg_value}``. This error could be the result of a bug in the "
                    f"deserialization function, or a result of not providing a value for "
                    f"this argument on the command line."
                )

        # Run the task_function
        return self.task_function(**deserialized_args)

    def help(self) -> str:
        """Return help text for the task_function."""
        # Parse the task function's docstring - Note that there may be nothing!
        task_function_docstring = docstring_parser.parse(  # type: ignore
            self.task_function.__doc__  # type: ignore
        )  # type: ignore

        # Map the parameter names to their annotations and descriptions
        task_function_docstring_param_names_to_annotations = {
            param.arg_name: param.type_name for param in task_function_docstring.params
        }
        task_function_docstring_param_names_to_descriptions = {
            param.arg_name: param.description
            for param in task_function_docstring.params
        }

        # Get the short description
        short_description = _get_short_description(task_function_docstring)

        # Get the parameter names and descriptions
        parameter_names_and_descriptions = self._get_param_names_and_descriptions(
            task_function_docstring_param_names_to_annotations,
            task_function_docstring_param_names_to_descriptions,
        )

        # Combine the full help text and return it
        return "\n".join(
            [
                HELP_TEXT_INTRO_FORMAT.format(full_path=self.full_path),
                short_description,
                *parameter_names_and_descriptions,
            ]
        )

    def _get_param_names_and_descriptions(
        self,
        task_function_docstring_param_names_to_annotations: Dict,
        task_function_docstring_param_names_to_descriptions: Dict,
    ) -> List[str]:
        parameter_names_and_descriptions = []

        for param, annotation in self.params.items():
            # If the parameter has an annotation, use that for the text.
            if param in task_function_docstring_param_names_to_annotations.keys():
                annotation_string = task_function_docstring_param_names_to_annotations[
                    param
                ]

            else:
                # Otherwise try to pull the annotation from the self.params annotation (this
                # will work for simple types like str, int, etc.)
                try:
                    annotation_string = annotation.__name__

                # Otherwise just stringify the annotation (this will work for custom or union
                # types like ``str | None``)
                except AttributeError:
                    annotation_string = str(annotation)

            this_param_text = f"\t--{param} [{annotation_string}]: "

            # If the parameter has a description in the docstring, add that to the text
            if param in task_function_docstring_param_names_to_descriptions.keys():
                this_param_text += (
                    f"{task_function_docstring_param_names_to_descriptions[param]}"
                )
            else:
                this_param_text += "No description found in docstring."

            parameter_names_and_descriptions.append(this_param_text)

        return parameter_names_and_descriptions


def task_generator(
    serializers: Dict,
    tool_name: str,
    naming_args: Optional[List[str]] = None,
    max_attempts: Optional[int] = None,
    module_source_path: Optional[str] = None,
    default_cluster_name: str = "",
    default_compute_resources: Optional[Dict[str, Any]] = None,
    default_resource_scales: Optional[Dict[str, float]] = None,
    yaml_file: Optional[str] = None,
) -> Callable:
    """Decorator for generating jobmon tasks from a python function."""

    def wrapper(task_function: Callable) -> TaskGenerator:
        """Wrap the task_function with a TaskGenerator."""
        return TaskGenerator(
            task_function=task_function,
            serializers=serializers,
            tool_name=tool_name,
            naming_args=naming_args,
            max_attempts=max_attempts,
            module_source_path=module_source_path,
            default_cluster_name=default_cluster_name,
            default_compute_resources=default_compute_resources,
            default_resource_scales=default_resource_scales,
            yaml_file=yaml_file,
        )

    return wrapper


def get_tasks_by_node_args(
    workflow: Workflow,
    task_generator: TaskGenerator,
    node_args_dict: dict[str, Any],
    error_on_empty: bool = True,
) -> list[Task]:
    """Get the tasks of a TaskGenerator in a workflow that have the given node arguments.

    This method does some value serialization and formatting before handing the node argument
    string over to ``workflow.get_tasks_by_node_args``.
    """
    # Re-serialize the node args dict and format them as CLI arguments
    serialized_node_args = {
        arg_name: make_cli_argument_string(
            arg_value=task_generator.serialize(
                obj=arg_value, expected_type=task_generator.params[arg_name]
            ),
        )
        for arg_name, arg_value in node_args_dict.items()
    }

    try:
        result = workflow.get_tasks_by_node_args(
            task_template_name=task_generator.name, **serialized_node_args
        )
    except ValueError as err:
        if error_on_empty:
            raise err
        result = []

    if not result and error_on_empty:
        raise ValueError(
            f"There were no tasks in the workflow that matched {node_args_dict}"
        )

    return result


class TaskGeneratorDocumenter(Directive):
    """Directive for generating documentation for a single task generator."""

    required_arguments = 1
    optional_arguments = 1
    final_argument_whitespace = True
    option_spec = {"optional": lambda x: x}  # Defines the 'optional' option

    def run(self) -> list[nodes.Node]:
        """The function sphinx/docutils use to generate documentation for the directive."""
        module_name = self.arguments[0]
        # if there are more than one arg, the second will be module path
        module_path = self.options.get("optional", None)
        task_generator = self._load_task_generator(module_name, module_path)

        return _generate_nodes(task_generator, self.state)

    def _load_task_generator(
        self, task_generator_path: str, module_path: Optional[str] = None
    ) -> TaskGenerator:
        """Load the task generator from the given path."""
        try:
            module_name, attr_name = task_generator_path.split(":", 1)
        except ValueError:
            raise self.error(
                f'"{task_generator_path}" is not of format "module:parser"'
            )

        try:
            if module_path:
                spec = importlib.util.spec_from_file_location(
                    module_name, module_path
                )  # type: ignore
                mod = importlib.util.module_from_spec(spec)  # type: ignore
                spec.loader.exec_module(mod)  # type: ignore
            else:
                mod = __import__(module_name, globals(), locals(), [attr_name])
        except (Exception, SystemExit) as exc:
            err_msg = f'Failed to import "{attr_name}" from "{module_name}". '
            if isinstance(exc, SystemExit):
                err_msg += "The module appeared to call sys.exit()"
            else:
                err_msg += (
                    f"The following exception was raised:\n{traceback.format_exc()}"
                )
            raise self.error(err_msg)

        if not hasattr(mod, attr_name):
            raise self.error(f'Module "{module_name}" has no attribute "{attr_name}"')

        task_generator = getattr(mod, attr_name)

        if not isinstance(task_generator, TaskGenerator):
            raise self.error(
                f'Attribute "{attr_name}" of module "{module_name}" is not a TaskGenerator'
            )

        return task_generator


class TaskGeneratorModuleDocumenter(Directive):
    """Directive for generating documentation for all the task generators in a module."""

    required_arguments = 1
    optional_arguments = 1
    final_argument_whitespace = True
    option_spec = {"optional": lambda x: x}  # Defines the 'optional' option

    def run(self) -> list[nodes.Node]:
        """The function sphinx/docutils use to generate documentation for the directive."""
        module_name = self.arguments[0]
        # if there are more than one arg, the second will be module path
        module_path = self.options.get("optional", None)
        module, task_generators = self._load_module_task_generators(
            module_name, module_path
        )

        section = self._generate_module_section(module_name, module, task_generators)

        for task_generator in task_generators:
            section.extend(_generate_nodes(task_generator, self.state))

        return [section]

    def _load_module_task_generators(
        self, module_name: str, module_path: Optional[str] = None
    ) -> tuple[ModuleType, list[TaskGenerator]]:
        """Load all the task generators in a given module."""
        task_generators = []

        try:
            if module_path:
                spec = importlib.util.spec_from_file_location(
                    module_name, module_path
                )  # type: ignore
                mod = importlib.util.module_from_spec(spec)  # type: ignore
                spec.loader.exec_module(mod)  # type: ignore
            else:
                mod = importlib.import_module(module_name)
        except (Exception, SystemExit) as exc:
            err_msg = f'Failed to import "{module_name}". '
            if isinstance(exc, SystemExit):
                err_msg += "The module appeared to call sys.exit()"
            else:
                err_msg += (
                    f"The following exception was raised:\n{traceback.format_exc()}"
                )
            raise self.error(err_msg)

        for attr_name in dir(mod):
            if attr_name.startswith("_"):
                continue

            attr = getattr(mod, attr_name)

            if isinstance(attr, TaskGenerator):
                task_generators.append(attr)

        return mod, task_generators

    def _generate_module_section(
        self,
        module_name: str,
        module: ModuleType,
        task_generators: Iterable[TaskGenerator],
    ) -> nodes.Node:
        """Makes the docutils node for the module section.

        Includes the docstring for the module, and a list of task generator names. Doesn't
        include any task generator detailed documentation.
        """
        # Set up base node
        section = nodes.section(
            "",
            nodes.title(text=module_name),
            ids=[nodes.make_id(module_name)],
            names=[nodes.fully_normalize_name(module_name)],
        )

        # Use sphinx and docutils tooling to format the docstring into rST and then parse it
        # into a docutils node.
        result = statemachine.ViewList()
        if module.__doc__:
            for line in statemachine.string2lines(
                module.__doc__, tab_width=4, convert_whitespace=True
            ):
                result.append(line, module_name)

        task_generator_path_lines = [""]
        for task_generator in task_generators:
            task_generator_path_lines.append(".. code-block:: shell")
            task_generator_path_lines.append("")
            task_generator_path_lines.append(
                f"    {TASK_RUNNER_NAME} {task_generator.full_path}"
            )
            task_generator_path_lines.append("")

        for line in task_generator_path_lines:
            result.append(line, module_name)

        sphinx_nodes.nested_parse_with_titles(self.state, result, section)

        return section


def _generate_nodes(task_generator: TaskGenerator, state: Any) -> nodes.Node:
    """Makes a docutils node with the documentation for a task generator.

    Includes the docstring description for the task generator and all the arguments.
    """
    # Set up base node
    section = nodes.section(
        "",
        nodes.title(text=task_generator.name),
        ids=[nodes.make_id(task_generator.full_path)],
        names=[nodes.fully_normalize_name(task_generator.full_path)],
    )

    # Get rST lines for the description and options
    parsed_docstring = docstring_parser.parse(str(task_generator.task_function.__doc__))
    lines = []
    lines.extend(
        _format_description(
            task_generator=task_generator, parsed_docstring=parsed_docstring
        )
    )
    lines.extend(
        _format_options(
            task_generator=task_generator, parsed_docstring=parsed_docstring
        )
    )

    # Convert the rST lines into a docutils node
    result = statemachine.ViewList()
    for line in lines:
        result.append(line, task_generator.name)
    sphinx_nodes.nested_parse_with_titles(state, result, section)

    return [section]


def _format_description(
    task_generator: TaskGenerator, parsed_docstring: docstring_parser.Docstring
) -> list[str]:
    """Format the description of the task generator into proper rST."""
    lines = []

    # Description from the docstring
    if parsed_docstring.short_description:
        lines.append(parsed_docstring.short_description)
        lines.append("")

    if parsed_docstring.long_description:
        for line in statemachine.string2lines(
            parsed_docstring.long_description, tab_width=4, convert_whitespace=True
        ):
            lines.append(line)
        lines.append("")

    # How to run on the cli
    lines.append(".. code-block:: shell")
    lines.append("")
    lines.append(f"    {TASK_RUNNER_NAME} {task_generator.full_path} [OPTIONS]")
    lines.append("")

    return lines


def _format_options(
    task_generator: TaskGenerator, parsed_docstring: docstring_parser.Docstring
) -> list[str]:
    """Format the options of the task generator into proper rST."""
    param_docs = {param.arg_name: param for param in parsed_docstring.params}
    lines = []

    # we use rubric to provide some separation without exploding the table
    # of contents
    lines.append(".. rubric:: Options")
    lines.append("")
    for param, annotation in task_generator.params.items():
        if is_optional_type(annotation):
            underlying_optional_type = get_optional_type_parameter(annotation)
            annotation_name = f"OPTIONAL[{underlying_optional_type.__name__.upper()}]"
        elif isinstance(annotation, type):
            annotation_name = annotation.__name__.upper()
        else:
            # it can be typing._GenericAlias for list type annotation
            annotation_name = str(annotation).upper()

        lines.append(f".. option:: --{param} <{annotation_name}>")
        lines.append("")
        if param in param_docs and param_docs[param].description:
            for line in statemachine.string2lines(
                param_docs[param].description, tab_width=4, convert_whitespace=True
            ):
                lines.append(line)
            lines.append("")

    return lines


def setup(app: application.Sphinx) -> dict:
    """The function that registers the extension with sphinx."""
    app.add_directive("task_generator", TaskGeneratorDocumenter)
    app.add_directive("task_generator_module", TaskGeneratorModuleDocumenter)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
