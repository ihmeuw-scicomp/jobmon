import ast
import inspect
import shutil
from typing import Any, Callable, Collection, Dict, List, Optional, Type, Union

import docstring_parser

from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.workflow import Workflow
from jobmon.core import __version__ as core_version

SIMPLE_TYPES = {str, int, float, bool}
BUILT_IN_COLLECTIONS = {list, tuple, set}
OPTIONAL_TYPES = {Optional, type(None)}

TASK_RUNNER_NAME = "worker_node_entry_point"
TASK_RUNNER_SUB_COMMAND = "task_generator"
HELP_TEXT_INTRO_FORMAT = "Command Line Documentation for {full_path}"

SERIALIZED_EMPTY_STRING = '""'


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


def make_cli_argument_string(serialized_kwargs: Any) -> str:
    """Make a CLI argument string from an argument name and value.

    Be aware that args ard passed in as a list of strings leading by "--args",
    with format key=value or key=[value1, value2], separated by ";".
    This function will return a string with the format '--args "arg1=1;arg2=[2, 3]"'.
    Note: there should be no space after the ";".
    """
    result_elements = []
    for key, value in serialized_kwargs.items():
        if isinstance(value, list):
            result_elements.append(f"{key}=[{', '.join(value)}]")
        else:
            result_elements.append(f"{key}={value}")
    return ";".join(result_elements)


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
    a tuple of serialization and deserialization functions. Serializers can return either a
    string or a list of strings. Lists of strings are represented on the command line as
    ``--arg-name value1 --arg-name value2``. If you're serializing to a list of strings, the
    deserializer should be prepared to handle either a single string or a list of strings.

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
    ) -> None:
        """Initialize TaskGenerator.

        Generates and saves the task template.

        Args:
            task_function: The function that the task will run.
            serializers: A dict mapping types to two callables. The first callable is the
                serializer, which converts an object of the type to a string. And the second is
                a deserializer, which converts a string to an object of the type.
            tool_name: A jobmon tool name for generating tasks.
            naming_args: A list of arguments to use in the task name. If not provided, uses all
            max_attempts: The max number of attempts jobmon will make on the tasks
            module_source_path: The path to the module source code. If not provided,
                                the module is assumed to be installed in the system.
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
        self._naming_args = (
            naming_args if naming_args is not None else self.params.keys()
        )

        self._validate_task_function()

        self._generate_task_template()

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
        if self.module_source_path:
            self._task_template = self.tool.get_task_template(
                template_name=self.name,
                command_template="{executable} "
                + TASK_RUNNER_SUB_COMMAND
                + " --expected_jobmon_version "
                + core_version
                + " --module_name "
                + self.mod_name
                + " --func_name "
                + self.name
                + " --module_source_path "
                + self.module_source_path
                + " --args {tgargs}",
                node_args=["tgargs"],
                op_args=["executable"],
            )
        else:
            self._task_template = self.tool.get_task_template(
                template_name=self.name,
                command_template="{executable} "
                + TASK_RUNNER_SUB_COMMAND
                + " --expected_jobmon_version "
                + core_version
                + " --module_name "
                + self.mod_name
                + " --func_name "
                + self.name
                + " --args {tgargs}",
                node_args=["tgargs"],
                op_args=["executable"],
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

    def serialize(self, obj: Any, expected_type: Type) -> Union[str, List]:
        """Serialize ``obj``, validating that it is actually of type ``expected_type``."""
        if not self._is_valid_type(obj, expected_type):
            raise TypeError(
                f"Expected something of type ``{expected_type}``, but got ``{obj}`` of "
                f"type ``{type(obj)}``."
            )

        serialized_result: Union[str, List[str]]

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
            # Raise an error if we've been given a multi-dimensional collection
            if any(type(item) in BUILT_IN_COLLECTIONS for item in obj):
                raise TypeError(
                    f"Cannot serialize multi-dimensional collection: {obj}."
                )

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

        else:
            raise TypeError(
                f"Cannot serialize unknown type {expected_type.__name__}. Upon "
                "constructing this TaskGenerator, please provide serialization and "
                "deserialization functions for this type in the ``serializers`` dictionary."
            )

        return serialized_result

    def deserialize(self, obj: Union[str, List], obj_type: Type) -> Any:
        """Deserialize ``obj``."""
        deserialized_result: Any

        if obj_type in self.serializers.keys():
            # The 1'st index of the serializers dict is the deserialization function
            deserialized_result = self.serializers[obj_type][1](obj)

        elif obj_type in SIMPLE_TYPES:
            # If we simply called something like ``bool("False")`` we would get ``True`` due to
            # the way Python thinks about truthiness. So instead we use ``ast.literal_eval``
            # to evaluate the string as a Python literal.
            if obj_type == bool:
                deserialized_result = ast.literal_eval(obj)  # type: ignore

            # For all other simple types, we can simply call the type on ``obj``
            else:
                deserialized_result = obj_type(obj)

        elif is_optional_type(obj_type):
            if obj == str(None):
                deserialized_result = None
            else:
                deserialized_result = self.deserialize(
                    obj=obj, obj_type=get_optional_type_parameter(obj_type)
                )

        elif _is_unannotated_built_in_collection_type(obj_type):
            raise TypeError(
                f"The provided type annotation ``{obj_type}`` does not provide enough "
                "information to deserialize.  Please provide a more specific type "
                "annotation like ``list[int]`` or ``tuple[str]``."
            )

        elif _is_annotated_built_in_collection_type(obj_type):
            collection_items_type = _get_generic_type_parameters(obj_type)

            # Raise an error if we've been given a multi-dimensional collection
            if any(
                _is_unannotated_built_in_collection_type(item_type)
                or _is_annotated_built_in_collection_type(item_type)
                for item_type in collection_items_type
            ):
                raise TypeError(
                    f"Cannot deserialize multi-dimensional collection: {obj}."
                )

            item_type_pairs = _zip_collection_items_and_types(
                obj, collection_items_type
            )
            deserialized_result = _get_collection_type(obj_type)(  # type: ignore
                [
                    self.deserialize(obj=item, obj_type=item_type)
                    for item, item_type in item_type_pairs
                ]
            )

        else:
            raise TypeError(
                f"Cannot deserialize unknown type {obj_type.__name__}. Upon "
                "constructing this TaskGenerator, please provide serialization and "
                "deserialization functions for this type in the ``serializers`` dictionary."
            )

        return deserialized_result

    def create_task(self, compute_resources: Dict, **kwargs: Any) -> Task:
        """Create a task for the task_function with the given kwargs."""
        executable_path = _find_executable_path(executable_name=TASK_RUNNER_NAME)

        # Serialize the kwargs
        serialized_kwargs = {
            name: self.serialize(obj=value, expected_type=self.params[name])
            for name, value in kwargs.items()
        }

        # Format the kwargs for the task
        tg_arg_string = make_cli_argument_string(serialized_kwargs)
        # We want a slightly different format to put list kwargs into the name
        kwargs_for_name = {
            name: ",".join(value) if isinstance(value, list) else value
            for name, value in serialized_kwargs.items()
            if name in self._naming_args
        }

        # Handle the case where we have an empty string placeholder in the name by making it
        # empty in the name; Jobmon will not accept quotes in the name
        for key, value in kwargs_for_name.items():
            if value == SERIALIZED_EMPTY_STRING:
                kwargs_for_name[key] = ""

        name = (
            self.name
            + ":"
            + ":".join(f"{name}={value}" for name, value in kwargs_for_name.items())
        )
        # trim ending :
        if name[-1] == ":":
            name = name[:-1]

        # Create the task
        task = self._task_template.create_task(
            name=name,
            compute_resources=compute_resources,
            max_attempts=self.max_attempts,
            executable=executable_path,
            tgargs=f"'{tg_arg_string}'",
        )

        return task

    def run(self, parsed_arg_value_pairs: Dict) -> Any:
        """Run the task_function with the given args and return any result."""
        # Raise an error if the user did not provide all of the arguments for the task_function
        if parsed_arg_value_pairs.keys() != self.params.keys():
            raise ValueError(
                "Some arguments to the task_function were not provided on the command line. "
                "The following arguments were not provided: "
                f"{set(self.params.keys()) - set(parsed_arg_value_pairs.keys())}"
            )

        # Deserialize the args, catching any errors that may come from the deserialization fn
        deserialized_args = dict()
        for arg_name, arg_value in parsed_arg_value_pairs.items():
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
        )

    return wrapper


def get_tasks_by_node_args(
    workflow: Workflow,
    task_generator: TaskGenerator,
    node_args_dict: Dict,
    error_on_empty: bool = True,
) -> List[Task]:
    """Get the tasks of a TaskGenerator in a workflow that have the given node arguments.

    This method does some value serialization and formatting before handing the node argument
    string over to ``workflow.get_tasks_by_node_args``.
    """
    try:
        result = workflow.get_tasks_by_node_args(
            task_template_name=task_generator.name, **node_args_dict  # type: ignore
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
