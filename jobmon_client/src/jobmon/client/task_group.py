from __future__ import annotations
from collections import defaultdict

from typing import Dict, Iterable, Iterator, List, Optional, Set, Union
from jobmon.client.task import Task


def get_label_map(
    tasks: Set[Task], labels: List[str]
) -> Dict[str, Dict[Union[str, int], Set[Task]]]:
    """Build up a mapping of tasks by label."""
    label_map: Dict[str, Dict[Union[str, int], Set[Task]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for task in tasks:
        for label in labels:
            val = task.labels[label]
            label_map[label][val].add(task)

    return dict(label_map)


def subset_by_label(
    label_map: Dict[str, Dict[Union[str, int], Set[Task]]],
    subsetter: Dict[str, Union[str, int, List[Union[str, int]]]],
) -> Set[Task]:
    # Create a dictionary to keep track of the number of matches for each task
    num_matches: Dict[Task, int] = {
        value: 0
        for val_dict in label_map.values()
        for task_set in val_dict.values()
        for value in task_set
    }

    # Iterate over all labels and values in the subsetter
    for label, values in subsetter.items():
        # convert it to a list with a single element
        if isinstance(values, str):
            iter_values: List[Union[str, int]] = [values]
        if not isinstance(values, Iterable):
            iter_values = [values]

        # Iterate over all values and increment the number of matches for matching tasks
        for val in iter_values:
            for match in label_map[label][val]:
                num_matches[match] += 1

    # Create a set of all tasks that have a number of matches equal to the number of labels in
    # the subsetter
    all_matches = set(
        task
        for task, matches in num_matches.items()
        if matches == len(subsetter.keys())
    )
    return all_matches


# Stubs
class TaskGroup:
    """TaskGroup stub."""

    tasks: Set[Task]
    name: str

    def __init__(self, tasks: Iterable[Task], name: str = ""):
        self.tasks = set(tasks)
        self.name = name

    def add_task(self, task: Task):
        """Adds a task to the group."""
        self.tasks.add(task)

    def add_tasks(self, tasks: Iterable[Task]):
        """Adds multiple tasks to the group."""
        for task in tasks:
            self.add_task(task)

    def add_labels(self, label_dict: Dict, subsetter: Optional[Dict] = None):
        if subsetter is not None:
            tasks = self.get_subset(**subsetter)
        else:
            tasks = self.tasks
        for label_name, label_value in label_dict.items():
            [task.add_label(label_name, label_value) for task in tasks]

    def union(self, other: TaskGroup, new_name: str = "") -> TaskGroup:
        """Combine two task groups."""
        return TaskGroup(self.tasks | other.tasks, new_name)

    def get_subset(self, **kwargs) -> Set[Task]:
        # build up a mapping of tasks by label
        label_map = get_label_map(self.tasks, list(kwargs.keys()))
        return subset_by_label(label_map, kwargs)

    def get_subgroup(
        self, empty_okay: bool = False, new_name: str = "", **kwargs
    ) -> TaskGroup:
        """probably the same implementation details as workflow.get_tasks_by_node_args

        kwargs can be task arguments or attributes.

        raises an error if the group is empty and empty_okay is False.
        """
        tasks = self.get_subset(**kwargs)

        if not tasks and not empty_okay:
            raise ValueError("No matching tasks in this TaskGroup.")

        return TaskGroup(tasks, new_name)

    def get_task(self, **kwargs) -> Task:
        """Raises an error if it doesn't uniquely identify a task.

        kwargs can be task arguments or attributes.
        """
        tasks = list(self.get_subset(**kwargs))

        if len(tasks) != 1:
            raise ValueError("Provided labels do not uniquely identify a task.")

        return tasks[0]

    def interleave_upstream(
        self,
        upstream_group: TaskGroup,
        dependency_specification: Dict[str, str],
        subsetter: Optional[Dict] = None,
        upstream_subsetter: Optional[Dict] = None,
    ):
        """Set interleaving dependencies between TaskGroups via label value matching.

        Dependencies are set between tasks where the values in all the specified labels match
        and the interleaved tasks fulfill any specified subset criteria.

        Args:
            upstream_group: The task group to interleave dependencies from.
            dependency_specification: A dictionary mapping label names between groups. Keys
                are labels on tasks in this group, and values are labels in tasks in the
                upstream_group.
            subsetter: A dictionary used to subset the tasks of the current TaskGroup by
                filtering label keys and values based on the supplied dict.
            upstream_subsetter: A dictionary used to subset the tasks of the upstream TaskGroup
                by filtering label keys and values based on the supplied dict.
        """

        # get the set of tasks for this TaskGroup based on the provided subsetter
        if subsetter is not None:
            tasks = self.get_subset(**subsetter)
        else:
            tasks = self.tasks

        # get the set of tasks for this TaskGroup based on the provided subsetter
        if upstream_subsetter is not None:
            upstream_tasks = upstream_group.get_subset(**upstream_subsetter)
        else:
            upstream_tasks = upstream_group.tasks
        upstream_label_map = get_label_map(
            upstream_tasks, list(dependency_specification.values())
        )

        for task in tasks:
            matches = upstream_tasks.copy()
            for key, upstream_key in dependency_specification.items():
                label_value = task.labels[key]
                matches.intersection(upstream_label_map[upstream_key][label_value])
            task.add_upstreams(list(matches))

    def interleave_downstream(
        self,
        upstream_group: TaskGroup,
        dependency_specification: Dict[str, str],
        subsetter: Optional[Dict] = None,
        upstream_subsetter: Optional[Dict] = None,
    ):
        pass

    def __iter__(self) -> Iterator[Task]:
        for task in self.tasks:
            yield task

    def __or__(self, other_group: TaskGroup) -> TaskGroup:
        """Combine two groups."""
        return self.union(other_group)

    def __len__(self) -> int:
        return len(self.tasks)
