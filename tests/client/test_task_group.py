from __future__ import annotations
from typing import List
import uuid
import warnings

from jobmon.client.task_group import TaskGroup
from jobmon.client.tool import Tool

import itertools
import pytest


@pytest.fixture(scope="function")
def tool(client_env):
    return Tool(name=str(uuid.uuid4()))


class TestUtils:
    """Test dunder methods and other simple utils."""

    @pytest.fixture(scope="function")
    def template(self, tool):
        return tool.get_task_template(
            template_name="template",
            command_template="script {arg}",
            node_args=["arg"],
            task_args=[],
            op_args=[],
        )

    def test_iter(self, template):
        """Test that the TaskGroup is iterable."""
        task_list = template.create_tasks(arg=[1, 2])
        task_group = TaskGroup(task_list, name="")
        task_set = set(task_group)  # set() just iterates through the argument provided.

        assert set(task_list) == task_set

    def test_tasks(self, template):
        """Test that the .tasks attribute is a set of all the contained tasks."""
        task_list = template.create_tasks(arg=[1, 2])
        task_group = TaskGroup(task_list, name="")

        assert set(task_group) == task_group.tasks

    def test_union(self, template):
        """Test that TaskGroups can be combined together."""
        list1 = template.create_tasks(arg=[1, 2])
        list2 = template.create_tasks(arg=[3, 4])

        group1 = TaskGroup(list1, name="1")
        group2 = TaskGroup(list2, name="2")

        combined_group = group1.union(group2)

        assert set(combined_group) == set(list1 + list2)

    def test_union_name(self, template):
        """Test that the new group has the requested name."""
        list1 = template.create_tasks(arg=[1, 2])
        list2 = template.create_tasks(arg=[3, 4])

        group1 = TaskGroup(list1, name="1")
        group2 = TaskGroup(list2, name="2")

        new_name = "new_name"
        combined_group = group1.union(group2, new_name)

        assert combined_group.name == new_name

    def test_or(self, template):
        """Test that `|` works like union."""
        list1 = template.create_tasks(arg=[1, 2])
        list2 = template.create_tasks(arg=[3, 4])

        group1 = TaskGroup(list1, "1")
        group2 = TaskGroup(list2, "2")

        combined_group = group1 | group2

        assert set(combined_group) == set(list1 + list2)

    @pytest.mark.parametrize(
        ["args", "expected_len"], [[[], 0], [[1], 1], [[1, 2, 3], 3]]
    )
    def test_len(self, args: List[int], expected_len: int, template):
        """Test that len works as expected."""
        tasks = template.create_tasks(arg=args)
        group = TaskGroup(tasks)

        assert len(group) == expected_len

    def test_uniqueness(self, template):
        """Assert that the same task can only get added once."""
        task_list = template.create_tasks(arg=[1, 2])
        group = TaskGroup(task_list * 2)

        assert len(group) == len(task_list)


class TestAddTasks:
    """Test adding tasks to the current workflow."""

    @pytest.fixture(scope="function")
    def template(self, tool):
        return tool.get_task_template(
            template_name="template",
            command_template="script {arg}",
            node_args=["arg"],
            task_args=[],
            op_args=[],
        )

    def test_add_task(self, template):
        """Test that it adds a task to a TaskGroup"""
        group = TaskGroup(template.create_tasks(arg=[1, 2]), "name")
        new_task = template.create_task(arg=3)

        group.add_task(new_task)

        assert new_task in group

    def test_add_uniqueness(self, template):
        """Test that if I add a task twice nothing happens."""
        task = template.create_task(arg=1)
        group = TaskGroup([task], "name")

        group.add_task(task)

        assert len(group) == 1

    def test_add_tasks(self, template):
        """Test that it adds multiple tasks to the TaskGroup"""
        group = TaskGroup(template.create_tasks(arg=[1, 2]), "name")
        new_tasks = template.create_tasks(arg=[3, 4])

        group.add_tasks(new_tasks)

        for task in new_tasks:
            assert task in group


class TestGetFunctions:
    """Tests for get_subgroup and get_task"""

    @pytest.fixture(scope="function")
    def template1(self, tool):
        return tool.get_task_template(
            template_name="template1",
            command_template="script {arg1} {arg2}",
            node_args=["arg1", "arg2"],
            task_args=[],
            op_args=[],
        )

    @pytest.fixture(scope="function")
    def template2(self, tool):
        return tool.get_task_template(
            template_name="template2",
            command_template="script {arg1} {arg2}",
            node_args=["arg1", "arg2"],
            task_args=[],
            op_args=[],
        )

    def test_subgroup_homogeneous(self, template1, template2):
        """Get a subset from a group with all one template."""
        in_group = TaskGroup(template1.create_tasks(arg1=[1], arg2=[1, 2]), "in_group")
        out_group = TaskGroup(
            template1.create_tasks(arg1=[2], arg2=[1, 2]), "out_group"
        )
        big_group = in_group | out_group

        assert in_group.tasks == big_group.get_subgroup(arg1=1).tasks

    def test_subgroup_with_template(self, template1, template2):
        """Get a subset using only one template in the group."""
        in_group = TaskGroup(template1.create_tasks(arg1=[1], arg2=[1, 2]), "in_group")
        out_group1 = TaskGroup(
            template1.create_tasks(arg1=[2], arg2=[1, 2]), "out_group1"
        )
        out_group2 = TaskGroup(
            template2.create_tasks(arg1=[1, 2], arg2=[1, 2]), "out_group2"
        )
        big_group = in_group | out_group1 | out_group2

        assert (
            in_group.tasks
            == big_group.get_subgroup(task_template_name="template1", arg1=1).tasks
        )

    def test_subgroup_multiple_templates(self, template1, template2):
        """Get a subgroup that has multiple templates which have a matching argument."""
        in_group1 = TaskGroup(
            template1.create_tasks(arg1=[1], arg2=[1, 2]), "in_group1"
        )
        out_group1 = TaskGroup(
            template1.create_tasks(arg1=[2], arg2=[1, 2]), "out_group2"
        )
        in_group2 = TaskGroup(
            template2.create_tasks(arg1=[1], arg2=[1, 2]), "in_group2"
        )
        out_group2 = TaskGroup(
            template2.create_tasks(arg1=[2], arg2=[1, 2]), "out_group2"
        )
        big_group = in_group1 | out_group1 | in_group2 | out_group2

        assert (in_group1 | in_group2).tasks == big_group.get_subgroup(arg1=1).tasks

    def test_subgroup_attributes(self, template1):
        """Get a subgroup based on attributes."""
        all_tasks = template1.create_tasks(arg1=[1, 2], arg2=[1, 2])
        for task in all_tasks[:2]:
            task.add_attribute("group", "in")
        for task in all_tasks[3:]:
            task.add_attribute("group", "out")

        group = TaskGroup(all_tasks, "group")
        assert set(all_tasks[:2]) == group.get_subgroup(group="in").tasks

    def test_subgroup_multiple_values(self, template1):
        """Get a subgroup with multiple values for a label."""
        in_tasks = template1.create_tasks(arg1=[1, 2], arg2=[1, 2])
        out_tasks = template1.create_tasks(arg1=[3], arg2=[1, 2])

        group = TaskGroup(in_tasks + out_tasks)

        assert set(in_tasks) == group.get_subgroup(arg1=[1, 2]).tasks

    def test_subgroup_args_and_attributes(self, template1, template2):
        """Get a subgroup with a mix of args and attributes specified."""
        tasks_1 = template1.create_tasks(
            arg1=[1],
            arg2=[1, 2, 3],
        )
        tasks_2 = template2.create_tasks(arg1=[2, 3], arg2=[1, 2, 3])
        for task in tasks_1[:2] + tasks_2[:2]:
            task.add_attribute("group", "in")
        for task in tasks_1[3:] + tasks_2[3:]:
            task.add_attribute("group", "out")

        group = TaskGroup(tasks_1 + tasks_2, "group")
        subgroup = group.get_subgroup(group="in", arg2=2)

        for task in subgroup:
            assert task.node.node_args["arg2"] == 2
            assert task.task_attributes["group"] == "in"

    def test_get_empty_subgroup(self, template1, template2):
        """Assert an error is thrown if attempting to get an empty subgroup.

        Not entirely sure this is the behavior we want, but it does seem broadly safer than
        returning an empty group.
        """
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")
        with pytest.raises(Exception):
            group.get_subgroup(arg1=3)

    def test_get_empty_subgroup_empty_okay(self, template1, template2):
        """Assert an error isn't thrown if attempting to get an empty subgroup and empty_okay
        is true.
        """
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")
        subgroup = group.get_subgroup(arg1=3, empty_okay=True)

        assert len(subgroup) == 0

    def test_get_subgroup_new_name(self, template1, template2):
        """Get subgroup with a new name."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")

        new_name = "new_name"
        sub_group = group.get_subgroup(arg1=1, new_name=new_name)

        assert sub_group.name == new_name

    def test_task_homogenous(self, template1, template2):
        """Get a task from a group with all one template."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")
        task = group.get_task(arg1=1, arg2=1)

        assert task.node.node_args["arg1"] == 1
        assert task.node.node_args["arg2"] == 1

    def test_task_with_template(self, template1, template2):
        """Get a task using a specified template."""
        group = TaskGroup(
            template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "part1"
        ) | TaskGroup(template2.create_tasks(arg1=[1, 2], arg2=[1, 2]), "part2")
        task = group.get_task(task_template_name="template1", arg1=1, arg2=2)

        assert (
            task.node.task_template_version.task_template.template_name == "template1"
        )
        assert task.node.node_args["arg1"] == 1
        assert task.node.node_args["arg2"] == 2

    def test_task_multiple_possible_templates(self, template1, template2):
        """Get a single task where multiple possible templates *could* match the arguments,
        but only one does.
        """
        group = TaskGroup(
            template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "part1"
        ) | TaskGroup(template2.create_tasks(arg1=[2], arg2=[1, 2]), "part2")
        task = group.get_task(arg1=1, arg2=2)

        assert (
            task.node.task_template_version.task_template.template_name == "template1"
        )
        assert task.node.node_args["arg1"] == 1
        assert task.node.node_args["arg2"] == 2

    def test_task_attributes(self, template1):
        """Get a task based on attributes."""
        all_tasks = template1.create_tasks(arg1=[1, 2], arg2=[1, 2])
        for task in all_tasks[:1]:
            task.add_attribute("group", "in")
        for task in all_tasks[2:]:
            task.add_attribute("group", "out")

        group = TaskGroup(all_tasks, "group")
        task = group.get_task(group="in")

        assert task.task_attributes["group"] == "in"

    def test_get_non_existent_task(self, template1):
        """Assert an error is thrown if attempting to get a non-existent task."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")
        with pytest.raises(Exception):
            group.get_task(arg1=3, arg2=1)

    def test_get_multiple_tasks(self, template1):
        """Assert an error is thrown if specification doesn't match only one task."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")
        with pytest.raises(Exception):
            group.get_task(arg1=1)

    def test_get_unused_label(self, template1):
        """Assert we don't get any tasks if we ask for a label that isn't on any of them."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]), "group")
        subgroup = group.get_subgroup(bad_arg="bad_val", empty_okay=True)
        assert len(subgroup) == 0

    def test_get_partially_used_label(self, template1, template2):
        """Assert we don't get tasks that don't have the label."""
        in_tasks = template1.create_tasks(arg1=[1, 2], arg2=[1, 2])
        out_tasks = template2.create_tasks(arg1=[1, 2], arg2=[1, 2])

        for task in in_tasks:
            task.add_attribute("group", "in")

        task_group = TaskGroup(in_tasks + out_tasks)

        in_group = task_group.get_subgroup(arg1=[1, 2], group="in")
        assert in_group.tasks == set(in_tasks)

    @pytest.mark.parametrize("subsetter", [{"arg1": 1}, {"arg1": [1, 2], "arg2": 1}])
    def test_add_label(self, subsetter, template1):
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]))
        group.add_labels({"new_label": "value"}, subsetter)

        expected = group.get_subgroup(**subsetter).tasks
        actual = group.get_subgroup(new_label="value").tasks

        assert expected == actual

    def test_add_label_all(self, template1):
        """Adds the label to all tasks."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]))
        group.add_labels({"new_label": "value"})

        for task in group:
            assert task.labels["new_label"] == "value"

    def test_add_label_bad_subsetter(self, template1):
        """Assert an error gets raised if the subsetter doesn't describe any tasks."""
        group = TaskGroup(template1.create_tasks(arg1=[1, 2], arg2=[1, 2]))
        with pytest.raises(Exception):
            group.add_labels({"new_label": "value"}, {"bad": "terrible"})


class TestDependencyHomogeneous:
    """The tests here revolve around setting dependencies between two groups of homogenous
    tasks, parallelized across two variables.
    """

    # Templates
    @pytest.fixture(scope="function")
    def template1(self, tool):
        return tool.get_task_template(
            template_name="template1",
            command_template="script1 {stage1_arg1} {stage1_arg2}",
            node_args=["stage1_arg1", "stage1_arg2"],
            task_args=[],
            op_args=[],
        )

    @pytest.fixture(scope="function")
    def template2(self, tool):
        return tool.get_task_template(
            template_name="template2",
            command_template="script2 {stage2_arg1} {stage2_arg2}",
            node_args=["stage2_arg1", "stage2_arg2"],
            task_args=[],
            op_args=[],
        )

    @pytest.fixture(scope="function")
    def template1_group(self, template1) -> TaskGroup:
        arg1 = ["val1", "val2"]
        arg2 = ["val3", "val4"]

        return TaskGroup(
            template1.create_tasks(stage1_arg1=arg1, stage1_arg2=arg2), "group1"
        )

    @pytest.fixture
    def template2_group(self, template2) -> TaskGroup:
        arg1 = ["val1", "val2"]
        arg2 = ["val3", "val4"]

        return TaskGroup(
            template2.create_tasks(stage2_arg1=arg1, stage2_arg2=arg2), "group2"
        )

    def test_all_to_all(self, template1_group: TaskGroup, template2_group: TaskGroup):
        """Test all-to-all dependency-relationship"""
        template2_group.interleave_upstream(
            template1_group, dependency_specification={}
        )

        for task1 in template1_group:
            for task2 in template2_group:
                assert task1 in task2.upstream_tasks

    def test_one_to_one_match(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Tasks depend on tasks that match them in both arg1 and arg2."""
        template2_group.interleave_upstream(
            template1_group,
            dependency_specification={
                "stage2_arg1": "stage1_arg1",
                "stage2_arg2": "stage1_arg2",
            },
        )

        arg1_vals = ["val1", "val2"]
        arg2_vals = ["val3", "val4"]
        for arg1_val, arg2_val in itertools.product(arg1_vals, arg2_vals):
            task1 = template1_group.get_task(
                task_template_name="template1",
                stage1_arg1=arg1_val,
                stage1_arg2=arg2_val,
            )
            task2 = template2_group.get_task(
                task_template_name="template2",
                stage2_arg1=arg1_val,
                stage2_arg2=arg2_val,
            )

            assert {task1} == task2.upstream_tasks

    def test_multiple_to_multiple(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Each task in group 1 has 2 downstreams, each task in group 2 has 2 upstreams."""
        template2_group.interleave_upstream(
            template1_group, dependency_specification={"stage2_arg1": "stage1_arg1"}
        )

        arg1_vals = ["val1", "val2"]
        for arg1_val in arg1_vals:
            for task2 in template2_group.get_subgroup(stage2_arg1=arg1_val):
                assert (
                    set(template1_group.get_subgroup(stage1_arg1=arg1_val))
                    == task2.upstream_tasks
                )

    def test_multiple_to_1(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Each task in group 1 has 2 downstreams, each task in group 2 has 1 upstream."""
        template1_subgroup = template1_group.get_subgroup(stage1_arg2="val3")

        template2_group.interleave_upstream(
            template1_subgroup, dependency_specification={"stage2_arg1": "stage1_arg1"}
        )
        arg1_vals = ["val1", "val2"]
        for arg1_val in arg1_vals:
            task1 = template1_subgroup.get_task(stage1_arg1=arg1_val)
            for task2 in template2_group.get_subgroup(stage2_arg1=arg1_val):
                assert {task1} == task2.upstream_tasks

    def test_1_to_multiple(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Each task in group 1 has 1 downstream, each task in group 2 has 2 uptreams."""
        template2_subgroup = template2_group.get_subgroup(stage2_arg2="val3")

        template2_subgroup.interleave_upstream(
            template1_group, dependency_specification={"stage2_arg1": "stage1_arg1"}
        )
        arg1_vals = ["val1", "val2"]
        for arg1_val in arg1_vals:
            task2 = template2_subgroup.get_task(stage2_arg1=arg1_val)
            assert (
                set(template1_group.get_subgroup(stage1_arg1=arg1_val))
                == task2.upstream_tasks
            )

    def test_upstream_subsetter(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Assert that we can set dependencies on only the upstream subset."""
        template2_group.interleave_upstream(
            template1_group,
            dependency_specification={"stage2_arg1": "stage1_arg1"},
            upstream_subsetter={"stage1_arg2": "val3"},
        )
        arg1_vals = ["val1", "val2"]
        for arg1_val in arg1_vals:
            task1 = template1_group.get_task(stage1_arg1=arg1_val, stage1_arg2="val3")
            for task2 in template2_group.get_subgroup(stage2_arg1=arg1_val):
                assert task2.upstream_tasks == {task1}

    def test_downstream_subsetter(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Assert that we can set dependencies on only the downstream subset."""
        template2_group.interleave_upstream(
            template1_group,
            dependency_specification={"stage2_arg1": "stage1_arg1"},
            subsetter={"stage2_arg2": "val3"},
        )
        arg1_vals = ["val1", "val2"]
        for arg1_val in arg1_vals:
            task2 = template2_group.get_task(stage2_arg1=arg1_val, stage2_arg2="val3")
            assert (
                set(template1_group.get_subgroup(stage1_arg1=arg1_val))
                == task2.upstream_tasks
            )

    def test_upstream_and_downstream_subsetter(
        self, template1_group: TaskGroup, template2_group: TaskGroup
    ):
        """Assert that we can set dependencies on only a downstream and an upstream subset."""
        template2_group.interleave_upstream(
            template1_group,
            dependency_specification={"stage2_arg1": "stage1_arg1"},
            subsetter={"stage2_arg2": "val3"},
            upstream_subsetter={"stage1_arg2": "val4"},
        )
        arg1_vals = ["val1", "val2"]
        for arg1_val in arg1_vals:
            task1 = template1_group.get_task(stage1_arg1=arg1_val, stage1_arg2="val4")
            task2 = template2_group.get_task(stage2_arg1=arg1_val, stage2_arg2="val3")
            assert task2.upstream_tasks == {task1}


class TestDependencyHeterogeneous:
    """These are test of setting dependency structures on groups containing multiple different
    task templates.
    """

    def test_downstream_with_aggregator(self, tool):
        """Here we're operating across one variable.

        The upstream group is homogenous.

        The downstream group has a parallelized step, dependent 1:1 on the upstream group, and
        a second aggregation step, that occurs after the parallelized step.
        """
        # Set up templates
        template1 = tool.get_task_template(
            template_name="template1",
            command_template="script1 {stage1_arg1}",
            node_args=["stage1_arg1"],
            task_args=[],
            op_args=[],
        )
        template2 = tool.get_task_template(
            template_name="template2",
            command_template="script2 {stage2_arg1}",
            node_args=["stage2_arg1"],
            task_args=[],
            op_args=[],
        )
        template_agg = tool.get_task_template(
            template_name="template_agg",
            command_template="script_agg {task_arg}",
            node_args=[],
            task_args=["task_arg"],
            op_args=[],
        )

        # Set up groups
        arg1_vals = ["val1", "val2"]

        group1 = TaskGroup(template1.create_tasks(stage1_arg1=arg1_vals), "group1")

        template2_tasks = template2.create_tasks(stage2_arg1=arg1_vals)
        agg_task = template_agg.create_task(task_arg="foo")
        agg_task.add_upstreams(template2_tasks)
        group2 = TaskGroup(template2_tasks + [agg_task], "group2")

        # Exercise
        group2.interleave_upstream(
            group1,
            subsetter={"task_template_name": "template2"},
            dependency_specification={"stage2_arg1": "stage1_arg1"},
        )

        # Verify group2 internals
        assert agg_task.upstream_tasks == set(template2_tasks)

        # Verify inter-group structure
        for arg1_val in arg1_vals:
            task1 = group1.get_task(stage1_arg1=arg1_val)
            task2 = group2.get_task(
                task_template_name="template2", stage2_arg1=arg1_val
            )
            assert task2.upstream_tasks == {task1}

    def test_both_groups_aggregate(self, tool):
        """Here we're operating across two variables.

        The upstream group has a first step parallelized over both variables. It has a second
        aggregation step parallelized over the first variable (combining results along the
        second variable.)

        The downstream group has a first step parallelized over one variable, dependent 1:1 on
        the aggregation step of the first group. It has a final aggregation step.
        """
        # Set up templates
        template1 = tool.get_task_template(
            template_name="template1",
            command_template="script1 {stage1_arg1} {stage1_arg2}",
            node_args=["stage1_arg1", "stage1_arg2"],
            task_args=[],
            op_args=[],
        )
        template1_agg = tool.get_task_template(
            template_name="template1_agg",
            command_template="script1_agg {stage1_agg_arg1}",
            node_args=["stage1_agg_arg1"],
            task_args=[],
            op_args=[],
        )
        template2 = tool.get_task_template(
            template_name="template2",
            command_template="script2 {stage2_arg1}",
            node_args=["stage2_arg1"],
            task_args=[],
            op_args=[],
        )
        template2_agg = tool.get_task_template(
            template_name="template2_agg",
            command_template="script2_agg {task_arg}",
            node_args=[],
            task_args=["task_arg"],
            op_args=[],
        )

        # Set up groups
        arg1_vals = ["val1", "val2"]
        arg2_vals = ["val3", "val4"]

        template1_tasks = TaskGroup(
            template1.create_tasks(stage1_arg1=arg1_vals, stage1_arg2=arg2_vals),
            "stage1_compute",
        )
        template1_agg_tasks = TaskGroup(
            template1_agg.create_tasks(stage1_agg_arg1=arg1_vals), "stage1_agg"
        )
        template1_agg_tasks.interleave_upstream(
            template1_tasks, {"stage1_agg_arg1": "stage1_arg1"}
        )
        group1 = template1_tasks | template1_agg_tasks

        template2_tasks = template2.create_tasks(stage2_arg1=arg1_vals)
        template2_agg_task = template2_agg.create_task(task_arg="foo")
        template2_agg_task.add_upstreams(template2_tasks)
        group2 = TaskGroup(template2_tasks + [template2_agg_task], "stage2")

        # Exercise
        group2.interleave_upstream(
            group1,
            subsetter={"task_template_name": "template2"},
            upstream_subsetter={"task_template_name": "template1_agg"},
            dependency_specification={"stage2_arg1": "stage1_agg_arg1"},
        )

        # Verify group 2 internal structure
        assert template2_agg_task.upstream_tasks == set(template2_tasks)

        for arg1_val in arg1_vals:
            task1_agg = group1.get_task(stage1_agg_arg1=arg1_val)
            # Verify group 1 internal structure
            assert task1_agg.upstream_tasks == set(
                template1_tasks.get_subgroup(stage1_arg1=arg1_val)
            )

            # Verify inter-group structure
            task2 = group2.get_task(
                task_template_name="template2", stage2_arg1=arg1_val
            )
            assert task2.upstream_tasks == {task1_agg}

    def test_hierarchy(self, tool):
        """Here we're testing an arbitrary hierarchy encoded using attributes.

        We're simulating the process of aggregating up a hierarchy, such that parents require
        all of their children to complete before they do (sorta the opposite of typical DAG
        notation).
        """
        # Set up templates
        child_template = tool.get_task_template(
            template_name="child_template",
            command_template="calculate_child {child_entity}",
            node_args=["child_entity"],
            task_args=[],
            op_args=[],
        )
        parent_template = tool.get_task_template(
            template_name="parent_template",
            command_template="calculate_parent {parent_entity}",
            node_args=["parent_entity"],
            task_args=[],
            op_args=[],
        )

        # Set up groups
        entity_hierarchy = {
            "parent1": ["child1", "child2"],
            "parent2": ["child3", "child4"],
        }

        child_tasks = []
        for parent, children in entity_hierarchy.items():
            for child in children:
                task = child_template.create_task(child_entity=child)
                task.add_attribute("parent_entity", parent)
                child_tasks.append(task)
        child_group = TaskGroup(child_tasks, "child")

        parent_group = TaskGroup(
            parent_template.create_tasks(parent_entity=entity_hierarchy.keys()),
            "parent",
        )

        # Exercise
        parent_group.interleave_upstream(
            child_group, {"parent_entity": "parent_entity"}
        )

        # Verify
        for parent, children in entity_hierarchy.items():
            parent_task = parent_group.get_task(parent_entity=parent)
            child_tasks = {
                child_group.get_task(child_entity=child) for child in children
            }

            assert parent_task.upstream_tasks == child_tasks


class TestDependencyIncomplete:
    """Tests for cases where a dependency spec might leave off some tasks."""

    @pytest.fixture(scope="function")
    def template1(self, tool):
        return tool.get_task_template(
            template_name="template1",
            command_template="script1 {stage1_arg}",
            node_args=["stage1_arg"],
            task_args=[],
            op_args=[],
        )

    @pytest.fixture(scope="function")
    def template2(self, tool):
        return tool.get_task_template(
            template_name="template2",
            command_template="stage2 {stage2_arg}",
            node_args=["stage2_arg"],
            task_args=[],
            op_args=[],
        )

    def test_extra_upstreams(self, template1, template2):
        """Test the 1:1 matching where the upstream has values not included in the downstreams.

        We expect a warning, and to set the right upstreams.
        """
        group1 = TaskGroup(
            template1.create_tasks(stage1_arg=["val1", "val2", "val3"]), "group1"
        )
        group2 = TaskGroup(
            template2.create_tasks(stage2_arg=["val1", "val2"]), "group2"
        )

        with pytest.warns(Warning):
            group2.interleave_upstream(
                group1, dependency_specification={"stage2_arg": "stage1_arg"}
            )

        for val in ["val1", "val2"]:
            task1 = group1.get_task(stage1_arg=val)
            task2 = group2.get_task(stage2_arg=val)
            assert task2.upstream_tasks == {task1}

    def test_extra_downstreams(self, template1, template2):
        """Test the 1:1 matching where the downstream has values not included in the upstreams.

        We expect a warning, and to set the right upstreams.
        """
        group1 = TaskGroup(
            template1.create_tasks(stage1_arg=["val1", "val2"]), "group1"
        )
        group2 = TaskGroup(
            template2.create_tasks(stage2_arg=["val1", "val2", "val3"]), "group2"
        )

        with pytest.warns(Warning):
            group2.interleave_upstream(
                group1, dependency_specification={"stage2_arg": "stage1_arg"}
            )

        for val in ["val1", "val2"]:
            task1 = group1.get_task(stage1_arg=val)
            task2 = group2.get_task(stage2_arg=val)
            assert task2.upstream_tasks == {task1}

        assert group2.get_task(stage2_arg="val3").upstream_tasks == set()

    def test_no_matches(self, template1, template2):
        """Test that an error is thrown when setting an upstream group would result in not
        actually setting any tasks' upstreams.
        """
        group1 = TaskGroup(
            template1.create_tasks(stage1_arg=["val1", "val2"]), "group1"
        )
        group2 = TaskGroup(
            template2.create_tasks(stage2_arg=["val3", "val4"]), "group2"
        )

        with pytest.raises(ValueError):
            group2.interleave_upstream(
                group1, dependency_specification={"stage2_arg": "stage1_arg"}
            )

    def test_no_warning(self, template1, template2):
        """Double check that warning doesn't get raised if it shouldn't."""
        group1 = TaskGroup(
            template1.create_tasks(stage1_arg=["val1", "val2"]), "group1"
        )
        group2 = TaskGroup(
            template2.create_tasks(stage2_arg=["val1", "val2"]), "group2"
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            group2.interleave_upstream(
                group1, dependency_specification={"stage2_arg": "stage1_arg"}
            )

    def test_unused_label(self, template1, template2):
        """Test that no upstreams get set for an unused label."""
        group1 = TaskGroup(
            template1.create_tasks(stage1_arg=["val1", "val2"]), "group1"
        )
        group2 = TaskGroup(
            template2.create_tasks(stage2_arg=["val1", "val2"]), "group2"
        )
        with pytest.raises(ValueError):
            group2.interleave_upstream(
                group1, dependency_specification={"stage2_arg": "bad_arg"}
            )

    def test_partially_used_label(self, template1, template2):
        """Test that upstreams don't include tasks without the label."""
        task_1_1 = template1.create_task(stage1_arg="val1")
        task_1_1.add_attribute("group", "in")
        task_1_2 = template1.create_task(stage1_arg="val2")
        group1 = TaskGroup([task_1_1, task_1_2])

        task_2_1 = template2.create_task(stage2_arg="val3")
        task_2_1.add_attribute("group", "in")
        task_2_2 = template2.create_task(stage2_arg="val4")
        group2 = TaskGroup([task_2_1, task_2_2])

        group2.interleave_upstream(group1, dependency_specification={"group": "group"})

        assert task_2_1.upstream_tasks == {task_1_1}
        assert task_2_2.upstream_tasks == set()
