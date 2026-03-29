"""
TaskGroup v4 API — Demo Use Cases

These are real workflow patterns derived from production workflows in the
jobmon database. They demonstrate how the TaskGroup API would be used to
build the same workflows with explicit pipeline structure.

These serve as the target API we're coding against for the v4 implementation.
"""

# =============================================================================
# USE CASE 1: dismod_at (wf 558363)
#
# 16,486 tasks, 5 templates
# Structure: linear pipeline with a tree cascade in the middle
#
#   fit_root (1 task)
#     -> continue_cascade (1094 tasks, internal parent->child tree)
#       -> predict (15389 tasks, fan-out by location)
#         -> diagnostics (1 task, barrier)
#           -> cleanup (1 task)
#
# Key patterns demonstrated:
#   - Intra-group task dependencies (cascade tree within one group)
#   - Label-based cross-group matching (location_id)
#   - Barrier pattern (one task depending on all tasks in a group)
#   - Linear chain
# =============================================================================

from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow
from jobmon.client.task_group import TaskGroup


def build_dismod_workflow(model_version_id: int, location_tree: dict):
    """Build a dismod_at workflow using TaskGroups.

    Args:
        model_version_id: Model version to run.
        location_tree: Dict mapping location_id -> parent_location_id.
            Root locations have parent_id = None.
    """
    tool = Tool("dismod_at")
    wf = Workflow("dismod_at", tool=tool)

    all_locations = list(location_tree.keys())
    root_locations = [
        loc for loc, parent in location_tree.items() if parent is None
    ]

    # ---- Groups ----

    fit_root = TaskGroup(
        name="fit_root",
        template=tool.get_template("fit_root"),
    )
    fit_root.create_tasks(model_version_id=model_version_id)

    cascade = TaskGroup(
        name="cascade",
        template=tool.get_template("continue_cascade"),
    )
    cascade.create_tasks(location_id=all_locations)

    predict = TaskGroup(
        name="predict",
        template=tool.get_template("predict"),
    )
    predict.create_tasks(
        location_id=all_locations,
        sex=[1, 2],
        measure=[5, 6],
    )

    diagnostics = TaskGroup(
        name="diagnostics",
        template=tool.get_template("diagnostics"),
    )
    diagnostics.create_tasks(model_version_id=model_version_id)

    cleanup = TaskGroup(
        name="cleanup",
        template=tool.get_template("cleanup"),
    )
    cleanup.create_tasks(model_version_id=model_version_id)

    # ---- Intra-group dependencies ----
    # The cascade is a location tree: child locations depend on parent.
    # These are task-level deps within a single group.
    for loc_id, parent_id in location_tree.items():
        if parent_id is not None:
            child_task = cascade.get_task(location_id=loc_id)
            parent_task = cascade.get_task(location_id=parent_id)
            child_task.add_upstream(parent_task)

    # ---- Cross-group dependencies ----

    # Root cascade locations depend on fit_root
    cascade.interleave_upstream(
        fit_root,
        subsetter={"location_id": root_locations},
    )

    # Each predict task depends on the cascade task for its location
    predict.interleave_upstream(
        cascade,
        dependency_specification={"location_id": "location_id"},
    )

    # Diagnostics waits for ALL predict tasks (barrier)
    diagnostics.add_upstream_group(predict)

    # Cleanup waits for diagnostics
    cleanup.add_upstream_group(diagnostics)

    # ---- Add to workflow and run ----
    wf.add_groups([fit_root, cascade, predict, diagnostics, cleanup])
    wf.run()

    # Result:
    #   5 TaskGroups, 4 TaskGroupEdges
    #   ~32K TaskDependency rows (materialized from group edges + cascade tree)
    #   Structural DAG query: instant (5 groups, 4 edges)
    #   No self-loops at group level (cascade tree is intra-group)


# =============================================================================
# USE CASE 2: fhs-pipeline-mortality (wf 557592)
#
# 5,140 tasks, 18 templates
# Structure: complex multi-stage pipeline where finalize_provenance_stage
#            runs at 7+ different pipeline stages
#
# Current problem: all finalize tasks are in one Array, creating 7 false
# bidirectional edges and 2 self-loops at the template level.
#
# With TaskGroups: finalize is split into separate groups per stage.
# The DAG is clean and acyclic.
#
# Key patterns demonstrated:
#   - Same template used in multiple groups (no false cycles)
#   - Organizational parent groups for pipeline stages
#   - Mix of label matching, fan-out, and barrier dependencies
#   - Intra-group tree deps (sum_to_all_cause location hierarchy)
# =============================================================================


def build_mortality_workflow(
    locations: list,
    sexes: list,
    location_tree: dict,
):
    """Build an fhs-pipeline-mortality workflow using TaskGroups.

    Args:
        locations: List of location_ids to process.
        sexes: List of sex_ids (e.g., [1, 2]).
        location_tree: Dict mapping location_id -> parent_location_id.
    """
    tool = Tool("fhs-pipeline-mortality")
    wf = Workflow("mortality", tool=tool)

    # Helper to get a template
    def tt(name):
        return tool.get_template(name)

    # ==== Stage 1: GK Modeling ====

    stage1 = TaskGroup("gk_modeling")  # organizational, no template

    gk_model = TaskGroup(
        "gk_model",
        template=tt("gk_model_stage"),
        parent=stage1,
    )
    gk_model.create_tasks(location_id=locations, sex=sexes)

    merge_gk = TaskGroup(
        "merge_gk_logs",
        template=tt("merge_gk_logs"),
        parent=stage1,
    )
    merge_gk.create_tasks(model_version_id=1)

    # finalize after GK — first use of finalize_provenance_stage template
    finalize_gk = TaskGroup(
        "finalize_after_gk",
        template=tt("finalize_provenance_stage"),
        parent=stage1,
    )
    finalize_gk.create_tasks(stage="gk")

    merge_gk.add_upstream_group(gk_model)
    finalize_gk.add_upstream_group(gk_model)
    finalize_gk.add_upstream_group(merge_gk)

    # ==== Stage 2: Cause Modeling ====

    stage2 = TaskGroup("cause_modeling")

    sum_to_ac = TaskGroup(
        "sum_to_all_cause",
        template=tt("sum_to_all_cause"),
        parent=stage2,
    )
    sum_to_ac.create_tasks(location_id=locations, sex=sexes)

    # Intra-group tree: child locations depend on parent (self-loop in
    # the old model, clean intra-group deps in TaskGroup model)
    for loc_id, parent_id in location_tree.items():
        if parent_id is not None:
            try:
                child = sum_to_ac.get_task(location_id=loc_id)
                parent = sum_to_ac.get_task(location_id=parent_id)
                child.add_upstream(parent)
            except ValueError:
                pass  # not all locations may be in this run

    arima = TaskGroup(
        "arima",
        template=tt("arima_stage"),
        parent=stage2,
    )
    arima.create_tasks(location_id=locations, sex=sexes)

    squeeze = TaskGroup(
        "squeeze",
        template=tt("squeeze_stage"),
        parent=stage2,
    )
    squeeze.create_tasks(location_id=locations)

    # Same squeeze intra-group tree pattern
    for loc_id, parent_id in location_tree.items():
        if parent_id is not None:
            try:
                child = squeeze.get_task(location_id=loc_id)
                parent = squeeze.get_task(location_id=parent_id)
                child.add_upstream(parent)
            except ValueError:
                pass

    # finalize after squeeze — second use of same template
    finalize_squeeze = TaskGroup(
        "finalize_after_squeeze",
        template=tt("finalize_provenance_stage"),
        parent=stage2,
    )
    finalize_squeeze.create_tasks(stage="squeeze")

    # Cross-group deps within stage 2
    sum_to_ac.interleave_upstream(
        finalize_gk,
        dependency_specification={"location_id": "location_id"},
    )
    arima.interleave_upstream(
        sum_to_ac,
        dependency_specification={"location_id": "location_id"},
    )
    squeeze.interleave_upstream(
        arima,
        dependency_specification={"location_id": "location_id"},
    )
    finalize_squeeze.add_upstream_group(squeeze)

    # ==== Stage 3: Aggregation ====

    stage3 = TaskGroup("aggregation")

    add_ext = TaskGroup(
        "add_external_causes",
        template=tt("add_external_causes_stage"),
        parent=stage3,
    )
    add_ext.create_tasks(model_version_id=1)
    add_ext.add_upstream_group(finalize_squeeze)

    agg_slices = TaskGroup(
        "aggregate_slices",
        template=tt("run_aggregate_slice_main"),
        parent=stage3,
    )
    agg_slices.create_tasks(location_id=locations)
    agg_slices.interleave_upstream(
        finalize_squeeze,
        dependency_specification={"location_id": "location_id"},
    )
    agg_slices.add_upstream_group(add_ext)

    merge_slices = TaskGroup(
        "merge_slices",
        template=tt("run_merge_slices_main"),
        parent=stage3,
    )
    merge_slices.create_tasks(location_id=locations, sex=sexes)
    merge_slices.interleave_upstream(
        agg_slices,
        dependency_specification={"location_id": "location_id"},
    )

    delete_intermediate = TaskGroup(
        "delete_intermediate",
        template=tt("run_delete_intermediate_main"),
        parent=stage3,
    )
    delete_intermediate.create_tasks(location_id=[1, 2])
    delete_intermediate.add_upstream_group(merge_slices)

    # finalize after aggregation — third use of same template
    finalize_agg = TaskGroup(
        "finalize_after_agg",
        template=tt("finalize_provenance_stage"),
        parent=stage3,
    )
    finalize_agg.create_tasks(stage="agg")
    finalize_agg.add_upstream_group(agg_slices)
    finalize_agg.add_upstream_group(merge_slices)
    finalize_agg.add_upstream_group(delete_intermediate)

    # ==== Stage 4: Post-processing ====

    stage4 = TaskGroup("post_processing")

    life_tables = TaskGroup(
        "life_tables",
        template=tt("life_tables_main"),
        parent=stage4,
    )
    life_tables.create_tasks(location_id=[1, 2])
    life_tables.add_upstream_group(finalize_agg)

    compute_pop = TaskGroup(
        "compute_population",
        template=tt("compute_population"),
        parent=stage4,
    )
    compute_pop.create_tasks(location_id=locations, sex=sexes)
    compute_pop.add_upstream_group(life_tables)

    collect_pop = TaskGroup(
        "collect_population",
        template=tt("collect_population_files"),
        parent=stage4,
    )
    collect_pop.create_tasks(model_version_id=1)
    collect_pop.add_upstream_group(compute_pop)

    agg_pop = TaskGroup(
        "aggregate_population",
        template=tt("aggregate_population"),
        parent=stage4,
    )
    agg_pop.create_tasks(model_version_id=1)
    agg_pop.add_upstream_group(collect_pop)

    remove_pop = TaskGroup(
        "remove_intermediate_pop",
        template=tt("remove_intermediate_population_files"),
        parent=stage4,
    )
    remove_pop.create_tasks(model_version_id=1)
    remove_pop.add_upstream_group(collect_pop)

    # ==== Stage 5: Summaries ====

    stage5 = TaskGroup("summaries")

    summary_one = TaskGroup(
        "summary_one_entity",
        template=tt("summary_maker_one_entity"),
        parent=stage5,
    )
    summary_one.create_tasks(location_id=locations, sex=sexes)

    # Summary depends on multiple upstream stages by location
    for upstream in [gk_model, arima, agg_slices, merge_slices]:
        summary_one.interleave_upstream(
            upstream,
            dependency_specification={"location_id": "location_id"},
        )

    summary_merge = TaskGroup(
        "summary_merge_entities",
        template=tt("summary_maker_merge_entities"),
        parent=stage5,
    )
    summary_merge.create_tasks(location_id=locations)
    summary_merge.add_upstream_group(summary_one)

    summary_delete = TaskGroup(
        "summary_delete_intermediate",
        template=tt("summary_maker_delete_intermediate_files"),
        parent=stage5,
    )
    summary_delete.create_tasks(location_id=locations)
    summary_delete.add_upstream_group(summary_merge)

    # ==== Add all top-level stages to workflow ====
    wf.add_groups([stage1, stage2, stage3, stage4, stage5])
    wf.run()

    # Result:
    #   5 organizational groups + ~17 leaf groups = 22 TaskGroups
    #   ~20 TaskGroupEdges (clean, acyclic)
    #   finalize_provenance_stage used in 3 separate groups (no cycles)
    #   sum_to_all_cause and squeeze self-loops → intra-group task deps
    #   Structural DAG: instant query on 22 groups + 20 edges
    #   No JSON blobs, no phase-splitting algorithm needed
