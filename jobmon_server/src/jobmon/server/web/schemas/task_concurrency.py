"""Pydantic schemas for Task Concurrency Timeline endpoints."""

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class TaskConcurrencyResponse(BaseModel):
    """Response model for task concurrency timeline.

    Contains time-bucketed concurrent task counts grouped by status,
    with optional template breakdown for hover details.
    """

    buckets: List[str] = Field(description="ISO timestamp strings for each time bucket")
    series: Dict[str, List[int]] = Field(
        description="Mapping of status names to concurrent counts per bucket"
    )
    template_breakdown: Optional[Dict[str, List[Dict[str, int]]]] = Field(
        default=None,
        description="Per-status breakdown: status -> [{template: count}] per bucket",
    )


class TaskStatusAuditRecord(BaseModel):
    """A single task status audit record."""

    task_id: int
    previous_status: Optional[str] = None
    new_status: str
    entered_at: Optional[str] = None
    exited_at: Optional[str] = None


class TaskStatusAuditResponse(BaseModel):
    """Response model for task status audit records."""

    audit_records: List[TaskStatusAuditRecord]


class WorkflowTaskTemplatesResponse(BaseModel):
    """Response model for workflow task templates list."""

    task_templates: List[str] = Field(
        description="List of distinct task template names used in this workflow"
    )


class TemplateTimelineRow(BaseModel):
    """One row in the timeline — a single task template's event series.

    Each entry in ``timestamps`` marks an actual status transition;
    the corresponding index in each ``series`` list gives the number
    of tasks in that status immediately after the transition.
    """

    template_name: str
    total_tasks: int
    timestamps: List[str] = Field(
        description="ISO timestamps of status transition events"
    )
    series: Dict[str, List[int]] = Field(
        description="Status category -> task count at each timestamp"
    )


class TemplateTimelineResponse(BaseModel):
    """Response for template execution timeline.

    Each template carries its own ``timestamps`` array (the moments
    where any task in the template changed status) together with
    per-status counts, suitable for rendering as a continuous
    stacked area chart.
    """

    templates: List[TemplateTimelineRow] = Field(
        description="Per-template event series, sorted by first activity"
    )
