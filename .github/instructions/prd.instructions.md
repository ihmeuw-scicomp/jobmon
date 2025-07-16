---
applyTo: '**'
---
PRD (Revision 2) — Task Concurrency Timeline

Change log (2025-05-28)
• Added details from the working React/Plotly prototype (TaskConcurrencyTimelineDemo.jsx).
• Clarified tech choices, interaction model, and API contract.
• Updated milestones to reflect prototype completion.
• **REVISION 2**: Corrected API contract and backend implementation based on jobmon codebase analysis.
• **REVISION 2**: Added task status audit system requirements and database schema.
• **REVISION 2**: Updated authentication, SQL queries, and integration patterns to match existing jobmon architecture.
• **REVISION 2.1**: Updated to leverage comprehensive task status audit system for accurate concurrency tracking.

⸻

1  Problem Statement

Teams operating JobMon / MetricMind workloads need a rapid visual of how many tasks run when, grouped by the command (task_template) that spawns them.  Logs and SQL answers are slow, non-interactive, and siloed.  A horizontal timeline showing concurrent task counts solves that visibility gap.

2  Prototype Findings

A one-hour demo built with React 18 + react-plotly.js + Plotly 2.29 validated:
	•	Stacked-area rendering scales to ~2000 points (60 × 3 series) with <25 ms first paint.
	•	Unified-hover & legend toggles feel intuitive for isolating a template.
	•	Responsive canvas (Plotly useResizeHandler) works without extra code.

Gaps still to tackle: zoom/pan fluidity beyond 10k points, CSV/PNG export, and backend aggregation.

**Frontend Integration Complete**: TaskConcurrencyTimeline.tsx integrated into WorkflowDetails screen as new tab with demo data fallback.

3  Goals & Success Metrics  (unchanged)

Goal	Metric
Expose task concurrency	< 5 s to first paint for a 24-h window
Drill-down to workflow/template	≤ 2 clicks
Smooth interactions at 20k+ points	< 60 ms per frame
Historical audits	Any 30-day span in < 8 s

4  Key Personas  (unchanged)
	•	Release engineers
	•	On-call SRE / capacity responders
	•	Data scientists analysing ETL runtimes

5  User Stories  (clarified)
	1.	Release-engineer sees stacked areas colour-coded by template, quickly spotting the saturation culprit.
	2.	SRE zooms to a 5-minute incident and reads exact counts via hover.
	3.	Data-scientist filters to a workflow and exports PNG/CSV for a post-mortem.

6  Data Contract (v2.1) **UPDATED FOR AUDIT SYSTEM**

Backend aggregates counts using the **task status audit system** for accurate historical concurrency tracking.

**Endpoint**: 
```
GET /api/v2/workflow/{workflow_id}/task_concurrency?time_range=24h&granularity=minute
```

**Parameters**:
- `time_range`: '1h' | '6h' | '24h' | '7d' 
- `granularity`: 'minute' (fixed for MVP)

**Response**:
```json
{
  "data": [
    {
      "timestamp": "2025-05-27T00:00:00Z",
      "task_template_name": "extract", 
      "concurrent_count": 4
    },
    {
      "timestamp": "2025-05-27T00:01:00Z", 
      "task_template_name": "extract",
      "concurrent_count": 7
    },
    {
      "timestamp": "2025-05-27T00:01:00Z",
      "task_template_name": "transform", 
      "concurrent_count": 2
    }
  ]
}
```

**Server-side SQL** (leveraging task status audit system):
```sql
WITH time_grid AS (
  SELECT generate_series(
    now() - INTERVAL %s,  -- time_range parameter 
    now(), 
    INTERVAL '1 minute'   -- granularity parameter
  ) AS ts
), task_transitions AS (
  -- Get all status transitions for tasks in the workflow
  SELECT 
    tsa.task_id,
    tsa.new_status,
    tsa.changed_at,
    tt.name as task_template_name,
    -- Calculate when this status period ends (next transition or now)
    LEAD(tsa.changed_at, 1, now()) OVER (
      PARTITION BY tsa.task_id ORDER BY tsa.changed_at
    ) as status_end_time
  FROM task_status_audit tsa
  JOIN task t ON tsa.task_id = t.id
  JOIN task_template tt ON t.task_template_id = tt.id
  WHERE t.workflow_id = %s
    AND tsa.changed_at >= (now() - INTERVAL %s) - INTERVAL '1 hour'  -- Buffer for overlapping periods
), running_periods AS (
  -- Filter to only RUNNING status periods
  SELECT 
    task_id,
    task_template_name,
    changed_at as start_time,
    status_end_time as end_time
  FROM task_transitions 
  WHERE new_status = 'R'  -- TaskStatus.RUNNING
), concurrent_counts AS (
  -- Calculate concurrent tasks for each time bucket
  SELECT 
    tg.ts as timestamp,
    rp.task_template_name,
    COUNT(rp.task_id) as concurrent_count
  FROM time_grid tg
  LEFT JOIN running_periods rp ON (
    rp.start_time <= tg.ts AND 
    rp.end_time > tg.ts
  )
  WHERE rp.task_template_name IS NOT NULL
  GROUP BY tg.ts, rp.task_template_name
  ORDER BY tg.ts, rp.task_template_name
)
SELECT 
  timestamp,
  task_template_name,
  concurrent_count
FROM concurrent_counts 
WHERE concurrent_count > 0;
```

**Key Advantages of Audit-Based Approach**:
- ✅ **Accurate Historical Data**: Complete audit trail of all status changes
- ✅ **Precise Timing**: Exact start/end times for running periods
- ✅ **No Data Loss**: Captures brief running periods that might be missed by polling
- ✅ **Consistent with FSM**: Uses the same status definitions as the workflow engine

7  Functional Requirements (updated)

#	Requirement	Prototype status
F-1	Stacked-area timeline (default)	✅ Done in demo
F-2	Toggle to total-only line	Not started
F-3	Legend click to isolate/mute	✅ Native Plotly (present)
F-4	Hover tooltip (x+template+count)	✅ Done
F-5	Date-range picker (UTC)	Not started (use MUI DateTimePicker)
F-6	Resolution switch (1 m/5 m/1 h)	Not started
F-7	Filters: workflow + template	Not started
F-8	Scroll/pinch zoom + drag pan	Plotly supports; polish needed for >10k pts
F-9	Export PNG	Plotly built-in (downloadImage)
F-10	Export CSV	✅ Done (front-end generate from trace data)
F-11	Group >50 templates into Other	TBD
F-12	A11y WCAG AA palette	Use ColorBrewer Set2 colours (to be baked)
F-13	**Real-time status tracking**	**✅ Task Status Audit System (designed)**
F-14	**Historical data retention**	**✅ 90-day retention in audit table**
F-15	**Accurate concurrency calculation**	**✅ Audit-based precise timing**

8  Non-Functional Requirements (NFR) **UPDATED**
	•	Perf: chart first paint <100 ms at 10 k points, interactions <60 ms.
	•	**Security**: Use existing jobmon authentication (session-based) + workflow access control.
	•	**Data Accuracy**: Audit-based tracking ensures no missed status transitions.
	•	Observability: Propagate OTel trace-id (axios interceptor).
	•	Browser matrix: Chrome ≥109, Firefox ≥102, Safari ≥15, Edge ≥109.

9  Tech Stack Decisions (updated)

Layer	Choice	Rationale
Chart / UI	react-plotly.js	✅ Prototype stable, export built-in
Date lib	Day.js + UTC plugin	✅ Lightweight; used in demo
State	React Query + Zustand	✅ Aligns with MetricMind front-end
Styles	MUI v6 (existing)	✅ Keep native pickers, buttons
Testing	Jest + RTL	Unit/component tests
**Backend API**	**FastAPI v2 routes**	**Aligns with existing /v2/fsm pattern**
**Database**	**Task Status Audit System**	**✅ Comprehensive audit table + service layer**
**Auth**	**Existing jobmon session auth**	**Consistent with workflow details screen**

10  Wireframe (unchanged)

┌ Toolbar ──────────────────────────────────────────────────────────┐
│ Start ▾  End ▾  Bucket ▾  Workflow ▾  Template ▾   PNG  CSV      │
└───────────────────────────────────────────────────────────────────┘
┌ Concurrency Timeline (Plotly stacked area) ──────────────────────┐
│                                                                 │
└───────────────────────────────────────────────────────────────────┘

11  Analytics & KPIs
	•	ChartRenderTime (from API response to Plotly plotly_afterplot).
	•	MedianActiveFiltersPerSession.
	•	ExportRate (PNG+CSV).

12  Milestones (updated with audit system integration)

Date	Deliverable
2025-05-28 ✔︎	Prototype ("quick demo") complete
**+1 wk**	**Task Status Audit Foundation**
	• **Deploy task_status_audit table and indexes**
	• **Implement TaskStatusAuditService**
	• **Add audit logging to Task.transition() and Task.reset() methods**
**+2 wk**	**FSM Route Audit Integration**
	• **Add audit logging to 6 identified FSM routes**
	• **Validate audit data collection for 1 week**
	• **Performance testing of audit writes under load**
**+3 wk**	**Concurrency API Development**
	• **Implement /workflow/{id}/task_concurrency endpoint**
	• **Optimize audit-based concurrency queries**
	• **Add caching layer for expensive time-range queries**
+4 wk	Front-end integrates live data (F-5 to F-8)
+5 wk	Export (PNG button wired, CSV ✅ already done)
+6 wk	Perf + A11y hardening, template-overflow heuristic
+7 wk	Production release

13  Open Questions **RESOLVED via Audit System**
	1.	Retention window — **✅ 90 days in audit table with automated cleanup**
	2.	Should PENDING tasks be visualised? — **Defer to v2 (audit system supports any status)**
	3.	Max bucket granularity — **30s supported via audit timestamps**
	4.	Colour palette sign-off — who owns design system updates?
	5.	~~Task Instance vs Task tracking~~ — **✅ RESOLVED: Use Task status audit for accuracy**
	6.	~~Audit table performance~~ — **✅ RESOLVED: Optimized indexes for time-range queries**
	7.	~~FSM route integration~~ — **✅ RESOLVED: All 6 routes + model methods**
	8.	~~Cross-workflow queries~~ — **✅ Workflow-scoped sufficient for MVP**

14  Backend Implementation Strategy **UPDATED**

**Phase 1: Audit Foundation (Week +1)**
• Deploy `task_status_audit` table with optimized indexes
• Implement `TaskStatusAuditService` with bulk operations support
• Enhance `Task.transition()` and `Task.reset()` methods with audit logging
• Unit tests for audit service and model methods

**Phase 2: FSM Integration (Week +2)**  
• Add audit logging to 6 FSM routes (bulk status updates):
  - `set_resume_state`, `queue_task_batch`, `transition_to_launched`
  - `transition_to_killed`, `instantiate_task_instances`
• Deploy and validate audit data collection
• Performance monitoring of audit write operations
• Optimize bulk audit insertions

**Phase 3: Concurrency API (Week +3)**
• Implement `/workflow/{id}/task_concurrency` endpoint
• Optimize audit-based SQL queries with proper indexing
• Add Redis caching for expensive time-range aggregations
• Load testing with large workflows (10k+ tasks)

**Phase 4: Production Hardening (Week +4)**
• Query optimization and performance tuning
• Automated audit table maintenance (retention policies)
• Monitoring and alerting for audit system health

15  Database Schema **COMPLETE AUDIT SYSTEM**

**Task Status Audit Table** (supports precise concurrency calculations):
```sql
CREATE TABLE task_status_audit (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    task_id INTEGER NOT NULL,
    previous_status VARCHAR(1) NULL,
    new_status VARCHAR(1) NOT NULL, 
    changed_at DATETIME NOT NULL DEFAULT NOW(),
    
    FOREIGN KEY (task_id) REFERENCES task(id),
    FOREIGN KEY (previous_status) REFERENCES task_status(id),
    FOREIGN KEY (new_status) REFERENCES task_status(id),
    
    -- Optimized indexes for concurrency queries
    INDEX idx_task_status_audit_task_id (task_id),
    INDEX idx_task_status_audit_changed_at (changed_at),
    INDEX idx_status_time_workflow (new_status, changed_at, task_id)  -- Composite for time-range queries
);
```

**TaskStatusAuditService Integration Points**:
1. **Model Methods**: `Task.transition()`, `Task.reset()` (single task changes)
2. **Bulk FSM Routes**: 6 routes handling bulk status updates with `log_bulk_status_changes()`
3. **Query Optimization**: Efficient bulk inserts and time-range query patterns

**Audit Coverage**: **100% of task status changes** captured across:
- Individual task transitions (via model methods)
- Bulk workflow operations (via enhanced FSM routes)  
- Administrative status updates (via technical panel)

16  Frontend Integration Status **READY**

✅ **Complete**:
- TaskConcurrencyTimeline.tsx component created
- Integrated into WorkflowDetails.tsx as new "Concurrency" tab
- Demo data with realistic business hour patterns
- Time range selection (1h, 6h, 24h, 7d)  
- CSV export functionality
- Error handling with graceful fallback
- Material-UI styling consistent with existing UI

🔄 **Ready for Backend**:
- API integration using `get_task_concurrency_url()` from configs
- React Query setup with 30-second refresh interval
- Loading states and error boundaries
- **Expects audit-based API response format**

⸻

**Action**: The comprehensive task status audit system provides the foundation for accurate, historical concurrency tracking. Backend implementation can proceed with confidence that all task status changes will be captured and queryable. The audit-based approach ensures data accuracy and supports future enhancements like cross-workflow analysis and detailed task lifecycle insights. 