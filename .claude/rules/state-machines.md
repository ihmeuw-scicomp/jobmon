# State Machine Rules

## Cascade Chain

When modifying state transitions, consider the full cascade:
```
TaskInstance → Task → Workflow
```

Changes to TaskInstance status can propagate up to Task and Workflow status.

## Code Organization

- FSM transition logic belongs in `services/` (e.g., `transition_service.py`)
- Only ORM class definitions in `models/` - no transition logic there
