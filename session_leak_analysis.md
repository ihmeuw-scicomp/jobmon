# Session Leak Analysis Report
## jobmon_server/src/jobmon/server/web/routes/v2/fsm/

### Executive Summary
Analysis of the FSM routes directory reveals **one critical session leak** and several session management anti-patterns that could lead to resource leaks and connection pool exhaustion.

### Critical Issues Found

#### 1. **CRITICAL SESSION LEAK** - workflow.py:380-395
**Location**: `task_status_updates` function in `workflow.py`

```python
# Session context ends here
with SessionMaker() as session:
    with session.begin():
        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S") if db_time else None

# Session is used OUTSIDE of context manager - LEAK!
tasks_by_status_query = select(Task.status, Task.id).where(*filter_criteria)
result_dict = defaultdict(list)
for row in session.execute(tasks_by_status_query):  # ❌ SESSION LEAK
    result_dict[row.status].append(row.id)
```

**Impact**: Session remains open indefinitely, leading to connection pool exhaustion.

**Fix Required**: Move the query execution inside the session context manager.

**FIXED CODE**:
```python
# get time from db AND execute query in same session
with SessionMaker() as session:
    with session.begin():
        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S") if db_time else None
        
        # Execute query within the same session context
        tasks_by_status_query = select(Task.status, Task.id).where(*filter_criteria)
        result_dict = defaultdict(list)
        for row in session.execute(tasks_by_status_query):
            result_dict[row.status].append(row.id)
```

#### 2. **Anti-Pattern** - task_instance.py:230-250
**Location**: `log_error_worker_node` function

**CURRENT PROBLEMATIC CODE**:
```python
with SessionMaker() as session:
    # ... setup code ...
    session.commit()  # ❌ First commit
    
    # add error log
    error_state = data["error_state"]
    error_description = data["error_description"]
    try:
        session.execute(select(Task).where(Task.id == task_instance.task_id).with_for_update()).scalar_one()
        task_instance.transition(error_state)
        session.commit()  # ❌ Second commit
    except InvalidStateTransition as e:
        # ... error handling ...
    else:
        error = TaskInstanceErrorLog(task_instance_id=task_instance.id, description=error_description)
        session.add(error)
        session.commit()  # ❌ Third commit
```

**Impact**: Breaks transaction atomicity and context manager pattern.

**FIXED CODE**:
```python
with SessionMaker() as session:
    with session.begin():
        select_stmt = select(TaskInstance).where(TaskInstance.id == task_instance_id)
        task_instance = session.execute(select_stmt).scalars().one()

        # Set optional values
        optional_vals = ["distributor_id", "stdout_log", "stderr_log", "nodename", "stdout", "stderr"]
        for optional_val in optional_vals:
            val = data.get(optional_val, None)
            if val is not None:
                setattr(task_instance, optional_val, val)
        
        # Handle error state and logging in single transaction
        error_state = data["error_state"]
        error_description = data["error_description"]
        
        try:
            # Lock the task
            session.execute(
                select(Task).where(Task.id == task_instance.task_id).with_for_update()
            ).scalar_one()
            
            # Transition state
            task_instance.transition(error_state)
            
            # Add error log
            error = TaskInstanceErrorLog(
                task_instance_id=task_instance.id, 
                description=error_description
            )
            session.add(error)
            
            # Single commit happens automatically at end of context manager
            
        except InvalidStateTransition as e:
            if task_instance.status == error_state:
                logger.warning(e)
            else:
                logger.error(e)
            # Transaction will be rolled back automatically
            
    resp = JSONResponse(
        content={"status": task_instance.status}, 
        status_code=StatusCodes.OK
    )
    return resp
```

### Session Management Assessment by File

#### ✅ **GOOD** - Proper Session Management
- **task_resources.py**: Consistent use of context managers
- **tool.py**: Proper session scoping
- **tool_version.py**: Clean session handling
- **cluster.py**: Simple, correct pattern
- **queue.py**: Proper context manager usage
- **dag.py**: Correct session management
- **node.py**: Good separation of concerns with `_insert_node_args`
- **array.py**: Proper use of separate sessions for bulk operations

#### ⚠️ **CAUTION** - Complex but Acceptable
- **task_template.py**: Uses nested transactions properly with `session.begin_nested()`
- **workflow.py**: Uses nested transactions correctly (except for the critical leak above)
- **task.py**: Complex but follows patterns correctly
- **workflow_run.py**: Proper transaction handling

#### ❌ **ISSUES** - Requires Attention
- **workflow.py**: Critical session leak identified
- **task_instance.py**: Anti-pattern with multiple commits

### Best Practices Observed
1. Consistent use of `with SessionMaker() as session:` context manager
2. Proper use of `with session.begin():` for transaction management
3. Good separation of database operations into helper functions with their own sessions
4. Appropriate use of `session.flush()` vs `session.commit()`

### Recommendations

#### Immediate Actions Required
1. **Fix the critical session leak in workflow.py** (see fix above)
2. **Refactor the multiple commit pattern in task_instance.py** (see fix above)

#### Long-term Improvements
1. Add session leak detection in testing
2. Implement connection pool monitoring
3. Consider implementing a session decorator for consistent patterns
4. Add linting rules to catch session usage outside context managers

### Risk Assessment
- **High Risk**: The workflow.py session leak could cause production outages
- **Medium Risk**: The task_instance.py anti-pattern could cause transaction inconsistencies
- **Low Risk**: All other patterns are acceptable with current usage

### Monitoring Recommendations
1. Monitor database connection pool utilization
2. Set up alerts for connection pool exhaustion
3. Track session lifecycle metrics
4. Implement periodic connection pool health checks