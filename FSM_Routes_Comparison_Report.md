# FSM Routes Functional Differences Report: v2 vs v3

## Executive Summary

This report analyzes the functional differences between the v2 and v3 implementations of the Finite State Machine (FSM) routes in the jobmon server. The analysis covers route endpoints, authentication, error handling, and feature additions/removals.

## Major Functional Differences

### 1. Authentication and Authorization

**v3 Enhancements:**
- **User Authentication**: v3 introduces user authentication using `get_request_username()` utility
- **Authorization Checks**: Multiple endpoints now verify user ownership before allowing operations
- **HTTP 401 Responses**: Proper unauthorized access handling with HTTPException

**Affected Endpoints:**
- `POST /workflow/{workflow_id}/set_resume` - Now checks if user owns the workflow
- `PUT /workflow/{workflow_id}/update_max_concurrently_running` - User authorization required
- `PUT /workflow/{workflow_id}/update_array_max_concurrently_running` - User authorization required

### 2. New Features in v3

#### Task Template DAG Visualization
- **New Endpoint**: `GET /workflow/{workflow_id}/task_template_dag`
- **Functionality**: Computes and returns the shape of a workflow's DAG organized by TaskTemplate
- **Uses**: Pandas DataFrame processing for data transformation
- **Output**: JSON representation of the task template DAG structure

#### Array-Level Concurrency Management
- **New Endpoint**: `PUT /workflow/{workflow_id}/update_array_max_concurrently_running`
- **Functionality**: Updates maximum concurrently running tasks for specific arrays within a workflow
- **Authorization**: Requires user ownership verification

### 3. Removed Features from v3

#### Task Killing Functionality
**v2 Only Features:**
- `POST /array/{array_id}/transition_to_killed` - Transitions task instances from KILL_SELF to ERROR_FATAL
- `_update_task_instance_killed()` helper function
- Complete task killing workflow management

**Impact**: v3 removes the ability to kill running tasks through the API

### 4. Error Handling Improvements

**v3 Enhancements:**
- **SQLAlchemy Exception Handling**: Import and use of `IntegrityError` and `NoResultFound`
- **HTTP 404 Responses**: Proper not found handling in `workflow_is_resumable` endpoint
- **HTTP 404 for Missing Workflows**: Enhanced error responses in `get_max_concurrently_running`

**v2 vs v3 Exception Handling:**
- v2: Uses generic exception handling with `sqlalchemy.exc.IntegrityError`
- v3: Specific imports and handling for `IntegrityError`, `NoResultFound` with proper HTTP status codes

### 5. Session Management Differences

**Database Session Handling:**
- **v2**: Combined database time and query execution in same session for `task_status_updates`
- **v3**: Separates database time retrieval and query execution into different sessions

### 6. Import Differences

**Additional v3 Imports:**
- `pandas as pd` - Used for task template DAG processing
- `HTTPException` from fastapi - For proper HTTP error responses
- `IntegrityError, NoResultFound` from sqlalchemy.exc - Enhanced exception handling
- Additional model imports: `Edge`, `Node`, `TaskTemplate`, `TaskTemplateVersion`, `WorkflowRun`
- `get_request_username` utility - For authentication

### 7. Router and Naming Conventions

**Router References:**
- v2: Uses `api_v2_router` 
- v3: Uses `api_v3_router`

**File Structure:**
Both versions maintain identical file structure and module organization.

## Detailed Endpoint Analysis

### Workflow Endpoints

| Endpoint | v2 Status | v3 Status | Changes |
|----------|-----------|-----------|---------|
| `POST /workflow` | ✅ | ✅ | Identical functionality |
| `GET /workflow/{workflow_args_hash}` | ✅ | ✅ | Identical functionality |
| `PUT /workflow/{workflow_id}/workflow_attributes` | ✅ | ✅ | Identical functionality |
| `POST /workflow/{workflow_id}/set_resume` | ✅ | ✅ | **v3 adds user authentication** |
| `GET /workflow/{workflow_id}/is_resumable` | ✅ | ✅ | **v3 adds proper 404 handling** |
| `GET /workflow/{workflow_id}/get_max_concurrently_running` | ✅ | ✅ | **v3 adds 404 error handling** |
| `PUT /workflow/{workflow_id}/update_max_concurrently_running` | ✅ | ✅ | **v3 adds user authentication** |
| `POST /workflow/{workflow_id}/task_status_updates` | ✅ | ✅ | **v3 changes session handling** |
| `GET /workflow/{workflow_id}/fetch_workflow_metadata` | ✅ | ✅ | Identical functionality |
| `GET /workflow/get_tasks/{workflow_id}` | ✅ | ✅ | Identical functionality |
| `GET /workflow_status/available_status` | ✅ | ✅ | Identical functionality |
| `PUT /workflow/{workflow_id}/update_array_max_concurrently_running` | ❌ | ✅ | **New in v3** |
| `GET /workflow/{workflow_id}/task_template_dag` | ❌ | ✅ | **New in v3** |

### Array Endpoints

| Endpoint | v2 Status | v3 Status | Changes |
|----------|-----------|-----------|---------|
| `POST /array` | ✅ | ✅ | Identical functionality |
| `POST /array/{array_id}/queue_task_batch` | ✅ | ✅ | Identical functionality |
| `POST /array/{array_id}/transition_to_launched` | ✅ | ✅ | Identical functionality |
| `POST /array/{array_id}/log_distributor_id` | ✅ | ✅ | Identical functionality |
| `GET /array/{array_id}/get_array_max_concurrently_running` | ✅ | ✅ | Identical functionality |
| `POST /array/{array_id}/transition_to_killed` | ✅ | ❌ | **Removed in v3** |

## Security Impact Analysis

### Positive Security Changes in v3:
1. **Authentication**: User identity verification for destructive operations
2. **Authorization**: Ownership checks before allowing workflow modifications
3. **Proper HTTP Status Codes**: More accurate error responses

### Potential Security Concerns:
1. **Removed Kill Functionality**: v3 removes the ability to kill running tasks, which might be needed for system administration

## Backward Compatibility

**Breaking Changes:**
1. **Authentication Requirements**: v3 requires user authentication for several endpoints that were open in v2
2. **Removed Endpoints**: `transition_to_killed` functionality completely removed
3. **Session Handling**: Different database session management patterns

**Compatible Changes:**
1. **New Endpoints**: v3 adds new endpoints without affecting existing ones
2. **Error Handling**: Enhanced error responses are generally backward compatible

## Recommendations

1. **For Migration to v3**:
   - Implement proper authentication mechanisms in client applications
   - Update error handling to accommodate new HTTP status codes
   - Remove dependencies on task killing functionality
   - Update to handle new authentication-required endpoints

2. **For API Consumers**:
   - Implement user authentication before calling protected endpoints
   - Handle new 401 Unauthorized responses appropriately
   - Consider using new task template DAG endpoint for visualization features

3. **For System Administrators**:
   - Plan alternative task management strategies since kill functionality is removed
   - Ensure authentication systems are properly configured
   - Test all existing integrations with enhanced error handling

## Conclusion

Version 3 represents a significant security and functionality enhancement over version 2, with the introduction of proper authentication, authorization, and improved error handling. However, it also removes some administrative functionality (task killing) and introduces breaking changes that require careful migration planning.

The addition of task template DAG visualization and array-level concurrency management provides enhanced workflow management capabilities, while the security improvements align with modern API design best practices.