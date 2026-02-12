# Backend Rules

## API Design

- All API endpoints must use Pydantic models for request/response validation
- Define models in route files or dedicated schema modules

## Database

- Use SQLAlchemy ORM/Core for all database operations
- No raw SQL strings - use SQLAlchemy constructs
- No stored procedures - all business logic in Python
- Run migrations after any schema changes
- Migrations location: `jobmon_server/src/jobmon/server/web/migrations/`

## State Transitions

- **Bulk SQL warning**: Direct `update()` statements bypass ORM hooks - use `TransitionService` for state changes
- **Status constants**: Use single-char codes from `jobmon_core.constants` (e.g., "Q", "R", "D")

## Code Organization

- Business logic belongs in `services/` modules
- Only ORM class definitions in `models/` - no business logic
- Data aggregation/processing happens in backend, not frontend
