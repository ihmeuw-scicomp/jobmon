"""SQLAlchemy database objects."""

from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules
from typing import Any

import structlog
from sqlalchemy import CheckConstraint, String, event, func
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta

logger = structlog.get_logger(__name__)


# declarative registry for model elements
class Base(DeclarativeBase):
    pass


@event.listens_for(Base, "instrument_class", propagate=True)
def add_string_length_constraint(Base: DeclarativeMeta, cls_: Any) -> None:
    """Add check constraint to enforce column size limits on SQLite."""
    table = cls_.__table__

    for column in table.columns:
        if isinstance(column.type, String):
            length = column.type.length

            if length is not None:
                constraint_name = f"ck_{table.name}_{column.name}_length"
                logger.debug(
                    f"adding check constraint {constraint_name} to "
                    f"{table.name}.{column.name} of len={length}"
                )
                check_constraint = CheckConstraint(
                    func.length(column) <= length, name=constraint_name
                )
                table.append_constraint(check_constraint)


def load_model() -> None:
    """Iterate through the modules in the current package."""
    package_dir = Path(__file__).resolve().parent
    for _, module_name, _ in iter_modules([str(package_dir)]):
        import_module(f"{__name__}.{module_name}")


def load_metadata(session_maker: sessionmaker[Session] | None = None) -> None:
    """Load metadata into a database.

    Args:
        session_maker: Optional sessionmaker. If not provided, creates one from config.
    """
    # load metadata
    from jobmon.server.web.models.arg_type import add_arg_types
    from jobmon.server.web.models.cluster import add_clusters
    from jobmon.server.web.models.cluster_type import add_cluster_types
    from jobmon.server.web.models.queue import add_queues
    from jobmon.server.web.models.task_instance_status import add_task_instance_statuses
    from jobmon.server.web.models.task_resources_type import add_task_resources_types
    from jobmon.server.web.models.task_status import add_task_statuses
    from jobmon.server.web.models.workflow_run_status import add_workflow_run_statuses
    from jobmon.server.web.models.workflow_status import add_workflow_statuses

    if session_maker is None:
        # Create sessionmaker from config for standalone usage (migrations, etc.)
        from jobmon.server.web.db.engine import create_engine_from_config

        engine, _, _ = create_engine_from_config()
        session_maker = sessionmaker(
            bind=engine, autoflush=False, expire_on_commit=False
        )

    with session_maker() as session:
        with session.begin():
            metadata_loaders = [
                add_arg_types,
                add_cluster_types,
                add_clusters,
                add_queues,
                add_task_resources_types,
                add_task_statuses,
                add_task_instance_statuses,
                add_workflow_statuses,
                add_workflow_run_statuses,
            ]
            for loader in metadata_loaders:
                loader(session)
                session.flush()
