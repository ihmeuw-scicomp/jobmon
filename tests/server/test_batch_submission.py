from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from jobmon.core.constants import TaskStatus, TaskInstanceStatus
from jobmon.server.web.models import load_model

load_model()


def test_array_launch_transition(web_server_in_memory):
    from jobmon.server.web.models.task import Task
    from jobmon.server.web.models.task_instance import TaskInstance

    # Make up some tasks and task instances in I state
    app, db_engine = web_server_in_memory
    t = Task(
        array_id=1,
        task_args_hash=123,
        command="echo 1",
        status=TaskStatus.INSTANTIATING,
    )

    # Add the task
    with Session(bind=db_engine) as session:
        session.add(t)
        session.commit()
        tid = t.id

    ti_params = {
        "task_id": tid,
        "status": TaskInstanceStatus.INSTANTIATED,
        "array_id": 1,
        "array_batch_num": 1,
        "array_step_id": 0,
    }

    ti1 = TaskInstance(**ti_params)
    ti2 = TaskInstance(**dict(ti_params, array_step_id=1))
    ti3 = TaskInstance(**dict(ti_params, array_step_id=2))

    # add tis to db
    with Session(bind=db_engine) as session:
        session.add_all([ti1, ti2, ti3])
        session.commit()
        ti1_id = ti1.id
        ti2_id = ti2.id
        ti3_id = ti3.id

    # Post the transition route, check what comes back
    resp = app.post(
        "/array/1/transition_to_launched",
        json={
            "batch_number": 1,
            "next_report_increment": 5 * 60,  # 5 minutes to report
        },
    )
    assert resp.status_code == 200

    # Check the statuses are updated
    with Session(bind=db_engine) as session:
        tnew = session.query(Task).where(Task.id == t.id).one()
        session.commit()
        ti1_r, ti2_r, ti3_r = (
            session.query(TaskInstance)
            .where(TaskInstance.id.in_([ti1_id, ti2_id, ti3_id]))
            .all()
        )

        assert tnew.status == TaskStatus.LAUNCHED
        assert [ti1_r.status, ti2_r.status, ti3_r.status] == [
            TaskInstanceStatus.LAUNCHED
        ] * 3

        # Check a single datetime
        submitted_date = ti1_r.submitted_date
        next_update_date = ti1_r.report_by_date
        assert next_update_date > datetime.utcnow()
        assert next_update_date <= timedelta(minutes=5) + datetime.utcnow()
        assert (
            datetime.utcnow() - timedelta(minutes=5)
            < submitted_date
            < datetime.utcnow()
        )

    # Post a request to log the distributor ids
    resp = app.post(
        "/array/1/log_distributor_id",
        json={
            ti1_id: "123_1",
            ti2_id: "123_2",
            ti3_id: "123_3",
        },
    )
    assert resp.status_code == 200

    with Session(bind=db_engine) as session:
        ti1_r, ti2_r, ti3_r = (
            session.query(TaskInstance)
            .where(TaskInstance.id.in_([ti1_id, ti2_id, ti3_id]))
            .all()
        )

        assert [ti1_r.distributor_id, ti2_r.distributor_id, ti3_r.distributor_id] == [
            "123_1",
            "123_2",
            "123_3",
        ]
