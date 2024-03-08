"""Initialize Web services."""
import os
import multiprocessing as mp
from time import sleep
from random import randint
from jobmon.server.web.app_factory import AppFactory  # noqa F401
from flask_cors import CORS
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from jobmon.client.api import Tool


print("This server starts a new jobmon server instance at port 8070 and continuously creates "
      "workflows as the login user. If this is your first time running the testing server on a"
      " node, please run _create_sqlite_db.py first.")
sql_file = "/tmp/tests.sqlite"

database_uri = f"sqlite:///{sql_file}"
os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"] = database_uri
os.environ["JOBMON__FLASK__SQLALCHEMY_DATABASE_URI"] = database_uri
os.environ["JOBMON__HTTP__SERVICE_URL"] = "http://localhost:8070"

_app_factory = AppFactory()
app = _app_factory.get_app()
CORS(app, resources={r"/*": {"origins": "*"}})


def run_server():
    with app.app_context():
        app.run(host="0.0.0.0", port=8070, debug=True)


def create_multiple_status_wf():
    """Create wf with:
           1. Multiple tt
           2. Various status in tt
           3. tt cross wfs
    """
    # give the large wf sometime to run
    sleep(60)
    C = "multiprocess"
    Q = "null.q"
    tool = Tool("complex_wf_tool")
    # shared tt for all wf
    delay_success_tt = tool.get_task_template(
        template_name="delay_success",
        command_template="sleep {arg} || true || {arg_filler}",
        node_args=["arg"],
        task_args=["arg_filler"]
    )

    delay_random_tt = tool.get_task_template(
        template_name="delay_random",
        command_template="sleep {arg1} && echo {arg2} 1>&2 && {t_or_f}",
        node_args=["arg1", "arg2"],
        task_args=["t_or_f"]
    )

    for i in range(100):
        wf = tool.create_workflow(
            name=f"wf_{i}",
            default_cluster_name=C,
            default_compute_resources_set={"queue": Q, "num_cores": 1},
        )

        tasks = []
        # group 1: three successful tasks to sleep 10, 90, and 170 sec as upstream to other tasks
        tasks_g1 = []
        for j in range(3):
            t = delay_success_tt.create_task(
                name=f"wf_{i}-group1-task_{j}",
                arg=j * 80 + 10,
                arg_filler=f"This is the first tier task for wf_{i} task_{j} {randint(0, 10000)}",
                compute_resources={"queue": Q, "num_cores": 1},
            )
            tasks.append(t)
            tasks_g1.append(t)

        tasks_g2 = []
        g2_total = randint(5, 20)
        for j in range(g2_total):
            # half of the tasks fail
            t_or_f = "true" if j % 2 else "false"
            # first half has group 1 upstream, so that they wait
            if j < g2_total/2:
                t = delay_random_tt.create_task(
                    name=f"wf_{i}-group2-task_{j}",
                    arg1=randint(1, 20),
                    arg2=f"group2_{j}_{randint(0, 10000)} {'content to fill in error log - ' * randint(1, 100)}",
                    t_or_f=t_or_f,
                    upstream_tasks=[tasks_g1[randint(0, 2)]],
                    compute_resources={"queue": Q, "num_cores": 1},
                )
            else:
                t = delay_random_tt.create_task(
                    name=f"wf_{i}-group2-task_{j}",
                    arg1=randint(1, 20),
                    arg2=f"group2_{j}_{randint(0, 10000)} {'content to fill in error log - ' * randint(1, 100)}",
                    t_or_f=t_or_f,
                    compute_resources={"queue": Q, "num_cores": 1},
                )
            tasks.append(t)
            tasks_g2.append(t)
        #   group 3: use a new tt for this wf only to compare
        tasks_g3 = []
        g3_total = randint(5, 50)
        new_tt = tool.get_task_template(
                        template_name=f"tt_for_wf_{i}",
                        command_template="echo {arg}",
                        node_args=["arg"],
        )
        for j in range(g3_total):
            t = new_tt.create_task(
                name=f"wf_{i}-group3-task_{j}",
                arg=str(j),
                upstream_tasks=[tasks_g2[randint(0, len(tasks_g2) - 1)]],
                compute_resources={"queue": Q, "num_cores": 1},
            )
            tasks.append(t)
            tasks_g3.append(t)

        wf.add_tasks(tasks)
        wf.run()
        # insert some random resource data
        db_engine = create_engine(database_uri)
        with Session(bind=db_engine) as session:
            for task in tasks:
                query = f"""
                        UPDATE task_instance
                        SET wallclock = {randint(100, 1000)}, maxrss = {randint(300000000, 4000000000)}
                        WHERE task_id = {task.task_id}"""
                session.execute(query)
            session.commit()
        # allow the large workflow to finish
        sleep(2)


def create_large_workflow():
    C = "dummy"
    Q = "null.q"

    TOTAL_TASK = 50000 # refer to production wf 53630
    tool = Tool("large_wf_tool")
    tt_base = tool.get_task_template(
        template_name=f"tt_for_large_wf_base",
        command_template="true || echo {arg}",
        node_args=["arg"]
    )
    wf = tool.create_workflow(name="i_am_huge", default_cluster_name=C)
    tasks = []
    task = tt_base.create_task(name=f"base",
                arg="I_am_the_one_everyone_else_depends_on",
                upstream_tasks=[],
                compute_resources={"queue": Q, "num_cores": 1},)
    tasks.append(task)
    tt = tool.get_task_template(
        template_name=f"tt_for_large_wf",
        command_template="true || {arg}",
        node_args=["arg"]
    )
    for i in range(TOTAL_TASK):
        t = tt.create_task(name=f"a_task_{i}",
                arg=f"whatever{i}",
                upstream_tasks=[task],
                compute_resources={"queue": Q, "num_cores": 1},)
        tasks.append(t)
    wf.add_tasks(tasks)
    wf.run()
    # fill in fake resource usage data


if __name__ == "__main__":
    ctx = mp.get_context("fork")
    p_server = ctx.Process(target=run_server, args=())
    p_server.start()
    # the large wf
    p_large_wf = ctx.Process(target=create_large_workflow, args=())
    p_large_wf.start()
    # multiple status wf in a seperate process
    create_multiple_status_wf()
