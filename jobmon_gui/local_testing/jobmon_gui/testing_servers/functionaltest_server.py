import multiprocessing as mp
import os
import socket
import sys
from time import sleep

from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session
from sqlalchemy_utils import create_database, database_exists
import uvicorn

from jobmon.client.api import Tool
from jobmon.core.configuration import JobmonConfig
from jobmon.server.web.api import get_app
from jobmon.server.web.db_admin import apply_migrations
from jobmon.server.web.models import load_metadata


TESTS_DB_FILEPATH = "/tmp/tests.sqlite"


class WebServerProcess:
    def __init__(self, filepath: str) -> None:
        if sys.platform == "darwin":
            self.web_host = "127.0.0.1"
        else:
            self.web_host = socket.getfqdn()
        self.web_port = 8070
        self.filepath = filepath

    def start_web_service(self):
        database_uri = f"sqlite:///{self.filepath}"
        print(f"Database URI: {database_uri}")
        os.environ["JOBMON__DB__SQLALCHEMY_DATABASE_URI"] = database_uri
        os.environ["JOBMON__WEB__SQLALCHEMY_DATABASE_URI"] = database_uri
        os.environ["JOBMON__OTLP__WEB_ENABLED"] = "true"
        os.environ["JOBMON__OTLP__SPAN_EXPORTER"] = ""
        os.environ["JOBMON__OTLP__LOG_EXPORTER"] = ""
        os.environ["JOBMON__HTTP__SERVICE_URL"] = "http://localhost:8070/api/v3"

        if not os.path.exists(self.filepath):
            open(self.filepath, 'a').close()
            # init db
            if not database_exists(database_uri):
                create_database(database_uri)
                load_metadata(database_uri)
            apply_migrations(database_uri)

        config = JobmonConfig(dict_config={"db": {"sqlalchemy_database_uri": database_uri}})
        app = get_app(config)
        config.set("http", "service_url", f"http://{self.web_host}:{self.web_port}/api/v3")
        config.write()

        uvicorn.run(app, host=self.web_host, port=self.web_port, log_level="info")


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
            workflow_attributes={"test_attribute": "test"}
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
            if j < g2_total / 2:
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
        db_engine = create_engine(f"sqlite:///{TESTS_DB_FILEPATH}")
        with Session(bind=db_engine) as session:
            for task in tasks:
                query = text(f"""
                        UPDATE task_instance
                        SET wallclock = {randint(100, 1000)}, maxrss = {randint(300000000, 4000000000)}
                        WHERE task_id = {task.task_id}""")
                session.execute(query)
            session.commit()
        # allow the large workflow to finish
        sleep(2)


def create_large_workflow():
    C = "dummy"
    Q = "null.q"

    TOTAL_TASK = 50000  # refer to production wf 53630
    tool = Tool("large_wf_tool")
    tt_base = tool.get_task_template(
        template_name=f"tt_for_large_wf_base",
        command_template="true || echo {arg}",
        node_args=["arg"]
    )
    wf = tool.create_workflow(
        name="i_am_huge",
        default_cluster_name=C,
        workflow_attributes={"test_attribute": "test"}
    )
    tasks = []
    task = tt_base.create_task(name=f"base",
                               arg="I_am_the_one_everyone_else_depends_on",
                               upstream_tasks=[],
                               compute_resources={"queue": Q, "num_cores": 1}, )
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
                           compute_resources={"queue": Q, "num_cores": 1}, )
        tasks.append(t)
    wf.add_tasks(tasks)
    wf.run()
    # fill in fake resource usage data


def start_web_service(filepath=TESTS_DB_FILEPATH):
    server = WebServerProcess(filepath=filepath)
    server.start_web_service()


if __name__ == "__main__":
    ctx = mp.get_context("fork")
    p_server = ctx.Process(target=start_web_service, args=())
    p_server.start()
    # p_large_wf = ctx.Process(target=create_large_workflow, args=()) # TODO: issue: cluster "dummy" dous not exist, so response from jobmon_core/src/jobmon/core/cluster.py: _, response = self.requester.send_request(app_route=app_route, message={}, request_type="get") returns {'cluster': null}
    # p_large_wf.start()
    # create_multiple_status_wf()  # TODO: issue: cluster "multiprocess" dous not exist, so response from jobmon_core/src/jobmon/core/cluster.py: _, response = self.requester.send_request(app_route=app_route, message={}, request_type="get") returns {'cluster': null}
