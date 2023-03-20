import os.path
from flask import Flask, Response, send_from_directory, jsonify, request
from flask_cors import CORS
from flask_cors import cross_origin
from time import sleep


app = Flask(__name__, static_url_path='')
CORS(app)


@app.route("/", methods=['GET'])
def health():
    resp = jsonify({'msg': "Jobmon GUI Flask backend is up and running."})
    resp.status_code = 200
    return resp


############################################################################################################

## Test routes
@app.route("/users", methods=['GET'])
def get_users():
    users = ["limingxu", "whoever"]
    resp = jsonify({'users': users})
    resp.status_code = 200
    return resp



@app.route("/workflow_info/<wfid>", methods=['GET'])
def worflow_info(wfid):
    resp = jsonify(id=wfid, name="whatever", status="R", tasks="100")
    resp.status_code = 200
    return resp



## Test routes
class C1():
    # return fake progress
    _counter = 0
    def __init__(self):
        C1._counter += 1
        if C1._counter == 3:
            C1._counter = 0
    def getCounter(self):
        return C1._counter



@app.route("/workflow_status_viz", methods=['get'])
def worflow_status():
    print(f"****************enter*************************")
    ids = request.args.getlist('workflow_ids[]')
    print(f"******************{ids}*********************")
    counter = C1().getCounter()
    if counter == 1:
        workflows = {wfid:

          {
            "id": wfid, "status": "R", "tasks": 99, "retries": 0, "PENDING": 74, "SCHEDULED": 10, "RUNNING": 10, "DONE": 5, "FATAL": 0, 'MAXC': 10000
            } for wfid in ids
          }
    elif counter == 2:
        workflows = {wfid:

          {
            "id": wfid, "status": "R", "tasks": 99, "retries": 0, "PENDING": 39, "SCHEDULED": 40, "RUNNING": 1, "DONE": 19, "FATAL": 0, 'MAXC': 10000
            } for wfid in ids
        }

    else:
        workflows = {wfid:


          {
            "id": wfid, "status": "D", "tasks": 99, "retries": 0, "PENDING": 0, "SCHEDULED": 0, "RUNNING": 0, "DONE": 99, "FATAL": 0, 'MAXC': 10000
            } for wfid in ids
        }
        if "2" in ids:
            workflows["2"] = {"id": "2", "status": "R", "tasks": 99, "retries": 0, "PENDING": 20, "SCHEDULED": 40, "RUNNING": 10, "DONE": 29, "FATAL": 0, 'MAXC': 10000}


    print(f"&&&&&&&&&&&&&&{workflows}")
    resp = jsonify(workflows)
    resp.status_code = 200
    return resp


class C2():
    # return fake progress
    _counter = 0
    def __init__(self):
        C2._counter += 1
        if C2._counter == 15:
            C2._counter = 1
    def getCounter(self):
        return C2._counter

@app.route("/workflow_tt_status_viz/<workflow_id>", methods=["GET"])
def get_workflow_tt_status_viz(workflow_id):
    print(f"****************{workflow_id}*************************")
    sleep(2)
    print("wake up")
    counter = C2().getCounter()
    if counter == 1:
        result = {
                   '5': {'DONE': 0, 'FATAL': 0, 'PENDING': 10, "SCHEDULED": 10, 'RUNNING': 0, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
                   '6': {'DONE': 0, 'FATAL': 0, 'PENDING': 10, "SCHEDULED": 10, 'RUNNING': 0, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
                   '7': {'DONE': 0, 'FATAL': 0, 'PENDING': 5, "SCHEDULED": 5, 'RUNNING': 0, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
                  }

    elif counter == 2:
        result = {
            '5': {'DONE': 0, 'FATAL': 0, 'PENDING': 10, "SCHEDULED": 10, 'RUNNING': 0, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 0, 'FATAL': 0, 'PENDING': 10, "SCHEDULED": 10, 'RUNNING': 0, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 0, 'FATAL': 0, 'PENDING': 9, "SCHEDULED": 0, 'RUNNING': 1, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 3:
        result = {
            '5': {'DONE': 0, 'FATAL': 0, 'PENDING': 12, "SCHEDULED": 6, 'RUNNING': 2, 'id': 5, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 0, 'FATAL': 0, 'PENDING': 12, "SCHEDULED": 6, 'RUNNING': 2, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 1, 'FATAL': 0, 'PENDING': 5, "SCHEDULED": 1,  'RUNNING': 3, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 4:
        result = {
            '5': {'DONE': 2, 'FATAL': 0, 'PENDING': 14, "SCHEDULED": 4, 'RUNNING': 0, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 2, 'FATAL': 0, 'PENDING': 14, "SCHEDULED": 4, 'RUNNING': 0, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 1, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 1,  'RUNNING': 8, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 5:
        result = {
            '5': {'DONE': 2, 'FATAL': 0, 'PENDING': 5, "SCHEDULED": 5, 'RUNNING': 8, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 2, 'FATAL': 0, 'PENDING': 5, "SCHEDULED": 5,  'RUNNING': 8, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 2, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 0,  'RUNNING': 8, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 6:
        result = {
            '5': {'DONE': 4, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 5, 'RUNNING': 11, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 4, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 5, 'RUNNING': 11, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 5, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 0, 'RUNNING': 5, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 7:
        result = {
            '5': {'DONE': 4, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 5, 'RUNNING': 11, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 4, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 5, 'RUNNING': 11, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 6, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 0, 'RUNNING': 4, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 8:
        result = {
            '5': {'DONE': 8, 'FATAL': 1, 'PENDING': 0, "SCHEDULED": 2, 'RUNNING': 11, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 8, 'FATAL': 1, 'PENDING': 0, "SCHEDULED": 2,  'RUNNING': 11, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 7, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 0, 'RUNNING': 3, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    elif counter == 9:
        result = {
            '5': {'DONE': 12, 'FATAL': 1, 'PENDING': 0, "SCHEDULED": 1, 'RUNNING': 6, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 12, 'FATAL': 1, 'PENDING': 0, "SCHEDULED": 1, 'RUNNING': 6, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 9, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 0,  'RUNNING': 1, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    else:
        result = {
            '5': {'DONE': 19, 'FATAL': 1, 'PENDING': 0, "SCHEDULED": 0, 'RUNNING': 0, 'id': 5, 'name': 'tt0', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 5, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '6': {'DONE': 19, 'FATAL': 1, 'PENDING': 0, "SCHEDULED": 0,  'RUNNING': 0, 'id': 6, 'name': 'tt1', 'tasks': 20, 'MAXC': 10000, "task_template_version_id": 6, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2},
            '7': {'DONE': 10, 'FATAL': 0, 'PENDING': 0, "SCHEDULED": 0, 'RUNNING': 0, 'id': 7, 'name': 'tt2', 'tasks': 10, 'MAXC': "NA", "task_template_version_id": 7, "num_attempts_avg": 1.5, "num_attempts_min": 1, "num_attempts_max": 2}
        }
    print(result)
    resp = jsonify(result)
    resp.status_code = 200
    return resp


@app.route("/workflow_overview_viz", methods=["GET"])
def worflows():
    workflows =[{"wf_tool": "tool_1", "DONE":0,"FATAL":0,"PENDING":3,"RUNNING":0,"wf_id":3,"wf_name":"i_am_fake_wf_2","wf_status":"RUNNING","wf_submitted_date":"Fri, 24 Jun 2022 19:10:12 GMT","wf_tool":"unknown-limingxu","wfr_id":30,"wfr_status":"RUNNING"},
                {"wf_tool": "tool_1", "DONE":2,"FATAL":0,"PENDING":0,"RUNNING":0,"wf_id":2,"wf_name":"i_am_fake_wf_1","wf_status":"DONE","wf_submitted_date":"Fri, 24 Jun 2022 19:08:41 GMT","wf_tool":"unknown-limingxu","wfr_id":20,"wfr_status":"DONE"},
                {"wf_tool": "tool_1", "DONE":1,"FATAL":0,"PENDING":0,"RUNNING":0,"wf_id":1,"wf_name":"i_am_fake_wf_0","wf_status":"DONE","wf_submitted_date":"Fri, 24 Jun 2022 19:07:09 GMT","wf_tool":"unknown-limingxu","wfr_id":10,"wfr_status":"DONE"}]
    resp = jsonify({'workflows': workflows})
    resp.status_code = 200
    return resp


@app.route("/tt_error_log_viz/<wf_id>/<tt_id>", methods=["GET"])
def get_tt_error_log_viz(tt_id, wf_id):
    sleep(2)
    if int(tt_id) == 7:
        result = [
            {'error': '/bin/sh: abc: command not found\n', 'error_time': 'Fri, 2 Dec 2022 12:24:33 GMT', 'task_id': 2,
              'task_instance_err_id': 2, 'task_instance_id': 2},
            {'error': 'I want to test line break.\nI want to test a looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong message\n', 'error_time': 'Thu, 1 Dec 2022 12:24:33 GMT', 'task_id': 2,
             'task_instance_err_id': 1, 'task_instance_id': 1},
        ]
    else:
        result = []
    print(result)
    resp = jsonify(result)
    resp.status_code = 200
    return resp


@app.route("/task_template_resource_usage", methods=["POST"])
@cross_origin()
def get_task_template_resource_usage():
    result = {"data": [3, 0, 388100096, 52096778.02, 15, 234, 27.31, 0, 26, [-145.24, 1345.24], [-4.84, 44.84]]}
    print(result)
    resp = jsonify(result)
    resp.status_code = 200
    return resp


@app.route("/task_table_viz/<workflow_id>", methods=["GET"])
def task_details_by_wf_id(workflow_id):
    sleep(2)
    result = {'tasks': [{'task_command': 'echo 1', 'task_id': 1, 'task_max_attempts': 3, 'task_name': 'tt_test_arg-1', 'task_num_attempts': 0, 'task_status': 'REGISTERING', 'task_status_date': 'Tue, 01 Nov 2022 22:23:00 GMT'}]}
    resp = jsonify(result)
    resp.status_code = 200
    return resp

#######################################################################################################################################################################################

@app.route("/workflow_tasks/<wfid>", methods=['GET'])
def get_workflow_tasks(wfid):
    tasks = [
        {"id": 1, "name": "Aberto", "status": "D", "retries": 0},
        {"id": 2, "name": "Alarte Ascendare", "status": "D", "retries": 0},
        {"id": 3, "name": "Alohomora", "status": "D", "retries": 0},
        {"id": 4, "name": "Anteoculatia", "status": "R", "retries": 0},
        {"id": 5, "name": "Aparecium", "status": "R", "retries": 0},
        {"id": 6, "name": "Appare Vestigium", "status": "D", "retries": 0},
        {"id": 7, "name": "Aque Eructo", "status": "Q", "retries": 0},
        {"id": 8, "name": "Arresto Momentum", "status": "D", "retries": 0},
        {"id": 9, "name": "Ascendio", "status": "Q", "retries": 0},
        {"id": 10, "name": "Avada Kedavra", "status": "Q", "retries": 0},
        {"id": 11, "name": "Avis", "status": "Q", "retries": 0},
        {"id": 12, "name": "Bombarda", "status": "D", "retries": 0},
        {"id": 13, "name": "Brackium Emendo", "status": "D", "retries": 0},
        {"id": 14, "name": "Calvorio", "status": "Q", "retries": 0},
        {"id": 15, "name": "Cantis", "status": "Q", "retries": 0},
        {"id": 16, "name": "Carpe Retractum", "status": "D", "retries": 0},
        {"id": 17, "name": "Cave inimicum", "status": "D", "retries": 0},
        {"id": 18, "name": "Cistem Aperio", "status": "D", "retries": 0},
        {"id": 19, "name": "Colloportus", "status": "D", "retries": 0},
        {"id": 20, "name": "Colloshoo", "status": "R", "retries": 0}
        ]
    resp = jsonify({'workflow_tasks': tasks})
    resp.status_code = 200
    return resp



@app.route("/task_up_down_stream/<taskid>", methods=["GET"])
def task_up_down_stream(taskid):
    taskid = int(taskid)
    if taskid == 1:
      up = []
      down = [{"id": 2, "status": "D"}, {"id": 11, "status": "F"}]
    elif taskid == 2:
      up = [{"id": 1, "status": "D"}]
      down = [{"id": 4, "status": "Q"}, {"id": 16, "status": "R"}, {"id": 18, "status": "R"}]
    elif taskid == 11:
      up = [{"id": 1, "status": "D"}]
      down = [{"id": 16, "status": "R"}]
    elif taskid == 4:
      up = [{"id":2, "status": "Q"}]
      down = []
    elif taskid == 16:
      up = [{"id": 2, "status": "D"}, {"id": 11, "status": "Q"}]
      down = [{"id": 10, "status": "Q"}]
    elif taskid == 18:
      up = [{"id": 2, "status": "D"}]
      down = [{"id": 10, "status": "Q"}]
    elif taskid == 10:
      up = [{"id": 16, "status": "R"}, {"id": 18, "status": "R"}]
      down = []
    else:
      up = []
      down = []
    resp = jsonify({"up":up, "down":down})
    resp.status_code = 200
    return resp


@app.route("/task_instances/<taskid>", methods=["GET"])
def task_instance_info(taskid):
    task1 = [{"task_instance_id": int(taskid) * 10, "executor_id": 12345, "status": "D", "resource_usage": "HP: 10, MP=10", "error_log": ""},
             {"task_instance_id": int(taskid) * 10 - 1, "executor_id": 1234, "status": "F", "resource_usage": "HP: 10, MP=100", "error_log": "Used cat hair instead of human hair"},
            ]
    resp = jsonify({"task": task1})
    resp.status_code = 200
    return resp


@app.route("/dag/<dagid>", methods=["GET"])
def get_dag(dagid):
    # generate dag and save a json

    resp = jsonify({"data": data})
    resp.status_code = 200
    return resp


if __name__ == "__main__":
    print("This creates a unit test server with fixed response data.")
    app.run(host="0.0.0.0", port=8070, debug=True)
