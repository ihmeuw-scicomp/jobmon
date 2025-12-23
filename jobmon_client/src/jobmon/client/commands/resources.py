"""Resource usage commands.

Commands for querying resource usage and generating resource YAML files:
- Task template resource usage queries
- Resource YAML generation
"""

from typing import Any, Dict, List, Optional

import structlog

from jobmon.core.constants import ExecludeTTVs
from jobmon.core.requester import Requester

logger = structlog.get_logger(__name__)


def task_template_resources(
    task_template_version: int,
    workflows: Optional[list] = None,
    node_args: Optional[Dict] = None,
    ci: Optional[float] = None,
    requester: Optional[Requester] = None,
) -> Optional[Dict]:
    """Get aggregate resource usage data for a given TaskTemplateVersion.

    Args:
        task_template_version: The task template version ID the user wants to find the
            resource usage of.
        workflows: list of workflows a user wants query by.
        node_args: dictionary of node arguments a user wants to query by.
        ci: confidence interval. Not calculate if None.
        requester: object to communicate with the flask services

    Returns:
        Dataframe of TaskTemplate resource usage
    """
    exclude_list = ExecludeTTVs.EXECLUDE_TTVS
    if task_template_version in exclude_list:
        msg = (
            f"Resource usage query for task_template_version {task_template_version}"
            f"  is restricted due to excessive size."
        )
        logger.warning(msg)
        print(msg)
        return None
    message: Dict[Any, Any] = dict()
    message["task_template_version_id"] = task_template_version
    if workflows:
        message["workflows"] = workflows
    if node_args:
        message["node_args"] = node_args
    if ci:
        message["ci"] = str(ci)  # Convert to string for V3 API

    if requester is None:
        requester = Requester.from_defaults()

    app_route = "/task_template_resource_usage"
    return_code, response = requester.send_request(
        app_route=app_route, message=message, request_type="post"
    )

    # Handle the V3 Pydantic response format
    return response["formatted_stats"]


def _get_yaml_data(
    wfid: Optional[int],
    tid: Optional[int],
    v_mem: str,
    v_core: str,
    v_runtime: str,
    requester: Requester,
) -> Dict:
    """Get resource data for YAML generation.

    Internal helper function to fetch resource usage data from the server.

    Args:
        wfid: Workflow ID (or None if using task ID)
        tid: Task ID (or None if using workflow ID)
        v_mem: Memory aggregation strategy ('avg', 'min', 'max')
        v_core: Core aggregation strategy ('avg', 'min', 'max')
        v_runtime: Runtime aggregation strategy ('avg', 'min', 'max')
        requester: Server requester object

    Returns:
        Dictionary mapping task template version IDs to [name, core, mem, runtime, queue]
    """
    tt_exclude_list = ExecludeTTVs.EXECLUDE_TTVS

    key_map_m = {"avg": "mean_mem", "min": "min_mem", "max": "max_mem"}
    key_map_r = {"avg": "mean_runtime", "min": "min_runtime", "max": "max_runtime"}

    # Get task template version ids
    rc, res = requester.send_request(
        app_route="/get_task_template_version",
        message={"task_id": tid} if wfid is None else {"workflow_id": wfid},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for get_task_template_version."
        )

    # data structure: {ttv_id: [name, core, mem, runtime, queue]}
    ttvis_dic = dict()

    # create a holder for tts that are in the exclude list
    tt_in_exclude = []
    for t in res["task_template_version_ids"]:
        if t["id"] in tt_exclude_list:
            tt_in_exclude.append(t)
            msg = f"Task template {t} contains too many tasks and will be excluded."
            logger.warning(msg)
            print(msg)
        else:
            ttvis_dic[t["id"]] = [t["name"]]

    for t in tt_in_exclude:
        # fill in template using default values
        print(f"Fill template for t using {t}")
        ttvis_dic[t["id"]] = [t["name"], 1, 1, 3600, "all.q"]

    # set of ttvs not in exclude list
    actual_ttvs = set(ttvis_dic.keys()) - set([i["id"] for i in tt_in_exclude])

    if len(actual_ttvs) == 0:
        return ttvis_dic

    # get core
    ttvis = str([i for i in actual_ttvs]).replace("[", "(").replace("]", ")")
    rc, res = requester.send_request(
        app_route="/get_requested_cores",
        message={"task_template_version_ids": f"{ttvis}"},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for /get_requested_cores."
        )
    core_info = res["core_info"]
    for record in core_info:
        ttvis_dic[int(record["id"])].append(record[v_core])

    # Get actually mem and runtime for each ttvi
    for ttv in actual_ttvs:
        rc, res = requester.send_request(
            app_route="/task_template_resource_usage",
            message={"task_template_version_id": ttv},
            request_type="post",
        )
        if rc != 200:
            raise AssertionError(
                f"Server returns HTTP error code: {rc} "
                f"for /task_template_resource_usage."
            )
        # Handle V3 API response format
        usage = res

        ttvis_dic[int(ttv)].append(int(usage[key_map_m[v_mem]]))
        ttvis_dic[int(ttv)].append(int(usage[key_map_r[v_runtime]]))

    # get queue
    rc, res = requester.send_request(
        app_route="/get_most_popular_queue",
        message={"task_template_version_ids": f"{ttvis}"},
        request_type="get",
    )
    if rc != 200:
        raise AssertionError(
            f"Server returns HTTP error code: {rc} " f"for /get_most_popular_queue."
        )
    for record in res["queue_info"]:
        ttvis_dic[int(record["id"])].append(record["queue"])
    return ttvis_dic


def _create_yaml(data: Optional[Dict] = None, clusters: Optional[List] = None) -> str:
    """Create YAML content from resource data.

    Internal helper function to format resource data as YAML.

    Args:
        data: Dictionary mapping task template version IDs to resource info
        clusters: List of cluster names to include

    Returns:
        YAML formatted string
    """
    yaml = "task_template_resources:\n"
    if clusters is None:
        clusters = []
    if data is None or len(clusters) == 0:
        return yaml
    for ttv in data.keys():
        yaml += f"  {data[ttv][0]}:\n"  # name
        for cluster in clusters:
            yaml += f"    {cluster}:\n"  # cluster
            yaml += f"      cores: {data[ttv][1]}\n"  # core
            yaml += f'      memory: "{data[ttv][2]}B"\n'  # mem
            yaml += f"      runtime: {data[ttv][3]}\n"  # runtime
            yaml += f'      queue: "{data[ttv][4]}"\n'  # queue
    return yaml


def create_resource_yaml(
    wfid: Optional[int],
    tid: Optional[int],
    v_mem: str,
    v_core: str,
    v_runtime: str,
    clusters: List,
    requester: Optional[Requester] = None,
) -> str:
    """Create a resource YAML file from workflow or task resource usage.

    Args:
        wfid: Workflow ID to query (or None if using task ID)
        tid: Task ID to query (or None if using workflow ID)
        v_mem: Memory aggregation strategy ('avg', 'min', 'max')
        v_core: Core aggregation strategy ('avg', 'min', 'max')
        v_runtime: Runtime aggregation strategy ('avg', 'min', 'max')
        clusters: List of cluster names to include in the YAML
        requester: object to communicate with the flask services

    Returns:
        YAML formatted string containing resource configurations
    """
    if requester is None:
        requester = Requester.from_defaults()

    ttvis_dic = _get_yaml_data(wfid, tid, v_mem, v_core, v_runtime, requester)
    yaml = _create_yaml(ttvis_dic, clusters)
    return yaml
