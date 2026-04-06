"""Tests for deferred triage when cluster accounting is not finalized."""

import time
from unittest import mock

import pytest

from jobmon.core.constants import TaskInstanceStatus
from jobmon.core.exit_info import RemoteExitInfo
from jobmon.core.requester import Requester
from jobmon.distributor.distributor_service import DistributorService
from jobmon.distributor.distributor_task_instance import DistributorTaskInstance
from jobmon.plugins.dummy.dummy_distributor import DummyDistributor


@pytest.fixture
def distributor_service():
    cluster_interface = DummyDistributor("dummy")
    requester = mock.MagicMock(spec=Requester)
    ds = DistributorService(cluster_interface, requester=requester)
    return ds


@pytest.fixture
def task_instance():
    requester = mock.MagicMock(spec=Requester)
    ti = DistributorTaskInstance(
        task_instance_id=1,
        workflow_run_id=1,
        status=TaskInstanceStatus.TRIAGING,
        requester=requester,
    )
    ti._distributor_id = "12345_1"
    return ti


def test_finalized_exit_info_transitions_immediately(
    distributor_service, task_instance
):
    """Finalized exit info should transition the TI on the first attempt."""
    finalized_result = RemoteExitInfo(
        error_state=TaskInstanceStatus.UNKNOWN_ERROR,
        error_message="OOM killed with exit code 71",
        finalized=True,
    )

    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=finalized_result,
    ):
        distributor_service.triage_error(task_instance)

    assert task_instance.triage_attempts == 1
    assert task_instance.error_state == TaskInstanceStatus.UNKNOWN_ERROR


def test_unfinalized_exit_info_defers_transition(distributor_service, task_instance):
    """Unfinalized exit info should leave TI in TRIAGING (no transition)."""
    unfinalized_result = RemoteExitInfo(
        error_state=TaskInstanceStatus.UNKNOWN_ERROR,
        error_message="status=SUCCESS, state=RUNNING",
        finalized=False,
    )

    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=unfinalized_result,
    ):
        distributor_service.triage_error(task_instance)

    assert task_instance.triage_attempts == 1
    assert task_instance.first_triage_time is not None
    # No transition — error_state should still be empty
    assert task_instance.error_state == ""


def test_unfinalized_then_finalized_transitions_on_retry(
    distributor_service, task_instance
):
    """Second attempt with finalized info should transition."""
    unfinalized = RemoteExitInfo(
        error_state=TaskInstanceStatus.UNKNOWN_ERROR,
        error_message="state=RUNNING",
        finalized=False,
    )
    finalized = RemoteExitInfo(
        error_state=TaskInstanceStatus.RESOURCE_ERROR,
        error_message="OOM killed",
        finalized=True,
    )

    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=unfinalized,
    ):
        distributor_service.triage_error(task_instance)

    assert task_instance.triage_attempts == 1
    assert task_instance.error_state == ""  # Not transitioned

    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=finalized,
    ):
        distributor_service.triage_error(task_instance)

    assert task_instance.triage_attempts == 2
    assert task_instance.error_state == TaskInstanceStatus.RESOURCE_ERROR


def test_unfinalized_past_timeout_transitions_anyway(
    distributor_service, task_instance
):
    """After MAX_TRIAGE_DURATION, should transition even if unfinalized."""
    unfinalized = RemoteExitInfo(
        error_state=TaskInstanceStatus.UNKNOWN_ERROR,
        error_message="state=RUNNING, contradictory",
        finalized=False,
    )

    # Set first_triage_time to well past the timeout
    task_instance.first_triage_time = time.time() - 600  # 10 min ago

    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=unfinalized,
    ):
        distributor_service.triage_error(task_instance)

    # Should have transitioned despite being unfinalized
    assert task_instance.error_state == TaskInstanceStatus.UNKNOWN_ERROR


def test_legacy_tuple_return_treated_as_finalized(distributor_service, task_instance):
    """Legacy plugins returning (str, str) should be treated as finalized."""
    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=(
            TaskInstanceStatus.UNKNOWN_ERROR,
            "legacy error message",
        ),
    ):
        distributor_service.triage_error(task_instance)

    assert task_instance.error_state == TaskInstanceStatus.UNKNOWN_ERROR


def test_triage_attempts_increment_across_deferrals(distributor_service, task_instance):
    """Each deferral should increment triage_attempts."""
    unfinalized = RemoteExitInfo(
        error_state=TaskInstanceStatus.UNKNOWN_ERROR,
        error_message="not ready",
        finalized=False,
    )

    with mock.patch.object(
        distributor_service.cluster_interface,
        "get_remote_exit_info",
        return_value=unfinalized,
    ):
        for i in range(5):
            distributor_service.triage_error(task_instance)

    assert task_instance.triage_attempts == 5
    assert task_instance.error_state == ""  # Still deferred
