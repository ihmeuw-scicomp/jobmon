"""Locks down the distributor's stderr breadcrumb protocol.

The orchestrator's ``DistributorContext.wait_for_startup_signal``:

  - watches stderr for the literal substring ``ALIVE`` (success) or
    ``SHUTDOWN`` (clean teardown);
  - on a ``DistributorStartupTimeout``, captures the full stderr buffer into
    the exception body so the user sees what the distributor was doing.

Adding any breadcrumb that contains either token would cause the orchestrator
to treat startup as succeeded prematurely or misinterpret shutdown — both
silent regressions that this test exists to catch. The test also checks the
emit order matches the public order documented in ``cli.run_distributor`` /
``DistributorService.run`` so consumers grepping the timeout payload don't
have to guess.
"""

from __future__ import annotations

import inspect
import re

from jobmon.distributor import cli as cli_mod
from jobmon.distributor import distributor_service as svc_mod


def _phase_tokens_in(source: str) -> list[str]:
    """Return phase names from breadcrumb emissions in ``source``.

    Matches both literal ``sys.stderr.write("JOBMON_PHASE: foo\\n")`` calls
    and the ``_phase("foo")`` helper call sites used in ``cli.py`` (the
    helper expands to the same literal at runtime, but at the source level
    it's a function call rather than a string). The ``_phase`` helper
    definition itself contains the f-string ``"JOBMON_PHASE: {name}\\n"``;
    exclude any match containing ``{`` so the helper's own format string
    doesn't show up as a phase named ``{name}``.
    """

    def _is_real_name(name: str) -> bool:
        return "{" not in name

    literals = [
        m
        for m in re.findall(r'"JOBMON_PHASE: ([^"\n]+)\\n"', source)
        if _is_real_name(m)
    ]
    helpers = re.findall(r'_phase\("([^"]+)"\)', source)
    return literals + helpers


def test_no_breadcrumb_contains_alive_or_shutdown_tokens():
    """Breadcrumbs must not collide with the orchestrator's signal tokens.

    A breadcrumb containing ``ALIVE`` would make the orchestrator skip the
    timeout wait and treat the distributor as ready before any real work
    has happened; a breadcrumb containing ``SHUTDOWN`` would let
    ``DistributorContext._shutdown`` short-circuit and miss a true shutdown.
    """
    cli_src = inspect.getsource(cli_mod)
    svc_src = inspect.getsource(svc_mod)
    for name in _phase_tokens_in(cli_src) + _phase_tokens_in(svc_src):
        assert (
            "ALIVE" not in name
        ), f"Breadcrumb {name!r} contains the orchestrator's success token"
        assert (
            "SHUTDOWN" not in name
        ), f"Breadcrumb {name!r} contains the orchestrator's shutdown token"


def test_breadcrumb_order_matches_documented_startup_sequence():
    """Locks down the public ordering of startup phases.

    Updating this list when you legitimately change the startup sequence is
    fine — but every change of the sequence is a contract change for
    anyone parsing ``DistributorStartupTimeout`` payloads, so it should be
    intentional.
    """
    cli_src = inspect.getsource(cli_mod)
    svc_src = inspect.getsource(svc_mod)
    assert _phase_tokens_in(cli_src) == [
        "cli_entered",
        "context_bound",
        "cluster_bound",
        "cluster_interface_built",
        "workflow_run_set",
    ]
    assert _phase_tokens_in(svc_src) == [
        "signal_handlers_set",
        "cluster_started",
        "transitioned_to_launched",
    ]
