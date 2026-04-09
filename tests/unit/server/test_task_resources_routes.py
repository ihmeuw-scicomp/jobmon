"""Unit tests for the task_resources route helpers."""

import pytest

from jobmon.server.web.routes.v3.fsm.task_resources import (
    _deserialize_requested_resources,
)


class TestDeserializeRequestedResources:
    """Test the deserialize helper used by /task_resources/{id}.

    Requested resources are written via ``json.dumps`` by
    ``/task/bind_resources`` (which emits lowercase JSON literals) but older
    rows may use Python ``repr``-style literals. The helper must handle both.
    """

    def test_json_dict_with_json_booleans(self) -> None:
        """Rows written by json.dumps contain lowercase true/false/null.

        Regression test: previously the route called ast.literal_eval which
        raised ValueError on JSON booleans, producing 500s that crashed the
        distributor and stuck tasks in INSTANTIATED indefinitely.
        """
        raw = (
            '{"project": "proj_dismod_at", "memory": 2, "runtime": 1024, '
            '"cores": 3, "absolute_buffer_applied": true, '
            '"absolute_buffer_min": 5, "runtime_before_buffer_sec": 723.23}'
        )
        result = _deserialize_requested_resources(raw)
        assert result["absolute_buffer_applied"] is True
        assert result["memory"] == 2
        assert result["runtime_before_buffer_sec"] == pytest.approx(723.23)

    def test_json_with_null(self) -> None:
        raw = '{"project": "proj_test", "stderr": null}'
        result = _deserialize_requested_resources(raw)
        assert result["stderr"] is None

    def test_python_repr_style_dict_fallback(self) -> None:
        """Legacy rows may use Python repr with True/False/None."""
        raw = (
            "{'project': 'proj_test', 'absolute_buffer_applied': True, 'stderr': None}"
        )
        result = _deserialize_requested_resources(raw)
        assert result["absolute_buffer_applied"] is True
        assert result["stderr"] is None

    def test_plain_json_dict(self) -> None:
        raw = '{"memory": 10, "cores": 1}'
        result = _deserialize_requested_resources(raw)
        assert result == {"memory": 10, "cores": 1}

    def test_invalid_raises(self) -> None:
        with pytest.raises((ValueError, SyntaxError)):
            _deserialize_requested_resources("not a dict at all {{{")
