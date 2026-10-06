"""Unit tests for ``BackendClient.launch_instance`` cluster placement.

``target`` must ride the ``?target=`` query parameter on the create POST
(placement only), and be absent when unset so a non-clustered daemon is
never sent a query parameter it does not understand. HTTP is mocked at the
``run_operation`` boundary.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from forgejo_lxd_runner.client import BackendClient


@pytest.fixture
def client() -> BackendClient:
    """A ``BackendClient`` whose ``run_operation`` is a mock."""
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.run_operation = MagicMock(return_value={})  # type: ignore[method-assign]
    return c


def test_launch_instance_omits_target_params_when_unset(client: BackendClient) -> None:
    client.launch_instance({"name": "job-1"})

    kwargs = client.run_operation.call_args.kwargs  # type: ignore[attr-defined]
    assert kwargs["params"] is None


def test_launch_instance_pins_cluster_target(client: BackendClient) -> None:
    client.launch_instance({"name": "job-1"}, target="node2")

    kwargs = client.run_operation.call_args.kwargs  # type: ignore[attr-defined]
    assert kwargs["params"] == {"target": "node2"}
