"""Unit tests for ``BackendPluginService.Exec``."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env


@pytest.fixture
def with_env(service: BackendPluginService, mock_pylxd_client: MagicMock) -> MagicMock:
    service._envs["job-1"] = _Env(instance_name="job-1")  # noqa: SLF001
    instance = MagicMock(name="lxd_instance")
    mock_pylxd_client.instances.get.return_value = instance
    return instance


def _drain(gen) -> list[plugin_pb2.ExecOutput]:  # noqa: ANN001
    return list(gen)


def test_exec_streams_stdout_stderr_then_complete(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    with_env.execute.return_value = SimpleNamespace(stdout="hello\n", stderr="warn\n", exit_code=0)
    req = plugin_pb2.ExecRequest(environment_id="job-1", command=["echo", "hi"])

    outs = _drain(service.Exec(req, context))
    kinds = [o.WhichOneof("Output") for o in outs]

    assert kinds == ["data", "data", "exec_complete"]
    assert outs[0].data.stream == plugin_pb2.DataChunk.STDOUT
    assert outs[0].data.data == b"hello\n"
    assert outs[1].data.stream == plugin_pb2.DataChunk.STDERR
    assert outs[1].data.data == b"warn\n"
    assert outs[2].exec_complete.exit_code == 0


def test_exec_omits_empty_streams(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    with_env.execute.return_value = SimpleNamespace(stdout="", stderr="", exit_code=42)
    req = plugin_pb2.ExecRequest(environment_id="job-1", command=["false"])

    outs = _drain(service.Exec(req, context))
    kinds = [o.WhichOneof("Output") for o in outs]

    assert kinds == ["exec_complete"]
    assert outs[0].exec_complete.exit_code == 42


def test_exec_rejects_non_numeric_user(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    with_env: MagicMock,  # noqa: ARG001
) -> None:
    req = plugin_pb2.ExecRequest(environment_id="job-1", command=["id"], user="ubuntu")
    with pytest.raises(aborted) as exc:
        _drain(service.Exec(req, context))
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]


def test_exec_unknown_environment_aborts_not_found(
    service: BackendPluginService, context: MagicMock, aborted: type[Exception]
) -> None:
    req = plugin_pb2.ExecRequest(environment_id="ghost", command=["id"])
    with pytest.raises(aborted) as exc:
        _drain(service.Exec(req, context))
    assert exc.value.code == grpc.StatusCode.NOT_FOUND  # type: ignore[attr-defined]
