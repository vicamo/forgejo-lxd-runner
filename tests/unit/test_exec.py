"""Unit tests for ``BackendPluginService.Exec``."""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import grpc
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env


@pytest.fixture
def with_env(service: BackendPluginService, mock_backend_client: MagicMock) -> MagicMock:
    service._envs["job-1"] = _Env(instance_name="job-1")  # noqa: SLF001
    return mock_backend_client


def _stream(*frames: tuple[str, object]) -> Iterator[tuple[str, object]]:
    yield from frames


def _drain(gen) -> list[plugin_pb2.ExecOutput]:  # noqa: ANN001
    return list(gen)


def test_exec_streams_stdout_stderr_then_complete(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    with_env.exec_stream.return_value = _stream(
        ("stdout", b"hello\n"),
        ("stderr", b"warn\n"),
        ("exit", 0),
    )
    req = plugin_pb2.ExecRequest(environment_id="job-1", command=["echo", "hi"])

    outs = _drain(service.Exec(req, context))
    kinds = [o.WhichOneof("Output") for o in outs]

    assert kinds == ["data", "data", "exec_complete"]
    assert outs[0].data.stream == plugin_pb2.DataChunk.STDOUT
    assert outs[0].data.data == b"hello\n"
    assert outs[1].data.stream == plugin_pb2.DataChunk.STDERR
    assert outs[1].data.data == b"warn\n"
    assert outs[2].exec_complete.exit_code == 0

    with_env.exec_stream.assert_called_once_with(
        "job-1", ["echo", "hi"], environment=None, user=None, cwd=None
    )


def test_exec_forwards_frames_as_they_arrive(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    """Interleaved stdout/stderr frames pass through in arrival order."""
    with_env.exec_stream.return_value = _stream(
        ("stdout", b"a"),
        ("stderr", b"E1\n"),
        ("stdout", b"b\n"),
        ("exit", 3),
    )
    req = plugin_pb2.ExecRequest(environment_id="job-1", command=["sh", "-c", "x"])

    outs = _drain(service.Exec(req, context))
    payloads = [(o.data.stream, o.data.data) for o in outs if o.WhichOneof("Output") == "data"]
    assert payloads == [
        (plugin_pb2.DataChunk.STDOUT, b"a"),
        (plugin_pb2.DataChunk.STDERR, b"E1\n"),
        (plugin_pb2.DataChunk.STDOUT, b"b\n"),
    ]
    assert outs[-1].exec_complete.exit_code == 3


def test_exec_emits_exec_failed_on_client_error(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    from forgejo_lxd_runner.client import BackendOperationError

    def _raise(*_a: object, **_kw: object) -> Iterator[tuple[str, object]]:
        raise BackendOperationError("nope")
        yield  # pragma: no cover — makes this a generator

    with_env.exec_stream.side_effect = _raise
    req = plugin_pb2.ExecRequest(environment_id="job-1", command=["false"])

    outs = _drain(service.Exec(req, context))
    assert [o.WhichOneof("Output") for o in outs] == ["exec_failed"]
    assert "nope" in outs[0].exec_failed.error_message


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
