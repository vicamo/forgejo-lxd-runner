"""Unit tests for ``BackendPluginService.CopyIn`` / ``CopyOut``."""

from __future__ import annotations

import io
import tarfile
from pathlib import Path
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


def _tar_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def test_copy_in_extracts_and_pushes(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    payload = _tar_bytes({"hello.txt": b"hi"})
    chunks = [
        plugin_pb2.CopyInChunk(environment_id="job-1", dest_path="/work", data=payload[:10]),
        plugin_pb2.CopyInChunk(data=payload[10:]),
    ]

    # Capture what recursive_put sees at call time — the source tree lives
    # in a TemporaryDirectory that's gone by the time CopyIn returns.
    captured: dict[str, bytes] = {}

    def _capture(src: str, dest: str) -> None:  # noqa: ARG001
        for path in Path(src).rglob("*"):
            if path.is_file():
                captured[path.relative_to(src).as_posix()] = path.read_bytes()

    with_env.files.recursive_put.side_effect = _capture

    resp = service.CopyIn(iter(chunks), context)

    assert isinstance(resp, plugin_pb2.CopyInResponse)
    with_env.execute.assert_called_once_with(["mkdir", "-p", "/work"])
    with_env.files.recursive_put.assert_called_once()
    _, dest_arg = with_env.files.recursive_put.call_args.args
    assert dest_arg == "/work"
    assert captured == {"hello.txt": b"hi"}


def test_copy_in_requires_first_chunk_envelope(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    with_env: MagicMock,  # noqa: ARG001
) -> None:
    chunks = [plugin_pb2.CopyInChunk(data=b"junk")]  # no env_id / dest_path
    with pytest.raises(aborted) as exc:
        service.CopyIn(iter(chunks), context)
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]


def test_copy_in_rejects_envelope_on_later_chunk(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    with_env: MagicMock,  # noqa: ARG001
) -> None:
    payload = _tar_bytes({"a.txt": b"x"})
    chunks = [
        plugin_pb2.CopyInChunk(environment_id="job-1", dest_path="/w", data=payload),
        plugin_pb2.CopyInChunk(environment_id="job-1", data=b""),  # illegal
    ]
    with pytest.raises(aborted) as exc:
        service.CopyIn(iter(chunks), context)
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]


def test_copy_out_streams_tar_of_src_path(
    service: BackendPluginService,
    context: MagicMock,
    with_env: MagicMock,
    tmp_path: Path,  # noqa: ARG001
) -> None:
    def _recursive_get(src: str, dest: str) -> None:  # noqa: ARG001
        (Path(dest) / "out.txt").write_bytes(b"bye")

    with_env.files.recursive_get.side_effect = _recursive_get

    req = plugin_pb2.CopyOutRequest(environment_id="job-1", src_path="/some/dir")
    chunks = list(service.CopyOut(req, context))
    with_env.files.recursive_get.assert_called_once()

    tar_bytes = b"".join(c.data for c in chunks)
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
        names = tar.getnames()
        member = tar.extractfile("out.txt")
        assert member is not None
        assert member.read() == b"bye"
    assert "out.txt" in names
