"""Unit tests for ``BackendPluginService.CopyIn`` / ``CopyOut``."""

from __future__ import annotations

import io
import tarfile
from unittest.mock import MagicMock, call

import grpc
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env


@pytest.fixture
def with_env(service: BackendPluginService, mock_backend_client: MagicMock) -> MagicMock:
    service._envs["job-1"] = _Env(instance_name="job-1")  # noqa: SLF001
    return mock_backend_client


def _tar_bytes(entries: dict[str, bytes | tuple[str, str]]) -> bytes:
    """Build a tar; ``value`` is file bytes, or ``("symlink", target)``."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, value in entries.items():
            info = tarfile.TarInfo(name=name)
            if isinstance(value, tuple) and value[0] == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = value[1]
                tar.addfile(info)
            else:
                assert isinstance(value, bytes)
                info.size = len(value)
                tar.addfile(info, io.BytesIO(value))
    return buf.getvalue()


def test_copy_in_replays_tar_entries_as_rest_pushes(
    service: BackendPluginService, context: MagicMock, with_env: MagicMock
) -> None:
    payload = _tar_bytes({"hello.txt": b"hi", "link": ("symlink", "hello.txt")})
    chunks = [
        plugin_pb2.CopyInChunk(environment_id="job-1", dest_path="/work", data=payload[:10]),
        plugin_pb2.CopyInChunk(data=payload[10:]),
    ]

    resp = service.CopyIn(iter(chunks), context)

    assert isinstance(resp, plugin_pb2.CopyInResponse)
    # The dest directory is created up front, then each tar entry maps
    # to a matching client call.
    assert with_env.push_directory.call_args_list == [call("job-1", "/work")]
    with_env.push_file.assert_called_once_with("job-1", "/work/hello.txt", b"hi", mode=0o644)
    with_env.push_symlink.assert_called_once_with("job-1", "/work/link", "hello.txt")


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
) -> None:
    # Simulate ``/some/dir`` containing one file: ``out.txt`` with body b"bye".
    def _pull(instance: str, path: str) -> tuple[str, bytes, int]:
        assert instance == "job-1"
        if path == "/some/dir":
            return "directory", b'["out.txt"]', 0o755
        if path == "/some/dir/out.txt":
            return "file", b"bye", 0o644
        raise AssertionError(f"unexpected pull_file path: {path}")

    with_env.pull_file.side_effect = _pull

    req = plugin_pb2.CopyOutRequest(environment_id="job-1", src_path="/some/dir")
    chunks = list(service.CopyOut(req, context))

    tar_bytes = b"".join(c.data for c in chunks)
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
        names = tar.getnames()
        member = tar.extractfile("dir/out.txt")
        assert member is not None
        assert member.read() == b"bye"
    assert "dir/out.txt" in names
