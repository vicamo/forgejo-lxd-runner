"""BackendPlugin gRPC service — LXD-backed implementation.

Commit 1 (MVP): the plugin respects only ``CreateRequest.label_arg`` (the
LXD image reference) and lets pylxd / LXD supply every other default —
project, profile, user, network, storage. Later commits add options one
at a time.

Known simplifications, all called out in code:

* ``CreateResponse`` reports a hardcoded filesystem layout and
  ``os=linux`` / ``arch=amd64``. Discovery from the running instance is
  a later commit.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator

import grpc
import pylxd
from pylxd.exceptions import LXDAPIException

from .proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc

log = logging.getLogger(__name__)


class _Env:
    """Per-environment state tracked by the plugin."""

    __slots__ = ("instance_name",)

    def __init__(self, instance_name: str) -> None:
        self.instance_name = instance_name


class BackendPluginService(plugin_pb2_grpc.BackendPluginServicer):
    """LXD-backed implementation of ``plugin.v1alpha.BackendPlugin``."""

    name = "lxd"
    """Wire-protocol backend name returned by ``Capabilities``.

    Must match the plugin's scheme in the runner's ``plugins:`` config —
    labels like ``mylabel:lxd://<image>`` are routed to this backend.
    Subclasses may override to reuse this service under a different scheme.
    """

    def __init__(self) -> None:
        # No endpoint / cert args yet: pylxd auto-detects the local socket
        # and lands in the ``default`` project.
        self._client = pylxd.Client()
        self._envs: dict[str, _Env] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # BackendPlugin RPCs
    # ------------------------------------------------------------------

    def Capabilities(  # noqa: N802 — gRPC method name
        self,
        request: plugin_pb2.CapabilitiesRequest,
        context: grpc.ServicerContext,
    ) -> plugin_pb2.CapabilitiesResponse:
        return plugin_pb2.CapabilitiesResponse(name=self.name)

    def Create(  # noqa: N802
        self,
        request: plugin_pb2.CreateRequest,
        context: grpc.ServicerContext,
    ) -> plugin_pb2.CreateResponse:
        if not request.label_arg:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "label_arg is required (set image via `mylabel:lxd://<image>`)",
            )

        image = request.label_arg
        # ``CreateRequest.name`` is the runner-assigned per-job handle; reuse
        # it verbatim so LXD instance name == environment_id.
        name = request.name
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "name is required")

        config = {
            "name": name,
            "source": {"type": "image", "alias": image},
        }
        try:
            instance = self._client.instances.create(config, wait=True)
        except LXDAPIException as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd create {image!r}: {exc}")
            raise AssertionError("unreachable") from exc

        with self._lock:
            self._envs[name] = _Env(instance_name=instance.name)

        log.info("created environment %s from image %s", name, image)

        # TODO: discover os/arch and expose a knob for the paths.
        return plugin_pb2.CreateResponse(
            environment_id=name,
            root_path="/root/actions-runner",
            act_path="/root/actions-runner/act",
            tool_cache_path="/opt/hostedtoolcache",
            temp_path="/tmp",
            os="linux",
            arch="amd64",
        )

    def Start(  # noqa: N802
        self, request: plugin_pb2.StartRequest, context: grpc.ServicerContext
    ) -> Iterator[plugin_pb2.StartOutput]:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Start not implemented")

    def Exec(  # noqa: N802
        self, request: plugin_pb2.ExecRequest, context: grpc.ServicerContext
    ) -> Iterator[plugin_pb2.ExecOutput]:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Exec not implemented")

    def CopyIn(  # noqa: N802
        self, request_iterator: Iterator[plugin_pb2.CopyInChunk], context: grpc.ServicerContext
    ) -> plugin_pb2.CopyInResponse:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "CopyIn not implemented")

    def CopyOut(  # noqa: N802
        self, request: plugin_pb2.CopyOutRequest, context: grpc.ServicerContext
    ) -> Iterator[plugin_pb2.CopyOutChunk]:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "CopyOut not implemented")

    def Remove(  # noqa: N802
        self, request: plugin_pb2.RemoveRequest, context: grpc.ServicerContext
    ) -> plugin_pb2.RemoveResponse:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Remove not implemented")
