"""BackendPlugin gRPC service — LXD-backed implementation.

Skeleton only — each RPC currently returns UNIMPLEMENTED. Fill in the LXD-backed
behaviour incrementally. The generated stubs live under
``forgejo_lxd_runner.proto.plugin.v1alpha`` after ``make proto``.
"""

from __future__ import annotations

from collections.abc import Iterator

import grpc

from .proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc


class BackendPluginService(plugin_pb2_grpc.BackendPluginServicer):
    """LXD-backed implementation of ``plugin.v1alpha.BackendPlugin``."""

    name = "lxd"

    def Capabilities(  # noqa: N802 — gRPC method name
        self,
        request: plugin_pb2.CapabilitiesRequest,
        context: grpc.ServicerContext,
    ) -> plugin_pb2.CapabilitiesResponse:
        return plugin_pb2.CapabilitiesResponse(name=self.name)

    def Create(  # noqa: N802
        self, request: plugin_pb2.CreateRequest, context: grpc.ServicerContext
    ) -> plugin_pb2.CreateResponse:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Create not implemented")

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
