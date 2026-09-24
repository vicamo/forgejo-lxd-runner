"""Full Create → Start → Remove lifecycle over a real gRPC channel.

The service is bound to a real unix socket in a tmp dir and driven by a
real gRPC stub; only pylxd is mocked. This catches wiring bugs (missing
handlers, wrong streaming shape, health service registration) that pure
unit tests miss.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc


def test_capabilities_over_grpc(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
) -> None:
    resp = plugin_stub.Capabilities(plugin_pb2.CapabilitiesRequest())
    assert resp.name == "lxd"


def test_create_start_remove_lifecycle(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.status_code = 102  # STOPPED
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created
    mock_pylxd_client.instances.get.return_value = created

    create_resp = plugin_stub.Create(
        plugin_pb2.CreateRequest(name="job-1", label_arg="ubuntu/24.04")
    )
    assert create_resp.environment_id == "job-1"

    outs = list(plugin_stub.Start(plugin_pb2.StartRequest(environment_id="job-1")))
    assert len(outs) == 1
    assert outs[0].WhichOneof("Output") == "start_complete"
    created.start.assert_called_once_with(wait=True)

    # Flip to RUNNING so Remove exercises the stop path.
    created.status_code = 103
    plugin_stub.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"))
    created.stop.assert_called_once_with(force=True, wait=True)
    created.delete.assert_called_once_with(wait=True)
