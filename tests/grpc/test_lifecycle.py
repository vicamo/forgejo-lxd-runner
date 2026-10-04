"""Full Create → Start → Remove lifecycle over a real gRPC channel.

The service is bound to a real unix socket in a tmp dir and driven by a
real gRPC stub; only the BackendClient is mocked. This catches wiring
bugs (missing handlers, wrong streaming shape, health service
registration) that pure unit tests miss.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc


def _instance_meta_resp() -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"metadata": {"expanded_config": {}}}
    return resp


def test_capabilities_over_grpc(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
) -> None:
    resp = plugin_stub.Capabilities(plugin_pb2.CapabilitiesRequest())
    assert resp.name == "lxd"


def test_create_start_remove_lifecycle(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    mock_backend_client: MagicMock,
) -> None:
    # Create launches the instance already running; Start only reads the
    # state back to confirm it (RUNNING status_code); Remove delegates the
    # whole state/stop/delete dance to BackendClient.remove_instance.
    mock_backend_client.get_instance_state.return_value = {"status_code": 103}
    mock_backend_client.call.return_value = {"expanded_config": {}}
    mock_backend_client.exec_capture.return_value = (0, "PATH=/usr/bin\n", "")

    create_resp = plugin_stub.Create(
        plugin_pb2.CreateRequest(name="job-1", label_arg="ubuntu:24.04")
    )
    assert create_resp.environment_id == "job-1"

    outs = list(plugin_stub.Start(plugin_pb2.StartRequest(environment_id="job-1")))
    assert len(outs) == 1
    assert outs[0].WhichOneof("Output") == "start_complete"

    plugin_stub.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"))

    mock_backend_client.launch_instance.assert_called_once()
    # Create asked for a booted instance in the single launch call rather
    # than launching stopped and starting it afterwards.
    config = mock_backend_client.launch_instance.call_args.args[0]
    assert config["start"] is True
    mock_backend_client.remove_instance.assert_called_once_with("job-1", project=None)
