"""Unit tests for image_env extraction on Start."""

from __future__ import annotations

from unittest.mock import MagicMock

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import (
    _LXD_STATUS_RUNNING,
    _image_env_from_instance,
)


def test_image_env_extracts_environment_prefixed_keys() -> None:
    inst = MagicMock()
    inst.expanded_config = {
        "environment.PATH": "/usr/local/bin:/usr/bin",
        "environment.LANG": "C.UTF-8",
        "limits.cpu": "2",
        "security.nesting": "true",
        "user.something": "ignored",
    }
    assert _image_env_from_instance(inst) == {
        "PATH": "/usr/local/bin:/usr/bin",
        "LANG": "C.UTF-8",
    }


def test_image_env_empty_when_no_matches() -> None:
    inst = MagicMock()
    inst.expanded_config = {"limits.cpu": "2"}
    assert _image_env_from_instance(inst) == {}


def test_image_env_handles_missing_expanded_config() -> None:
    inst = MagicMock(spec=[])  # no attributes at all
    assert _image_env_from_instance(inst) == {}


def test_image_env_ignores_bare_prefix_key() -> None:
    inst = MagicMock()
    inst.expanded_config = {"environment.": "bogus"}
    assert _image_env_from_instance(inst) == {}


def test_image_env_stringifies_non_string_values() -> None:
    inst = MagicMock()
    inst.expanded_config = {"environment.PORT": 8080}
    assert _image_env_from_instance(inst) == {"PORT": "8080"}


def _service_with_instance(instance):
    from forgejo_lxd_runner.server import BackendPluginService, _Env

    service = BackendPluginService.__new__(BackendPluginService)
    # Bypass __init__ — we only need the bits Start touches.
    service._lock = __import__("threading").Lock()
    service._envs = {"env-1": _Env(instance_name="env-1")}
    client = MagicMock()
    client.instances.get.return_value = instance
    service._clients = {None: client}
    return service


def test_start_populates_image_env_in_start_complete() -> None:
    inst = MagicMock()
    inst.status_code = _LXD_STATUS_RUNNING
    inst.expanded_config = {
        "environment.FOO": "bar",
        "environment.BAZ": "qux",
        "limits.memory": "512MB",
    }
    service = _service_with_instance(inst)

    request = plugin_pb2.StartRequest(environment_id="env-1")
    outputs = list(service.Start(request, context=MagicMock()))

    assert len(outputs) == 1
    complete = outputs[0].start_complete
    assert dict(complete.image_env) == {"FOO": "bar", "BAZ": "qux"}
