# Usage

Command-line flags, per-label backend options, and runtime deployment for
`forgejo-lxd-runner`. The CLI block and the backend-options table below are
checked against the code in CI (`tests/unit/test_usage_docs.py`), so they
cannot drift from what the program actually accepts.

## Command-line options

`forgejo-lxd-runner` is the long-running plugin daemon. The runner dials the
socket it listens on; these flags configure that daemon. One process serves one
backend `--name`; run several for several backends (see *Deployment* below).

<!-- BEGIN GENERATED: cli-help -->
```console
$ forgejo-lxd-runner --help
usage: forgejo-lxd-runner [-h] [--name NAME] [--address ADDRESS]
                          [--workers WORKERS]
                          [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
                          [--health-check-interval HEALTH_CHECK_INTERVAL]
                          [--max-environment-timeout MAX_ENVIRONMENT_TIMEOUT]
                          [--instance-name-prefix INSTANCE_NAME_PREFIX]
                          [--endpoint ENDPOINT] [--client-cert CLIENT_CERT]
                          [--client-key CLIENT_KEY]
                          [--tls-server-cert TLS_SERVER_CERT]
                          [--metrics-address METRICS_ADDRESS]
                          [--cluster-target CLUSTER_TARGET]

options:
  -h, --help            show this help message and exit
  --name NAME           Backend name returned by Capabilities and referenced
                        by the runner's label scheme (<label>:<name>://<arg>).
                        Change it when running multiple plugin processes with
                        different connection settings so each is addressable
                        independently. Default: lxd.
  --address ADDRESS     gRPC bind address (e.g. unix:///path/to.sock or
                        127.0.0.1:50051).
  --workers WORKERS     Thread-pool size.
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Root logger level (case-insensitive). Default: INFO.
  --health-check-interval HEALTH_CHECK_INTERVAL
                        Seconds between LXD health probes. The result is
                        reflected into the standard grpc.health.v1 status. 0
                        disables the poller.
  --max-environment-timeout MAX_ENVIRONMENT_TIMEOUT
                        Upper bound (seconds) on how long Create will wait for
                        LXD to provision an instance. 0 disables the cap; the
                        runner-supplied environment_timeout is honoured as-is.
                        When both are set the smaller wins.
  --instance-name-prefix INSTANCE_NAME_PREFIX
                        String prepended to every LXD instance name. The
                        runner's environment_id is unchanged; only the LXD-
                        side name is namespaced. Default empty (1:1 mapping).
                        Set this when multiple daemons share one LXD project.
  --endpoint ENDPOINT   Remote LXD/Incus HTTPS endpoint (https://host:port).
                        When set, the daemon is reached over mutual TLS using
                        --client-cert and --client-key instead of a local Unix
                        socket. Default: autodetect the local socket. The
                        daemon's server certificate is verified against the
                        system trust store.
  --client-cert CLIENT_CERT
                        Path to the PEM client certificate for mutual-TLS auth
                        against --endpoint. Required with --endpoint.
  --client-key CLIENT_KEY
                        Path to the PEM client private key for mutual-TLS auth
                        against --endpoint. Required with --endpoint.
  --tls-server-cert TLS_SERVER_CERT
                        Path to a PEM certificate used to verify the remote
                        daemon's server certificate (pin a self-signed cert —
                        the LXD/Incus default). Only meaningful with
                        --endpoint. Default: verify against the system trust
                        store.
  --metrics-address METRICS_ADDRESS
                        Enable a Prometheus metrics HTTP endpoint on this
                        host:port (e.g. 127.0.0.1:9095). A bare :port binds
                        loopback only. Default: disabled. Requires the
                        optional prometheus-client dependency.
  --cluster-target CLUSTER_TARGET
                        Default cluster member to place instances on (?target=
                        at create time) when the daemon is clustered. A per-
                        label 'cluster-target' backend option overrides this.
                        Default: let the cluster schedule.
```
<!-- END GENERATED: cli-help -->

## Backend options

Backend options are per-label `key=value` settings the runner forwards to
`Create` in the label's `backend_options` map. They are set in the runner's
`config.yaml` on the label, not on this daemon's command line, so different
labels pointing at the same daemon can request different projects, profiles, or
instance types.

<!-- BEGIN GENERATED: backend-options -->
| Option | Values | Default | Description |
| ------ | ------ | ------- | ----------- |
| `project` | project name | daemon `default` project | LXD/Incus project the instance, its network, and everything this environment owns are created in (per-tenant quotas, ACLs, isolation). |
| `cluster-target` | cluster member name | the daemon's `--cluster-target`, else the cluster schedules | Pin this label's instances onto a named cluster member, overriding the daemon-wide `--cluster-target`. Placement only, applied at create time. |
| `system-ready` | shell command or `builtin:` preset | empty (no wait) | Poll inside the instance until the command succeeds, before detecting the runtime or starting a job. For cloud-init profiles, use `builtin:cloud-init`; see readiness presets below. |
| `system-ready-timeout` | positive finite seconds | `600` | Readiness polling budget. An unsuccessful command fails Create with `FAILED_PRECONDITION` and cleans up the instance. Individual command execution must also terminate for polling to enforce this budget. |
| `profiles` | comma-separated profile names | daemon `default` profile | LXD/Incus profiles to apply to the instance. An empty string is treated as absence (the daemon applies `default`), not as "no profiles". |
| `type` | `container` or `virtual-machine` | `container` | Instance type the daemon builds. The value is forwarded verbatim, so an unknown value yields the daemon's own error rather than a guess here. |
| `ephemeral` | `true`/`1`/`yes`/`on` or `false`/`0`/`no`/`off` (case-insensitive) | daemon default (non-ephemeral) | When truthy, the daemon deletes the instance as soon as it stops — a backstop for `Remove` if the instance is stopped out-of-band. A non-boolean value is rejected with `INVALID_ARGUMENT`. |
<!-- END GENERATED: backend-options -->

Example label wiring in the runner's `config.yaml`. Backend options are the
keys under a label's `backend-options:` mapping; the runner passes them to this
plugin verbatim as the `backend_options` map:

```yaml
runner:
  labels:
    ubuntu-vm:
      # ubuntu-minimal:24.04 image, built as a VM in the "ci" LXD project
      backend: lxd
      backend-options:
        image: ubuntu-minimal:24.04
        type: virtual-machine
        project: ci
```

(The label-to-`backend_options` encoding is defined by the runner, not by this
plugin. The mapping form above is the unambiguous one; older runners also accept
a string label with options as a `?key=value` query string. Consult the Forgejo
Runner documentation for what your version supports.)

## System readiness presets

Set `system-ready` to a preset name or a custom shell command. Readiness is
checked after the instance agent answers and before runtime detection.

| Preset | Ready condition |
| --- | --- |
| `builtin:cloud-init` | Runs `cloud-init status --wait --long`; accepts exit 0 or 2. Exit 2 logs recoverable errors as a warning. |
| `builtin:cloud-init-strict` | Runs `cloud-init status --wait --long`; accepts only exit 0. |
| `builtin:systemd` | Runs `systemctl is-system-running --wait`; accepts `running` or `degraded`. Degraded boot logs a warning with failed-unit diagnostics. |
| `builtin:systemd-strict` | Runs `systemctl is-system-running --wait`; accepts only `running` with exit 0. |

For example, a label's backend-options mapping can contain:

```yaml
system-ready: builtin:cloud-init
system-ready-timeout: "600"
```

Presets describe the provisioning mechanism, so the cloud-init preset works
for Fedora and Ubuntu images that include cloud-init. A missing required tool
fails readiness; it is not treated as an image that needs no wait. Unknown
`builtin:` names are rejected with `INVALID_ARGUMENT` before allocating resources.

Custom shell commands require exit 0. To require cloud-init to finish without
recoverable errors, use `builtin:cloud-init-strict` or `cloud-init status --wait`
directly. An absent or empty
value skips system readiness. Runtime detection still checks the container
runtime after system readiness succeeds.

The timeout bounds polling between completed commands; an individual blocking
command must terminate for the budget to be enforced. Failure reports the last
exit code and stderr or stdout, and Create cleans up the instance and network.

### VM guest agent readiness

Create waits up to 300 seconds for the guest agent, polling every 2 seconds,
before checking system readiness. This wait is separate from
`system-ready-timeout`. If the agent does not connect, the runner logs the last
instance state and tries to retrieve the console log before removing the
instance. Console retrieval has a 5-second HTTP timeout; the last 16,384
characters are logged when the daemon supports VM console logs. Retrieval
failures are logged without changing the agent-readiness error.

### Cloud-init user-data missing in virtual machines

For VM cloud images, cloud-init can run before the guest agent creates
`/dev/lxd/sock`. If it falls back to `DataSourceNone`, profile user-data is
not applied even though provisioning reports completion. Load
[`examples/profiles/vm.yaml`](../examples/profiles/vm.yaml) to provide a
NoCloud configuration disk available from boot:

```sh
lxc profile create vm < examples/profiles/vm.yaml
# Or, with Incus:
incus profile create vm < examples/profiles/vm.yaml
```

Then include it alongside the default and runtime profiles:

```yaml
type: virtual-machine
profiles: default,vm,base,docker
system-ready: builtin:cloud-init
```

Use a cloud-init-enabled VM image. The VM profile supplies only the
configuration drive; it does not install cloud-init, the guest agent, or a
runtime. Both LXD and Incus support this disk source; it cannot be applied
to container instances.

## Metrics

The daemon can expose a Prometheus metrics endpoint for operator monitoring.
It is disabled by default; pass `--metrics-address host:port` to enable it. A
bare `:port` (or a host of `127.0.0.1`) binds loopback only — metrics are
operator-facing, so widen the bind explicitly only when a remote Prometheus
must reach it. The endpoint requires the optional `prometheus-client`
dependency (`pip install forgejo-lxd-runner[metrics]`).

```
forgejo-lxd-runner --metrics-address 127.0.0.1:9095
curl -s http://127.0.0.1:9095/metrics
```

Exposed series (plus the default `prometheus_client` process/platform metrics):

| Metric | Type | Meaning |
| --- | --- | --- |
| `forgejo_lxd_runner_rpc_requests_total{method,code}` | counter | gRPC RPCs handled, labelled by method name and resulting status code (`OK` or a gRPC code such as `NOT_FOUND`). |
| `forgejo_lxd_runner_lxd_reachable` | gauge | `1` if the last LXD health probe succeeded, `0` otherwise. Updated on the `--health-check-interval` cadence. |
| `forgejo_lxd_runner_active_environments` | gauge | Environments currently tracked by the backend, evaluated at scrape time. |

## Deployment (systemd)

The runner never spawns this plugin — it dials the socket the plugin
listens on. So a deployment is: run one daemon process per backend name,
and point the runner's config at each socket.

A template unit is provided in
[`packaging/systemd/forgejo-lxd-runner@.service`](../packaging/systemd/forgejo-lxd-runner@.service).
The instance name (`%i`) is used as both the backend `--name` and the
socket basename, so one unit serves any number of independently-named
backends.

1. Install the package system-wide so `forgejo-lxd-runner` is on `PATH`
   (distro package, or `sudo pip install .`).

2. Install and start the unit:

   ```sh
   sudo cp packaging/systemd/forgejo-lxd-runner@.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now forgejo-lxd-runner@lxd
   ```

   This serves a backend named `lxd` on
   `/run/forgejo-lxd-runner/lxd.sock`. The service runs under a
   `DynamicUser` with `SupplementaryGroups=lxd incus-admin` for socket
   access and `RuntimeDirectory=` for an owned `/run` directory — no
   manual user or `chmod` needed.

3. Customise flags with a drop-in rather than editing the unit (extra
   arguments, a different socket, a remote mutual-TLS endpoint, a cluster
   target):

   ```sh
   sudo systemctl edit forgejo-lxd-runner@lxd
   ```
   ```ini
   [Service]
   ExecStart=
   ExecStart=/usr/bin/forgejo-lxd-runner --name lxd \
       --address unix:///run/forgejo-lxd-runner/lxd.sock \
       --instance-name-prefix "runner-lxd-"
   ```

   (The empty `ExecStart=` line clears the unit's default before the
   replacement.)

4. Point the runner at the socket in its `config.yaml`:

   ```yaml
   plugins:
     lxd:
       address: unix:///run/forgejo-lxd-runner/lxd.sock
   runner:
     labels:
       - ubuntu-lxd:lxd://ubuntu-minimal:24.04
   ```

Serve additional backends (e.g. a separate remote daemon) by enabling
another instance — `forgejo-lxd-runner@lxd-remote` — with its own
drop-in and its own `plugins:` block.
