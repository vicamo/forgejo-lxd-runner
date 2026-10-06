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
| `profiles` | comma-separated profile names | daemon `default` profile | LXD/Incus profiles to apply to the instance. An empty string is treated as absence (the daemon applies `default`), not as "no profiles". |
| `type` | `container` or `virtual-machine` | `container` | Instance type the daemon builds. The value is forwarded verbatim, so an unknown value yields the daemon's own error rather than a guess here. |
| `ephemeral` | `true`/`1`/`yes`/`on` or `false`/`0`/`no`/`off` (case-insensitive) | daemon default (non-ephemeral) | When truthy, the daemon deletes the instance as soon as it stops — a backstop for `Remove` if the instance is stopped out-of-band. A non-boolean value is rejected with `INVALID_ARGUMENT`. |
<!-- END GENERATED: backend-options -->

Example label wiring in the runner's `config.yaml`:

```yaml
runner:
  labels:
    # ubuntu-minimal:24.04 image, built as a VM in the "ci" LXD project
    - ubuntu-vm:lxd://ubuntu-minimal:24.04#type=virtual-machine,project=ci
```

(How each label string encodes its `backend_options` is defined by the runner,
not by this plugin; consult the Forgejo Runner documentation for the exact
syntax your runner version expects.)

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
