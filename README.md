# forgejo-lxd-runner

A [Forgejo Runner](https://code.forgejo.org/forgejo/runner) act backend plugin
that runs CI jobs inside [LXD](https://linuxcontainers.org/lxd/) instances.

It implements the experimental `plugin.v1alpha.BackendPlugin` gRPC service
introduced in Forgejo Runner v13.1.0, so the runner can drive the LXD instance
lifecycle (`Create` / `Start` / `Exec` / `CopyIn` / `CopyOut` / `Remove`) over
a Unix or TCP socket.

## Status

Alpha — the upstream plugin protocol itself is `v1alpha` and may change in
incompatible ways between runner releases.

## Install

```sh
pip install .
# or, for development:
pip install -e '.[dev]'
```

## Generate the gRPC stubs

The `.proto` file is vendored in `proto/plugin/v1alpha/plugin.proto` from
Forgejo Runner v13.1.0. Regenerate the Python bindings with:

```sh
nox -s proto
```

## Development

```sh
pipx install nox   # or: pip install nox

nox                # lint + type + tests on the current interpreter
nox -s lint        # ruff check + format --check
nox -s fmt         # ruff check --fix + format (writes changes)
nox -s type        # mypy
nox -s tests       # pytest
nox -s tests-3.12  # pytest on a specific Python version
nox -l             # list all sessions
```

## Run

```sh
forgejo-lxd-runner --address unix:///run/forgejo-lxd-runner.sock
```

Then point the runner at it via its plugin configuration.

## Deploy (systemd)

The runner never spawns this plugin — it dials the socket the plugin
listens on. So a deployment is: run one daemon process per backend name,
and point the runner's config at each socket.

A template unit is provided in
[`packaging/systemd/forgejo-lxd-runner@.service`](packaging/systemd/forgejo-lxd-runner@.service).
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

## Layout

```
proto/                  vendored .proto files
src/forgejo_lxd_runner/  package sources
  proto/                 generated gRPC stubs (git-ignored)
  server.py              BackendPlugin service implementation
  __main__.py            CLI entry point
tests/                   pytest suite
```

## License

GPL-3.0-or-later. The vendored `.proto` file is MIT-licensed by its upstream
authors.
