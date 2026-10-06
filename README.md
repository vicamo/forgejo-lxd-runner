# forgejo-lxd-runner

[![ci](https://github.com/vicamo/forgejo-lxd-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/vicamo/forgejo-lxd-runner/actions/workflows/ci.yml)

A [Forgejo Runner](https://code.forgejo.org/forgejo/runner) act backend plugin
that runs CI jobs inside [LXD](https://linuxcontainers.org/lxd/) instances.

It implements the experimental `plugin.v1alpha.BackendPlugin` gRPC service
introduced in Forgejo Runner [v13.1.0](https://forgejo.org/2026-09-runner-release-v131/), so the runner can drive the LXD instance
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
nox -s e2e         # pytest against a live LXD or Incus daemon
nox -l             # list all sessions
```

## Run

```sh
forgejo-lxd-runner --address unix:///run/forgejo-lxd-runner.sock
```

Then point the runner at it via its plugin configuration. See
[`docs/usage.md`](docs/usage.md) for every command-line flag, the per-label
backend options, and the systemd deployment walkthrough.

## Layout

```
proto/                  vendored .proto files
src/forgejo_lxd_runner/  package sources
  proto/                 generated gRPC stubs (git-ignored)
  client.py              minimal REST client for the LXD / Incus ``/1.0`` API
  executor.py            job instance execution context
  health.py              health-check gRPC service reflecting LXD reachability
  server.py              BackendPlugin service implementation
  __main__.py            CLI entry point
tests/                   pytest suite
```

## License

GPL-3.0-or-later. The vendored `.proto` file is MIT-licensed by its upstream
authors.
