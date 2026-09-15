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
