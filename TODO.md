# TODO

Working list of features, refactors, and fixes for this branch.
Ordered roughly by priority: ship the simplest thing that lets us
dogfood against a real Forgejo runner, then pile features up one at a
time. Each item should land as its own atomic, sign-off commit;
`nox -s fmt` must be a no-op after each.

The first two sections (backend options, runtime/correctness,
operational) mirror the 10 commits already living on `master` past
this branch's base; they are re-listed here so this branch tracks the
same scope. The later sections are pure planning — nothing yet.

## Backend options (workflow-author-facing knobs on `CreateRequest`)

- [ ] `lxd_arch`: LXD-native architecture name (`x86_64`, `aarch64`, …)
      forwarded verbatim to the instance config so cluster placement
      picks a matching node.
- [ ] Derive `RUNNER_OS` from the LXD image metadata so the standard
      GHA-compatible `RUNNER_OS` / `RUNNER_ARCH` environment variables
      are set correctly inside the instance without the workflow
      author naming them.
- [ ] `project`: run the instance under a named LXD project (per-tenant
      quotas, network isolation, ACLs). Requires a per-project pylxd
      client cache — `pylxd.Client(project=…)` binds the project at
      construction time, so we need one Client per project seen.
- [ ] `profiles`: comma-separated LXD profile names applied to the
      instance. Whitespace-stripped, empty entries dropped; when the
      key is absent or effectively empty, LXD applies its own
      `default` profile — do not synthesise `["default"]` ourselves.
- [ ] `type`: forward `container` or `virtual-machine` verbatim to
      `config["type"]`. Reject anything else with `INVALID_ARGUMENT`.
- [ ] `ephemeral`: parse the usual truthy/falsy strings
      (`true/1/yes/on` and `false/0/no/off`, case + whitespace
      insensitive) into `config["ephemeral"]`. Absent → LXD default.
      Invalid → `INVALID_ARGUMENT`.
- [ ] `image_env`: forward workflow-author-supplied environment
      variables into the LXD instance config (`environment.<KEY>` or
      the `config` map). Complements the existing image alias handling
      from `label_arg`.

## Runtime / correctness

- [ ] Avoid holding `self._lock` across the blocking
      `pylxd.Client(project=…)` construction. Double-checked locking
      in `_client_for`: fast-path lock-free dict read, take the lock
      only on cache miss, re-check under the lock.
- [ ] Map LXD HTTP errors to gRPC status codes so the runner's retry
      logic distinguishes "operator misconfigured" from "LXD is
      broken": 400/409/422 → `INVALID_ARGUMENT`, 404 → `NOT_FOUND`,
      403 → `PERMISSION_DENIED`, everything else → `INTERNAL`. Apply
      at every `context.abort` site.
- [ ] Honour `CreateRequest.environment_timeout` with a plugin-side
      cap (`--max-environment-timeout SECONDS`, default 0 = disabled).
      pylxd's `wait_for_operation` has no timeout parameter, so wrap
      the blocking `instances.create` in a single-thread
      `ThreadPoolExecutor` and call `future.result(timeout=…)`. On
      timeout: best-effort delete the half-created instance and abort
      `DEADLINE_EXCEEDED`. `Duration.ToSeconds()` truncates to int —
      use `ToNanoseconds() / 1e9`.
- [ ] `_envs` leak audit: every RPC path that removes an instance must
      also remove its `_envs` entry, and `Remove` must be idempotent
      on already-gone entries. Add a stress test that Creates + Removes
      thousands of environments and asserts `len(service._envs) == 0`.
- [ ] Signal-handler shutdown polish: current `_handle` in
      `__main__.serve` mixes `stop.set()` and `server.stop(grace=5)`;
      simplify to one path (stop checker → `server.stop(grace=…).wait()`).

## Operational (operator-facing CLI flags)

- [ ] `--instance-name-prefix STR` (default `""`): prepend to
      `config["name"]` for the LXD instance. `environment_id` (the
      runner's handle) stays raw; only the on-LXD instance name is
      prefixed. No auto-generated default — random breaks restart
      cleanup, hostname is often DNS-unsafe, PIDs recycle. Empty
      default; operator sets it in the systemd unit if they want it.
- [ ] `--cluster-target` CLI default for LXD cluster deployments;
      optional per-label override mechanism TBD.
- [ ] `--remote NAME=URL[,cert=...,key=...]` (repeatable) operator-defined
      remote catalog. Backend options reference a remote by *name only*
      — never raw URLs from workflow authors. Depends on multi-client
      cache keyed on `(remote_name, project_name)`.
- [ ] Remote LXD connection flags: `--endpoint`, `--client-cert`,
      `--client-key`, `--trust-password` for the default remote. Until
      these land, the plugin only talks to the local unix socket.
- [ ] Config file support (`--config-file`): only if CLI flags grow
      unwieldy. Defer until we have a concrete reason.

## Runtime behaviour (planning)

- [ ] Streaming `Exec`: today the RPC blocks until the command exits
      and returns a single response. Move to the streaming variant so
      log lines reach the runner as they're produced; needed for
      long-running steps to show progress in the Forgejo UI.

## Nice-to-have

- [ ] Prometheus metrics endpoint: RPC counts, latencies, active env
      count, LXD health status, per-remote reachability.
- [ ] Per-remote health status: when the remote catalog lands, poll
      each remote and expose per-remote status via
      `Check{service="lxd:<remote-name>"}`.
- [ ] Structured logging (JSON) behind a `--log-format` flag.

## Deferred / rejected (kept here so we don't relitigate)

- Passing raw LXD config keys through `backend_options` — covered by
  LXD profiles; workflow authors shouldn't need LXD vocabulary.
- Auto-generated `--instance-name-prefix` default (random / hostname /
  PID) — breaks restart-time cleanup, hides collisions. Empty default,
  operator sets in the systemd unit.
- Pre-validating `project` / `profiles` existence before Create — race
  condition + extra round-trips + LXD's own error is already
  descriptive. `_lxd_error_to_grpc` handles the mapping.
- Embedding LXD health probing inside the `Capabilities` RPC — replaced
  by the background poller on `grpc.health.v1.Health`.
- Making connection info (endpoint, certs) a `backend_option` — those
  are operator-authoritative, not workflow-author-authoritative. Rule
  of thumb: workflow authors never need to know they're on LXD.
