# TODO

Working list of features, refactors, and fixes for this branch.
Ordered roughly by priority: ship the simplest thing that lets us
dogfood against a real Forgejo runner, then pile features up one at a
time. Each item should land as its own atomic, sign-off commit;
`nox -s fmt` must be a no-op after each.

The sections below mix scope: `Backend options` is workflow-author
`backend_options` knobs, `Runtime / correctness` is behavioural fixes,
and `Operational` is CLI flags for the operator. Everything under
`Nice-to-have` is pure planning — nothing yet.

## Runtime / correctness

- [ ] Signal-handler shutdown polish: current `_handle` in
      `__main__.serve` mixes `stop.set()` and `server.stop(grace=5)`;
      simplify to one path (stop checker → `server.stop(grace=…).wait()`).

## Backend options (workflow-author-facing knobs on `CreateRequest.backend_options`)

- [ ] `project`: run the instance under a named LXD project (per-tenant
      quotas, network isolation, ACLs). Pass `project` as a query
      parameter on each REST call; no per-project client state.
- [ ] `type`: forward `container` or `virtual-machine` verbatim to
      `config["type"]`. Reject anything else with `INVALID_ARGUMENT`.
- [ ] `ephemeral`: parse the usual truthy/falsy strings
      (`true/1/yes/on` and `false/0/no/off`, case + whitespace
      insensitive) into `config["ephemeral"]`. Absent → LXD default.
      Invalid → `INVALID_ARGUMENT`.

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
  descriptive. The error-mapping item under *Runtime / correctness*
  covers turning those into sensible gRPC codes.
- Embedding LXD health probing inside the `Capabilities` RPC — replaced
  by the background poller on `grpc.health.v1.Health`.
- Making connection info (endpoint, certs) a `backend_option` — those
  are operator-authoritative, not workflow-author-authoritative. Rule
  of thumb: workflow authors never need to know they're on LXD.
