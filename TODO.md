# TODO

Working list of features, refactors, and fixes for this branch.
Ordered roughly by priority: ship the simplest thing that lets us
dogfood against a real Forgejo runner, then pile features up one at a
time. Each item should land as its own atomic, sign-off commit;
`nox -s fmt` must be a no-op after each.

The sections below mix scope: `Backend options` is workflow-author
`backend_options` knobs, `Protocol coverage` is `plugin.v1alpha`
fields the server currently drops on the floor, `Runtime /
correctness` is behavioural fixes, and `Operational` is CLI flags for
the operator. Everything under `Nice-to-have` is pure planning —
nothing yet.

## Protocol coverage (unhandled `plugin.v1alpha` fields)

One bullet per proto field that the server currently drops on the
floor. Each lands as its own atomic commit that wires the field into
the LXD REST call it maps to (or documents why it's a no-op on LXD).

- [ ] `CreateRequest.cap_add`: advisory Linux capability *additions*.
      Translate to `security.privileged` / `raw.lxc lxc.cap.keep`
      entries on the instance config; entries the LXD kernel refuses
      abort `INVALID_ARGUMENT`.
- [ ] `CreateRequest.cap_drop`: advisory Linux capability *drops*.
      Translate to `raw.lxc lxc.cap.drop` entries; symmetric handling
      to `cap_add`.
- [ ] `ExecRequest.user` (name form): `Exec` currently accepts only
      numeric UIDs and rejects names with `INVALID_ARGUMENT`. Resolve
      names via `getent passwd <name>` inside the instance (or
      `/etc/passwd` scrape) and pass the UID into `exec_stream`.
- [ ] Honour `CreateRequest.environment_timeout` with a plugin-side
      cap (`--max-environment-timeout SECONDS`, default 0 = disabled).
      `run_operation` already takes a `timeout` — thread the capped
      value through the Create path. On timeout: best-effort delete
      the half-created instance and abort `DEADLINE_EXCEEDED`.
      `Duration.ToSeconds()` truncates to int — use
      `ToNanoseconds() / 1e9`.

## Runtime / correctness

- [ ] Map LXD HTTP errors to gRPC status codes so the runner's retry
      logic distinguishes "operator misconfigured" from "LXD is
      broken": 400/409/422 → `INVALID_ARGUMENT`, 404 → `NOT_FOUND`,
      403 → `PERMISSION_DENIED`, everything else → `INTERNAL`. Apply
      at every `context.abort` site.

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
- [ ] Config file support (`--config-file`), flat single-profile form:
      name, address, workers, instance-name-prefix, plus ONE connection
      profile (endpoint + `client-cert`/`client-key` paths + default
      project). CLI flags override file values. Concrete reason: keep the
      key path and endpoint in one root-owned 0600 file instead of an
      EnvironmentFile flag-string DSL. `--trust-password` stays OUT of
      this file — feed it via systemd `LoadCredential=` for one-time
      trust bootstrap, never a long-lived daemon secret.
- [ ] `--config-file` nested `remotes:` section — the schema extension
      that lands together with the remote catalog (above), not before.

## Nice-to-have

- [ ] Prometheus metrics endpoint: RPC counts, latencies, active env
      count, LXD health status, per-remote reachability.
- [ ] Structured logging (JSON) behind a `--log-format` flag.

## Deferred / rejected (kept here so we don't relitigate)

- One named backend instance = ONE connection profile (endpoint, certs,
  auth). Serving multiple runner labels from one daemon is fine when the
  labels differ only in instance SCOPE/SHAPE (project, profiles,
  ephemeral, image alias) — that already works, carried per-instance on
  the `_Env` record so every post-Create RPC routes correctly. It is
  NOT allowed to span differing CONNECTION profiles: the label scheme
  (`<name>`) already routes per-daemon, so multiple connection profiles =
  multiple `--name` daemon processes (own socket, own Client, own health
  signal, no cross-tenant blast radius). The single sanctioned exception
  is the remote-catalog item above: operator-named remotes, referenced by
  *name only* from backend_options, backed by a multi-Client cache keyed
  on `(remote_name, project_name)` — never raw URLs/certs from workflow
  authors.
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
