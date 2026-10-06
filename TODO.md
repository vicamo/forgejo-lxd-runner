# TODO

Working list of features, refactors, and fixes for this branch.
Ordered roughly by priority: ship the simplest thing that lets us
dogfood against a real Forgejo runner, then pile features up one at a
time. Each item should land as its own atomic, sign-off commit;
`nox -s fmt` must be a no-op after each.

The sections below mix scope: `Operational` is CLI flags for the
operator. Everything under `Nice-to-have` is pure planning — nothing yet.

## Operational (operator-facing CLI flags)

- [ ] `--cluster-target` CLI default for LXD cluster deployments;
      optional per-label override mechanism TBD.
- [ ] Config file support (`--config-file`), flat single-profile form:
      name, address, workers, instance-name-prefix, plus ONE connection
      profile (endpoint + `client-cert`/`client-key` paths + default
      project). CLI flags override file values. Concrete reason: keep the
      key path and endpoint in one root-owned 0600 file instead of an
      EnvironmentFile flag-string DSL. Any one-time trust-bootstrap
      credential stays OUT of this file — feed it via systemd
      `LoadCredential=`, never a long-lived daemon secret.

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
  is the remote-catalog item below: operator-named remotes, referenced by
  *name only* from backend_options, backed by a multi-Client cache keyed
  on `(remote_name, project_name)` — never raw URLs/certs from workflow
  authors.
- In-process remote catalog (`--remote NAME=URL[,cert=,key=]` repeatable,
  plus a `--config-file` nested `remotes:` section and a multi-Client
  cache keyed on `(remote_name, project_name)`). Rejected for now:
  one-daemon-per-connection-profile already covers N remotes — spin up a
  second `--name` process per remote, routed by the label scheme. The
  catalog only earns its keep if ONE process must span many remotes and
  per-remote daemons become too costly (dozens of remotes, or dynamic
  remote sets); revisit only then. Backend options would reference a
  remote by *name only*, never raw URLs from workflow authors.
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
