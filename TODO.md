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

- [ ] `CreateRequest.image`: the workflow's optional `container:` block
      — the job runs *inside that container image*, on the instance
      `label_arg` selected. The two are orthogonal, not alternatives:
      `label_arg` picks the machine, `image` optionally containerises
      the job on it, and `image` is empty for the common
      `runs-on: lxd-ubuntu-2404` job. Settled by reading all three
      implementations:

      * GitHub's runner treats `container:` as optional and independent
        of `runs-on:`; `container == null` means "run on the host"
        (there is even a `RequireJobContainer` knob to forbid that).
      * nektos/act collapses them — `platformImage()` is
        `container.image` if set, else the `runs-on` image — because
        act has no VM layer and must resolve exactly one image.
      * Forgejo's adapter sends both unresolved (`Image: input.Image`
        from `container.image`, `LabelArg` from the `runs-on` scheme
        suffix) and lets the plugin decide.

      We follow **GitHub's model**. Act's precedence rule is explicitly
      rejected: it would reinterpret a registry reference (`node:20`)
      as an LXD image alias, which is a different namespace and would
      usually fail.

      Implementation, one commit per step:

      - [x] Create: pull the image and create an idle job container
            (`tail -f /dev/null`, matching GitHub's `ContainerEntryPoint`)
            on the instance, bind-mounting at identical paths exactly
            the three directories `CreateResponse` promises —
            `root_path`, `tool_cache_path`, `temp_path`. Identical paths
            are why `CopyIn`/`CopyOut` need no container awareness. The
            proto forwards no volume list, so this layout is ours to
            declare, not an approximation of act's Docker binds.
            `image` empty means no container: the job runs on the
            instance itself and every step below is a no-op.
      - [x] Create, not Start, owns this: `Create` is `docker create`
            and `Start` is `docker start`. Pulling in `Start` would
            report a bad image reference against the wrong RPC and
            leave `Create` claiming success for an environment that
            cannot exist.
      - [x] Start: start the created container.
      - [ ] Start: populate `StartComplete.image_env` by running `env`
            in the started container (closes the `image_env` item
            below).
      - [ ] Exec: `<runtime> exec` into the job container instead of
            running on the instance.
      - [ ] Remove: tear the container down before the instance.

      Runtime selection is autodetected by probing the instance for
      `docker` / `podman`. No backend option: GitHub Actions has no
      workflow-level counterpart, and the operator already chose by
      picking the LXD image and profiles in the Forgejo config.
      Requires `profiles: "base,docker"` (or `base,podman`) — see
      `examples/profiles/`.
- [ ] `CreateRequest.cap_add`: advisory Linux capability *additions*.
      Translate to `security.privileged` / `raw.lxc lxc.cap.keep`
      entries on the instance config; entries the LXD kernel refuses
      abort `INVALID_ARGUMENT`.
- [ ] `CreateRequest.cap_drop`: advisory Linux capability *drops*.
      Translate to `raw.lxc lxc.cap.drop` entries; symmetric handling
      to `cap_add`.
- [ ] `CreateRequest.services`: workflow `services:` sidecars. Bring
      each `ServiceContainer` up as a peer LXD instance sharing a
      user-defined network with the job instance, expose the requested
      `ports`, tear them down in `Remove`.
- [ ] `CreateResponse.os`: hard-coded to `"Linux"` today. Derive from
      `expanded_config["image.os"]` (or the image metadata) so the
      `RUNNER_OS` env var inside the job reflects the actual image.
- [ ] `CreateResponse.arch`: hard-coded to `"X64"` today. Map
      `metadata["architecture"]` through the GHA-canonical arch table
      so `RUNNER_ARCH` reflects the real instance architecture.
- [ ] `CreateResponse.path_variable_name`: leave unset on Linux (runner
      defaults to `"PATH"`); revisit only if a non-POSIX backend lands.
      Belongs in *Deferred / rejected* until then.
- [ ] `CreateResponse.default_path_variable`: leave unset; the runner's
      fallback is the job-supplied `PATH`. Deferred until a backend
      needs a specific default.
- [ ] `CreateResponse.path_separator`: leave unset on Linux (runner
      defaults to `":"`); deferred until a non-POSIX backend lands.
- [ ] `CreateResponse.environment_case_insensitive`: leave unset (=
      `false`); Linux env is case-sensitive. Deferred, same rationale.
- [ ] `StartComplete.image_env`: run `env` where the job's steps run
      and surface what it reports, so those steps see the environment
      they inherit without the author re-declaring it.
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
