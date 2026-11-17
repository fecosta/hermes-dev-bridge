# Hermes Dev Bridge — U5.2

The bridge accepts only the deterministic `fixture` repository and `preflight`, `status`, or `smoke` operations, plus the read-only `pfy/preflight` readiness snapshot. It rejects commands, paths, environment overrides, model overrides, and unsupported fields.

## Security behavior

Authorization is not based on a username or caller-controlled environment variable. Each request requires the effective numeric UID, GID, and exact supplementary-group set to match the root-owned JSON policy at `/etc/hermes-dev-bridge/policy.json`. The policy names the authorized UID/GID and worker workspace. The bridge source and policy must be root-owned and not group/world writable. The workspace must be a real directory (no symlinks anywhere in its path), owned by the worker UID/GID with mode `0700`; every parent directory must be root-owned and not group/world writable, so only root can swap a path component. This is checked before each run, but a check-then-use window remains if root itself is compromised; that is out of scope.

The fixture uses a fixed interpreter command, `stdin=DEVNULL`, minimal `PATH`, a new session/process group, a bounded timeout, and process-group `SIGKILL`. After every run (timeout or normal exit) the bridge scans `/proc` and requires the group to have no live members; a `killpg` error other than "no such process", or any survivor, is reported as `PROCESS_GROUP_CLEANUP_FAILED`. Linux `/proc` is required.

Cleanup failures fail closed: `execute` returns `status: "failed"`, `cleanup: "failed"`, `error` (`PROCESS_GROUP_CLEANUP_FAILED` or `WORKSPACE_CLEANUP_FAILED`), `fixture_exit_code`, and `artifacts` (leftover `workspace` directory name, `process_group` and `process_group_pids`, and the full `errors` list) for the operator to clean up. The in-memory run registry holds at most 1000 runs, then rejects with `RUN_REGISTRY_FULL` until restart. Run IDs are not reusable within a process lifetime.

No OpenCode, Telegram, GitHub, Hermes, or production execution integration exists. The bridge does not access the PFY repository; it only reads the operator-managed readiness snapshot.

## Operator-only installation/staging

Run these commands as an operator, outside normal agent execution. Replace `997` and `988` with verified numeric values of a dedicated, unprivileged worker (never 0) that has no supplementary groups and no write access to `/opt/hermes-dev-bridge` or `/etc/hermes-dev-bridge`. Review `bridge.py` first; run the commands from the repository root, one at a time, and stop at the first failure.

```sh
UID_W=997; GID_W=988
sudo install -d -o root -g root -m 755 /opt/hermes-dev-bridge /etc/hermes-dev-bridge
sudo install -o root -g root -m 755 bridge.py /opt/hermes-dev-bridge/bridge.py
sudo install -d -o "$UID_W" -g "$GID_W" -m 700 /var/lib/hermes-fixture
policy=$(mktemp)
printf '{"uid":%s,"gid":%s,"workspace":"/var/lib/hermes-fixture"}\n' "$UID_W" "$GID_W" > "$policy"
sudo install -o root -g root -m 644 "$policy" /etc/hermes-dev-bridge/policy.json
rm -f "$policy"
sudo -u "#$UID_W" -g "#$GID_W" -- id
sudo -u "#$UID_W" -g "#$GID_W" -- /usr/bin/python3 /opt/hermes-dev-bridge/bridge.py <<<'{"run_id":"smoke-1","operation":"smoke","repository":"fixture"}'
```

Check that `id` prints exactly the worker UID and GID with no extra `groups=`; the bridge reports `IDENTITY_MISMATCH` otherwise. If the smoke run returns `blocked`, the `error` field names the failed check.

To stage PFY preflight, create `/etc/hermes-dev-bridge/pfy-readiness.json` as root with mode `0644` (readable by the worker, not group/world-writable) and content that satisfies the schema above. The bridge does not refresh this file automatically; the operator must rewrite it when readiness changes.

Do not run privileged commands automatically. Before staging, verify the installed source is the intended reviewed file and that the worker cannot write `/opt/hermes-dev-bridge`, `/etc/hermes-dev-bridge`, or any parent of `/var/lib/hermes-fixture`.

## One-use execution authorization (U4.4)

Execution requires a durable, operator-controlled one-use grant. The bridge reads grants from `/etc/hermes-dev-bridge/execution.json` at startup. The legacy `{"execute_operations":[...]}` format is rejected; the current format is:

```json
{"grants":[{"grant_id":"<unique-id>","operation":"smoke"}]}
```

Each `grant_id` authorizes exactly one execution attempt of the fixed `smoke` fixture. Consumption is recorded in the SQLite ledger and survives restart, failure, timeout, and uncertain (`recovery-required`) outcomes. Hermes cannot create, renew, reset, or revoke grants. Removing a consumed grant from the configuration does not make it available again; adding a new `grant_id` makes one additional attempt available.

Missing or invalid authorization fails closed. If no unconsumed `smoke` grant exists, `bridge_execute_smoke` receives `state: "blocked"`, `error_code: "GRANT_EXHAUSTED"`.

## PFY readiness preflight (U5.2)

`bridge_pfy_preflight` is a parameter-free Hermes tool that requests a read-only readiness snapshot. The bridge never touches the PFY repository; it reads only the operator-controlled file `/etc/hermes-dev-bridge/pfy-readiness.json`.

Request (fixed):
```json
{"request_id":"<id>","operation":"pfy/preflight","repository":"fixture","timeout_seconds":5}
```

The bridge verifies, before trusting the file:
- absolute trusted path under `/etc/hermes-dev-bridge`, no symlinks;
- regular file, root-owned, not group/world-writable;
- size at most 64 KiB;
- strict JSON schema: exactly `generated_at`, `source_revision`, `ready`, `blockers`, `max_age_seconds`;
- valid types and consistency (`ready: true` only with an empty `blockers` list);
- freshness: `now <= generated_at + max_age_seconds`.

Failures are reported as `status: "blocked"`, `ready: false`, with one of:
`PFY_READINESS_UNVERIFIED`, `PFY_READINESS_OVERSIZED`, `PFY_READINESS_INVALID`, `PFY_READINESS_STALE`, `PFY_READINESS_BLOCKED`.

A verified, ready snapshot returns:
```json
{"status":"acknowledged","state":"ready","ready":true,
 "generation_time":<float>,"source_revision":"<rev>","blockers":[]}
```

`ready: true` does **not** authorize execution. `execute: true` is rejected for `pfy/preflight` and never consumes a `smoke` grant.

The operator creates `/etc/hermes-dev-bridge/pfy-readiness.json` manually. Example blocked metadata:
```json
{"generated_at":1731849600.0,"source_revision":"pfy-abc123",
 "ready":false,"blockers":["dirty working tree","Node-version drift unresolved"],
 "max_age_seconds":3600}
```

## Current verification

`python3 -m unittest` runs unprivileged. It covers rejection paths, real subprocess/descendant cleanup (timeout and normal exit, verified dead via `/proc`), `killpg` and survivor fail-closed behavior, the public `execute` cleanup-failure result, the registry limit, workspace path checks (using a temporary trusted root), and one-use grant consumption including restart and concurrent admission. It does not cover the live `/etc` policy, UID/GID switching, or the installed filesystem layout; those require the operator staging above. Until staging and an independent review complete, deterministic fixture execution is **NO-GO**.
