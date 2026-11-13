#!/usr/bin/env python3
"""Restricted fixture bridge; authorization is bound to operator-owned OS policy."""
import json, os, re, shutil, signal, stat, subprocess, sys, tempfile, threading, time
from pathlib import Path
from typing import Any

MAX_OUTPUT = 16 * 1024
DEFAULT_TIMEOUT = 5
ALLOWED_OPERATIONS = {"preflight", "status", "smoke"}
ALLOWED_REPOSITORY = "fixture"
POLICY_PATH = Path("/etc/hermes-dev-bridge/policy.json")
MAX_RUNS = 1000  # ponytail: in-memory, fail closed when full; add persistence/eviction if long-lived runs are needed
KILL_WAIT = 2.0
RUNS: dict[str, dict[str, Any]] = {}
LOCK = threading.Lock()
SECRET_RE = re.compile(r"(?i)(?:api[_-]?key|token|secret|password|credential|authorization)\s*[:=]\s*[^\s,;]+")

class BridgeError(ValueError): pass

class CleanupError(BridgeError):
    """Cleanup failed; carries the fixture outcome and leftover artifacts for the caller."""
    def __init__(self, error: str, code: int, artifacts: dict[str, Any]):
        super().__init__(error); self.code, self.artifacts = code, artifacts

def _sanitize(value: str) -> str:
    value = SECRET_RE.sub(lambda m: m.group(0)[:m.group(0).find('=') + 1] + '[REDACTED]', value)
    return re.sub(r"/(?:home|root|tmp)/[^\s]+", "[PATH]", value)[:MAX_OUTPUT]

def _validate(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict): raise BridgeError("INVALID_REQUEST")
    if set(request) - {"run_id", "operation", "repository", "timeout_seconds"}: raise BridgeError("UNSUPPORTED_FIELD")
    if request.get("operation") not in ALLOWED_OPERATIONS: raise BridgeError("UNSUPPORTED_OPERATION")
    if request.get("repository") != ALLOWED_REPOSITORY: raise BridgeError("UNSUPPORTED_REPOSITORY")
    run_id = request.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id): raise BridgeError("INVALID_RUN_ID")
    timeout = request.get("timeout_seconds", DEFAULT_TIMEOUT)
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= 30: raise BridgeError("INVALID_TIMEOUT")
    return request

def _secure_file(path: Path, owner: int) -> bool:
    try:
        st = path.lstat()
        return st.st_uid == owner and not (st.st_mode & 0o022) and path.is_file()
    except OSError: return False

def isolation_status() -> tuple[bool, str]:
    """Verify numeric identity and root-owned executable/policy; no env or username is trusted."""
    try:
        policy = json.loads(POLICY_PATH.read_text())
        uid, gid = os.geteuid(), os.getegid()
        expected_uid, expected_gid = int(policy["uid"]), int(policy["gid"])
        code = Path(__file__).resolve()
        if uid != expected_uid or gid != expected_gid or set(os.getgroups()) != {expected_gid}:
            return False, "IDENTITY_MISMATCH"
        if not _secure_file(POLICY_PATH, 0) or not _secure_file(code, 0): return False, "TRUSTED_CODE_UNVERIFIED"
        try: _workspace_root()
        except (OSError, ValueError, KeyError, TypeError): return False, "WORKSPACE_UNVERIFIED"
        return True, "IDENTITY_VERIFIED"
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False, "WORKER_POLICY_UNVERIFIED"

def _fixture_script(operation: str) -> str:
    return {"preflight": "print('fixture-preflight-ok')", "status": "print('fixture-ready')", "smoke": "print('fixture-ok')", "_timeout": "import subprocess,sys,time; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); time.sleep(60)"}[operation]

def _chain_ok(path: Path, uid: int, gid: int, trusted_uid: int = 0, start: Path = Path("/")) -> bool:
    """No symlinks; every ancestor is a trusted-owned dir not group/world-writable (so only root can swap one); leaf is worker-owned 0700."""
    try:
        if not path.is_absolute() or ".." in path.parts: return False
        parts = path.relative_to(start).parts
        for d in [start] + [start.joinpath(*parts[:i + 1]) for i in range(len(parts))]:
            st = d.lstat()
            if not stat.S_ISDIR(st.st_mode): return False
            if d == path:
                if st.st_uid != uid or st.st_gid != gid or st.st_mode & 0o077: return False
            elif st.st_uid != trusted_uid or st.st_mode & 0o022: return False
        return True
    except (OSError, ValueError): return False

def _workspace_root() -> Path:
    policy = json.loads(POLICY_PATH.read_text())
    root = Path(policy["workspace"])
    if not _chain_ok(root, int(policy["uid"]), int(policy["gid"])): raise ValueError("WORKSPACE_UNVERIFIED")
    return root

def _group_pids(pgid: int) -> list[int]:
    """Live (non-zombie) members of process group pgid, read from /proc."""
    pids = []
    for name in os.listdir("/proc"):
        if not name.isdigit(): continue
        try: fields = Path(f"/proc/{name}/stat").read_text().rpartition(")")[2].split()
        except OSError: continue
        if fields[0] != "Z" and int(fields[2]) == pgid: pids.append(int(name))
    return pids

def _terminate_group(pgid: int) -> list[int]:
    """SIGKILL the group and return survivors after KILL_WAIT. Only ESRCH is tolerated from killpg."""
    try: os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError: pass
    deadline = time.monotonic() + KILL_WAIT
    while True:
        left = _group_pids(pgid)
        if not left or time.monotonic() >= deadline: return left
        time.sleep(0.05)

def _wait_exit(process: subprocess.Popen, timeout: float) -> bool:
    """Wait for the leader to exit WITHOUT reaping it (WNOWAIT), so its PID/PGID stays reserved. False on timeout."""
    deadline = time.monotonic() + timeout
    while True:
        if os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None: return True
        if time.monotonic() >= deadline: return False
        time.sleep(0.02)

def _run_fixture(operation: str, timeout: int) -> tuple[int, str]:
    try: workspace = Path(tempfile.mkdtemp(prefix="hermes-fixture-", dir=_workspace_root()))
    except (OSError, ValueError, KeyError, TypeError): return 125, "WORKSPACE_CREATE_FAILED"
    code, evidence, errors, artifacts = 125, "FIXTURE_START_FAILED", [], {}
    # Output goes to a file, not pipes: no communicate()/reap needed before the group is killed, and a
    # descendant holding the pipe open cannot block us. ponytail: output unbounded on disk, read capped at 64 KiB.
    with tempfile.TemporaryFile("w+", errors="replace") as out:
        try:
            process = subprocess.Popen([sys.executable, "-c", _fixture_script(operation)], cwd=workspace, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, start_new_session=True, env={"PATH": "/usr/bin:/bin"})
        except OSError: process = None
        if process:
            pgid = process.pid
            # Ordering: the leader stays unreaped (alive or zombie) until the group is killed and checked, so the
            # kernel cannot reuse pgid for an unrelated process while we signal it. Reap only afterwards.
            try: timed_out = not _wait_exit(process, timeout)
            except OSError: timed_out = True  # cannot observe leader; killpg below is still safe (unreaped)
            try:
                left = _terminate_group(pgid)  # also kills descendants left behind by a fixture that exited
            except OSError as exc:
                errors.append("PROCESS_GROUP_CLEANUP_FAILED")
                artifacts["process_group_error"] = type(exc).__name__
                try: process.kill()
                except OSError: pass
                left = _group_pids(pgid)  # read-only; never signals after this point
            try: process.wait(timeout=KILL_WAIT)  # reap last
            except subprocess.TimeoutExpired: errors.append("PROCESS_REAP_FAILED")
            if left:
                if "PROCESS_GROUP_CLEANUP_FAILED" not in errors: errors.append("PROCESS_GROUP_CLEANUP_FAILED")
                artifacts["process_group_pids"] = left
            if errors: artifacts["process_group"] = pgid
            if timed_out: code, evidence = 124, "TIMEOUT; child group terminated"
            else:
                out.seek(0)
                code, evidence = process.returncode, _sanitize(out.read(65536).strip())
    try: shutil.rmtree(workspace)
    except OSError: errors.append("WORKSPACE_CLEANUP_FAILED"); artifacts["workspace"] = workspace.name
    if errors: raise CleanupError(errors[0], code, {**artifacts, "errors": errors})
    return code, evidence

def execute(request: Any) -> dict[str, Any]:
    started = time.time()
    try: request = _validate(request)
    except BridgeError as exc: return {"status": "rejected", "error": str(exc)}
    with LOCK:
        if request["run_id"] in RUNS: return {"status": "rejected", "error": "DUPLICATE_RUN_ID"}
        if len(RUNS) >= MAX_RUNS: return {"status": "rejected", "error": "RUN_REGISTRY_FULL"}
        RUNS[request["run_id"]] = {"status": "running"}
    ok, reason = isolation_status()
    if not ok: result = {"status": "blocked", "error": reason, "exit_code": None, "evidence": "fixture not executed"}
    else:
        try: code, evidence = _run_fixture(request["operation"], request.get("timeout_seconds", DEFAULT_TIMEOUT)); result = {"status": "passed" if code == 0 else "failed", "exit_code": code, "evidence": evidence}
        except CleanupError as exc: result = {"status": "failed", "error": str(exc), "cleanup": "failed", "exit_code": 125, "fixture_exit_code": exc.code, "evidence": "cleanup failed", "artifacts": exc.artifacts}
    result.update({"run_id": request["run_id"], "operation": request["operation"], "started_at": started, "ended_at": time.time()})
    with LOCK: RUNS[request["run_id"]] = result
    return result

def main() -> None:
    if len(sys.argv) != 1: raise SystemExit("stdin JSON only")
    for line in sys.stdin:
        try: print(json.dumps(execute(json.loads(line)), sort_keys=True), flush=True)
        except (json.JSONDecodeError, OSError): print(json.dumps({"status": "rejected", "error": "INVALID_JSON"}), flush=True)

if __name__ == "__main__": main()
