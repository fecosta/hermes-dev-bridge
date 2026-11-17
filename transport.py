#!/usr/bin/env python3
"""Authenticated, acknowledgement-only local transport for the U2 bridge."""
import json
import os
import socket
import stat
import struct
import time
from pathlib import Path
from typing import Any

import bridge
from ledger import Ledger, LedgerError, LedgerFull, RequestReuse

MAX_FRAME = 4096
MAX_CONNECTIONS = 16
READ_TIMEOUT = 2.0
LEDGER_PATH = "/var/lib/hermes-fixture/runs.sqlite3"  # operator-controlled; never request data

# Explicit operator-controlled default. Deployment must replace this value deliberately.
AUTHORIZED_PEER_UIDS = frozenset({997})

EXECUTION_CONFIG_PATH = Path("/etc/hermes-dev-bridge/execution.json")
PFY_READINESS_PATH = Path("/etc/hermes-dev-bridge/pfy-readiness.json")
PFY_OPERATION = "pfy/preflight"
MAX_PFY_SIZE = 64 * 1024
MAX_PFY_BLOCKERS = 16
MAX_PFY_BLOCKER_LEN = 128


def _secure_file(path: Path, owner: int = 0) -> bool:
    try:
        s = path.lstat()
        return stat.S_ISREG(s.st_mode) and s.st_uid == owner and not s.st_mode & 0o022
    except OSError:
        return False


def _trusted_chain(path: Path) -> bool:
    try:
        current = path
        if current.is_symlink() or not current.is_file():
            return False
        while True:
            s = current.lstat()
            if current != path and (s.st_uid != 0 or s.st_mode & 0o022):
                return False
            if current == current.parent:
                return True
            current = current.parent
    except OSError:
        return False


def load_execution_allowlist(path: Path = EXECUTION_CONFIG_PATH) -> list[dict[str, str]]:
    """Operator-controlled one-use execution grants. Missing config disables execution.
    Legacy execute_operations format is rejected; each grant authorizes one attempt."""
    if not path.exists():
        return []
    if not _trusted_chain(path) or not _secure_file(path):
        raise RuntimeError("EXECUTION_CONFIG_UNVERIFIED")
    try:
        value = json.loads(path.read_text())
        if not isinstance(value, dict) or set(value.keys()) != {"grants"}:
            raise RuntimeError("EXECUTION_CONFIG_INVALID")
        grants = value["grants"]
        if not isinstance(grants, list):
            raise RuntimeError("EXECUTION_CONFIG_INVALID")
        seen = set()
        result = []
        for grant in grants:
            if not isinstance(grant, dict) or set(grant.keys()) != {"grant_id", "operation"}:
                raise RuntimeError("EXECUTION_CONFIG_INVALID")
            grant_id = grant["grant_id"]
            operation = grant["operation"]
            if not isinstance(grant_id, str) or not isinstance(operation, str):
                raise RuntimeError("EXECUTION_CONFIG_INVALID")
            if operation != "smoke":
                raise RuntimeError("EXECUTION_CONFIG_INVALID")
            if grant_id in seen:
                raise RuntimeError("EXECUTION_CONFIG_INVALID")
            seen.add(grant_id)
            result.append({"grant_id": grant_id, "operation": operation})
        return result
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("EXECUTION_CONFIG_UNVERIFIED") from exc


def _read_pfy_readiness(path: Path, now: float | None = None) -> dict[str, Any]:
    """Read and validate the operator-controlled PFY readiness snapshot.

    Returns a fail-closed result.  The file is never trusted until its path,
    ownership, permissions, size, schema, and freshness have been verified.
    """
    now = time.time() if now is None else now
    failure = {
        "ready": False,
        "valid": False,
        "generation_time": None,
        "source_revision": None,
        "blockers": [],
        "error_code": "PFY_READINESS_UNVERIFIED",
        "evidence": "PFY readiness metadata could not be verified",
    }
    if not _trusted_chain(path) or not _secure_file(path):
        return {**failure, "error_code": "PFY_READINESS_UNVERIFIED", "evidence": "PFY readiness file failed trusted-path verification"}
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return {**failure, "error_code": "PFY_READINESS_UNVERIFIED", "evidence": "PFY readiness file could not be opened"}
    try:
        size = os.fstat(fd).st_size
        if size > MAX_PFY_SIZE:
            return {**failure, "error_code": "PFY_READINESS_OVERSIZED", "evidence": "PFY readiness file exceeds size limit"}
        data = b""
        while len(data) < MAX_PFY_SIZE + 1:
            chunk = os.read(fd, 8192)
            if not chunk:
                break
            data += chunk
        if len(data) > MAX_PFY_SIZE:
            return {**failure, "error_code": "PFY_READINESS_OVERSIZED", "evidence": "PFY readiness file exceeds size limit"}
        try:
            value = json.loads(data.decode("utf-8", errors="replace"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return {**failure, "error_code": "PFY_READINESS_INVALID", "evidence": "PFY readiness file is not valid JSON"}
    finally:
        try:
            os.close(fd)
        except OSError:
            pass

    if not isinstance(value, dict):
        return {**failure, "error_code": "PFY_READINESS_INVALID", "evidence": "PFY readiness metadata is not a JSON object"}
    expected_keys = {"generated_at", "source_revision", "ready", "blockers", "max_age_seconds"}
    if set(value) != expected_keys:
        return {**failure, "error_code": "PFY_READINESS_INVALID", "evidence": "PFY readiness metadata has incorrect fields"}

    generated_at = value["generated_at"]
    source_revision = value["source_revision"]
    ready = value["ready"]
    blockers = value["blockers"]
    max_age = value["max_age_seconds"]

    if (
        isinstance(generated_at, bool)
        or not isinstance(generated_at, (int, float))
        or isinstance(source_revision, bool)
        or not isinstance(source_revision, str)
        or not isinstance(ready, bool)
        or isinstance(blockers, bool)
        or not isinstance(blockers, list)
        or isinstance(max_age, bool)
        or not isinstance(max_age, (int, float))
        or max_age <= 0
    ):
        return {**failure, "error_code": "PFY_READINESS_INVALID", "evidence": "PFY readiness metadata has invalid types"}

    if not all(isinstance(b, str) and not isinstance(b, bool) for b in blockers):
        return {**failure, "error_code": "PFY_READINESS_INVALID", "evidence": "PFY readiness blockers must be strings"}
    if ready and blockers:
        return {**failure, "error_code": "PFY_READINESS_INVALID", "evidence": "PFY readiness claims ready with unresolved blockers"}

    safe_blockers: list[str] = []
    for b in blockers[:MAX_PFY_BLOCKERS]:
        safe = bridge._sanitize(b[:MAX_PFY_BLOCKER_LEN])
        if safe and safe not in safe_blockers:
            safe_blockers.append(safe)

    if now > generated_at + max_age:
        return {
            **failure,
            "generation_time": float(generated_at),
            "source_revision": source_revision,
            "blockers": safe_blockers,
            "error_code": "PFY_READINESS_STALE",
            "evidence": "PFY readiness snapshot has expired",
        }

    return {
        "ready": ready,
        "valid": True,
        "generation_time": float(generated_at),
        "source_revision": source_revision,
        "blockers": safe_blockers,
        "error_code": None if ready else "PFY_READINESS_BLOCKED",
        "evidence": "PFY readiness snapshot verified" if ready else "PFY readiness snapshot reports blockers",
    }


def _pfy_response(request_id: str, result: dict[str, Any]) -> dict[str, Any]:
    revision = result["source_revision"]
    if isinstance(revision, str):
        revision = revision[:128]
    response: dict[str, Any] = {
        "status": "acknowledged" if result["ready"] else "blocked",
        "request_id": request_id,
        "operation": PFY_OPERATION,
        "repository": "fixture",
        "state": "ready" if result["ready"] else "blocked",
        "ready": result["ready"],
        "generation_time": result["generation_time"],
        "source_revision": revision,
        "blockers": result["blockers"],
        "evidence": result["evidence"],
    }
    if result["error_code"]:
        response["error_code"] = result["error_code"]
    return response


REQUEST_FIELDS = {"request_id", "operation", "repository", "timeout_seconds", "execute"}


def _error(code: str) -> dict[str, str]:
    return {"status": "rejected", "error": code}


def validate_request(value: Any) -> dict[str, Any] | None:
    """Validate the Unit 1 protocol without invoking bridge.execute."""
    if not isinstance(value, dict):
        return None
    if set(value) - REQUEST_FIELDS:
        return None
    request_id = value.get("request_id")
    if not isinstance(request_id, str) or not __import__("re").fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", request_id
    ):
        return None
    if value.get("operation") not in (bridge.ALLOWED_OPERATIONS | {PFY_OPERATION}):
        return None
    if value.get("repository") != bridge.ALLOWED_REPOSITORY:
        return None
    timeout = value.get("timeout_seconds", bridge.DEFAULT_TIMEOUT)
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= 30:
        return None
    execute = value.get("execute", False)
    if execute is not False and execute is not True:
        return None
    return {**value, "execute": execute}


def _peer_uid(conn: socket.socket) -> int | None:
    try:
        raw = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        return struct.unpack("3i", raw)[1]
    except (AttributeError, OSError, struct.error):
        return None


def _read_frame(conn: socket.socket) -> bytes | None:
    data = bytearray()
    while len(data) < MAX_FRAME:
        chunk = conn.recv(min(1024, MAX_FRAME + 1 - len(data)))
        if not chunk:
            return bytes(data) if data else None
        data.extend(chunk)
        newline = data.find(b"\n")
        if newline >= 0:
            if newline != len(data) - 1 or data.find(b"\n", newline + 1) >= 0:
                return None
            return bytes(data[:newline])
    return None


def _terminal_response(row: dict[str, Any]) -> dict[str, Any]:
    status_map = {
        "acknowledged": "acknowledged",
        "running": "running",
        "passed": "completed",
        "failed": "completed",
        "blocked": "blocked",
        "recovery-required": "completed",
    }
    response = {
        "status": status_map[row["state"]],
        "request_id": row["request_id"],
        "operation": row["operation"],
        "repository": row["repository"],
        "state": row["state"],
        "evidence": row["evidence"],
    }
    if row["exit_code"] is not None:
        response["exit_code"] = row["exit_code"]
    if row["error_code"]:
        response["error_code"] = row["error_code"]
    return response


def _map_bridge_result(ledger: Ledger, request_id: str, result: dict[str, Any]) -> dict[str, Any]:
    status = result.get("status")
    if status == "passed":
        return ledger.transition(request_id, "passed", exit_code=result.get("exit_code"), evidence=result.get("evidence"))
    if status == "failed":
        error_code = result.get("error")
        evidence = result.get("evidence") or ""
        if result.get("exit_code") == 124 and not error_code:
            error_code = "EXECUTION_TIMEOUT"
        if result.get("fixture_exit_code") is not None or result.get("artifacts"):
            details = json.dumps(
                {"fixture_exit_code": result.get("fixture_exit_code"), "artifacts": result.get("artifacts")},
                sort_keys=True,
                separators=(",", ":"),
            )
            evidence = f"{evidence}; details={details}"
        return ledger.transition(
            request_id, "failed",
            exit_code=result.get("exit_code"),
            evidence=evidence,
            error_code=error_code,
            cleanup_status=result.get("cleanup"),
        )
    if status == "blocked":
        return ledger.transition(request_id, "blocked", evidence=result.get("evidence"), error_code=result.get("error"))
    return ledger.transition(request_id, "blocked", evidence=result.get("error") or f"bridge returned {status}", error_code="BRIDGE_REJECTED")


def handle_connection(conn: socket.socket, authorized_uids: frozenset[int] = AUTHORIZED_PEER_UIDS, ledger=None) -> None:
    conn.settimeout(READ_TIMEOUT)
    try:
        if _peer_uid(conn) not in authorized_uids:
            conn.sendall(json.dumps(_error("UNAUTHORIZED_PEER"), separators=(",", ":")).encode() + b"\n")
            return
        frame = _read_frame(conn)
        if frame is None:
            response = _error("INVALID_FRAME")
        else:
            try:
                value = json.loads(frame)
            except (UnicodeDecodeError, json.JSONDecodeError):
                response = _error("INVALID_JSON")
            else:
                request = validate_request(value)
                if request is None:
                    response = _error("INVALID_REQUEST")
                elif request["operation"] == PFY_OPERATION and request.get("execute"):
                    response = _error("INVALID_REQUEST")
                else:
                    try:
                        ledger = ledger or Ledger(LEDGER_PATH)
                        row = ledger.reserve(request)
                        if request.get("execute"):
                            if row["state"] != "accepted":
                                response = _terminal_response(row)
                            else:
                                claimed = ledger.dispatch_running(request["request_id"])
                                if claimed is None:
                                    row = ledger.get(request["request_id"])
                                    response = _terminal_response(row)
                                else:
                                    grant_id = ledger.consume_grant(request["operation"], request["request_id"])
                                    if grant_id is None:
                                        row = ledger.transition(
                                            request["request_id"], "blocked",
                                            error_code="GRANT_EXHAUSTED",
                                            evidence="execution grant exhausted or unavailable",
                                        )
                                        response = _terminal_response(row)
                                    else:
                                        bridge_request = {
                                            "run_id": request["request_id"],
                                            "operation": request["operation"],
                                            "repository": request["repository"],
                                            "timeout_seconds": request.get("timeout_seconds", bridge.DEFAULT_TIMEOUT),
                                        }
                                        try:
                                            result = bridge.execute(bridge_request)
                                        except Exception as exc:
                                            row = ledger.transition(
                                                request["request_id"], "recovery-required",
                                                error_code="EXECUTION_UNCERTAIN",
                                                evidence=bridge._sanitize(str(exc)),
                                            )
                                            response = _terminal_response(row)
                                        else:
                                            row = _map_bridge_result(ledger, request["request_id"], result)
                                            response = _terminal_response(row)
                        else:
                            if request["operation"] == PFY_OPERATION:
                                result = _read_pfy_readiness(PFY_READINESS_PATH)
                                if row["state"] == "accepted":
                                    if result["ready"]:
                                        ledger.transition(
                                            request["request_id"], "acknowledged",
                                            evidence=result["evidence"],
                                        )
                                    else:
                                        ledger.transition(
                                            request["request_id"], "blocked",
                                            evidence=result["evidence"],
                                            error_code=result["error_code"],
                                        )
                                response = _pfy_response(request["request_id"], result)
                            else:
                                if row["state"] == "accepted":
                                    row = ledger.transition(request["request_id"], "acknowledged", evidence="not executed; dispatch unavailable")
                                response = {
                                    "status": "acknowledged",
                                    "request_id": row["request_id"],
                                    "operation": row["operation"],
                                    "repository": row["repository"],
                                    "state": row["state"],
                                    "evidence": row["evidence"],
                                }
                    except RequestReuse:
                        response = _error("REQUEST_ID_REUSE")
                    except LedgerFull:
                        response = _error("LEDGER_FULL")
                    except LedgerError:
                        response = _error("LEDGER_UNAVAILABLE")
        conn.sendall(json.dumps(response, separators=(",", ":")).encode() + b"\n")
    except (OSError, socket.timeout):
        return
    finally:
        conn.close()


def serve(path: str, authorized_uids: frozenset[int] = AUTHORIZED_PEER_UIDS, ledger=None) -> None:
    if os.path.exists(path):
        raise RuntimeError("SOCKET_PATH_EXISTS")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.settimeout(READ_TIMEOUT)
    try:
        listener.bind(path)
        listener.listen(MAX_CONNECTIONS)
        active = 0
        while True:
            conn, _ = listener.accept()
            if active >= MAX_CONNECTIONS:
                conn.close()
                continue
            active += 1
            try:
                handle_connection(conn, authorized_uids, ledger)
            finally:
                active -= 1
    finally:
        listener.close()
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    raise SystemExit("socket activation not configured; use serve() from an operator-controlled service")
