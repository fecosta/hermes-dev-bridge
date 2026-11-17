"""Fixed acknowledgement-only and execution bridge tools."""
from __future__ import annotations

import json
import secrets
import socket
from typing import Any

SOCKET_PATH = "/run/hermes-dev-bridge/bridge.sock"
_CONNECT_TIMEOUT = 2.0
_READ_TIMEOUT_ACK = 2.0
_READ_TIMEOUT_EXEC = 10.0
_MAX_FRAME = 4096
_REQUEST_ID_BYTES = 16


def _error(code: str, request_id: str | None = None) -> str:
    payload: dict[str, Any] = {"success": False, "error": code}
    if request_id is not None:
        payload["request_id"] = request_id
    return json.dumps(payload, separators=(",", ":"))


def _request_id() -> str:
    return "hermes-dev-bridge-" + secrets.token_hex(_REQUEST_ID_BYTES)


def _read_response(conn: socket.socket) -> bytes:
    data = bytearray()
    while len(data) < _MAX_FRAME:
        chunk = conn.recv(min(1024, _MAX_FRAME + 1 - len(data)))
        if not chunk:
            break
        data.extend(chunk)
        newline = data.find(b"\n")
        if newline >= 0:
            if newline != len(data) - 1 or data.find(b"\n", newline + 1) >= 0:
                raise ValueError("MALFORMED_RESPONSE")
            return bytes(data[:newline])
    raise ValueError("OVERSIZED_RESPONSE" if len(data) >= _MAX_FRAME else "MALFORMED_RESPONSE")


def _send_request(conn: socket.socket, request: dict[str, Any], read_timeout: float) -> Any:
    conn.settimeout(_CONNECT_TIMEOUT)
    conn.connect(SOCKET_PATH)
    conn.settimeout(read_timeout)
    conn.sendall(json.dumps(request, separators=(",", ":")).encode("ascii") + b"\n")
    return json.loads(_read_response(conn))


def _validate_response(value: Any, request_id: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("MALFORMED_RESPONSE")
    if value.get("status") != "acknowledged":
        raise ValueError("SERVER_REJECTED")
    if value.get("request_id") != request_id:
        raise ValueError("MALFORMED_RESPONSE")
    if value.get("operation") != "smoke" or value.get("repository") != "fixture":
        raise ValueError("MALFORMED_RESPONSE")
    if value.get("state") != "acknowledged":
        raise ValueError("MALFORMED_RESPONSE")
    evidence = value.get("evidence")
    if not isinstance(evidence, str) or "not executed" not in evidence.lower():
        raise ValueError("EXECUTION_NOT_CONFIRMED_ABSENT")
    return {"success": True, "acknowledged": True, "executed": False, "request_id": request_id}


def bridge_smoke(_args: dict[str, Any], **_kwargs: Any) -> str:
    request_id = _request_id()
    request = {"request_id": request_id, "operation": "smoke", "repository": "fixture", "timeout_seconds": 5}
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            response = _send_request(conn, request, _READ_TIMEOUT_ACK)
        return json.dumps(_validate_response(response, request_id), separators=(",", ":"))
    except socket.timeout:
        return _error("SOCKET_TIMEOUT", request_id)
    except (FileNotFoundError, ConnectionRefusedError, NotADirectoryError):
        return _error("SOCKET_UNAVAILABLE", request_id)
    except (OSError, UnicodeDecodeError):
        return _error("TRANSPORT_ERROR", request_id)
    except json.JSONDecodeError:
        return _error("MALFORMED_RESPONSE", request_id)
    except ValueError as exc:
        return _error(str(exc), request_id)


def _validate_execute_response(value: Any, request_id: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("MALFORMED_RESPONSE")
    status = value.get("status")
    if status == "rejected":
        raise ValueError(value.get("error", "SERVER_REJECTED"))
    if value.get("request_id") != request_id:
        raise ValueError("MALFORMED_RESPONSE")
    if value.get("operation") != "smoke" or value.get("repository") != "fixture":
        raise ValueError("MALFORMED_RESPONSE")
    state = value.get("state")
    evidence = value.get("evidence")
    if not isinstance(evidence, str):
        raise ValueError("MALFORMED_RESPONSE")
    if status != "completed":
        raise ValueError("MALFORMED_RESPONSE")
    if state == "passed":
        exit_code = value.get("exit_code")
        if exit_code != 0:
            raise ValueError("EXECUTION_FAILED")
        return {"success": True, "executed": True, "request_id": request_id, "state": "passed", "evidence": evidence}
    if state == "failed":
        exit_code = value.get("exit_code")
        error_code = value.get("error_code", "EXECUTION_FAILED")
        if exit_code == 124:
            error_code = "EXECUTION_TIMEOUT"
        return {"success": False, "executed": False, "request_id": request_id, "error": error_code, "state": "failed", "exit_code": exit_code}
    if state == "blocked":
        return {"success": False, "executed": False, "request_id": request_id, "error": value.get("error_code", "EXECUTION_BLOCKED"), "state": "blocked"}
    if state == "recovery-required":
        return {"success": False, "executed": False, "request_id": request_id, "error": value.get("error_code", "EXECUTION_UNCERTAIN"), "state": "recovery-required"}
    raise ValueError("MALFORMED_RESPONSE")


def bridge_execute_smoke(_args: dict[str, Any], **_kwargs: Any) -> str:
    request_id = _request_id()
    request = {"request_id": request_id, "operation": "smoke", "repository": "fixture", "timeout_seconds": 5, "execute": True}
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            response = _send_request(conn, request, _READ_TIMEOUT_EXEC)
        return json.dumps(_validate_execute_response(response, request_id), separators=(",", ":"))
    except socket.timeout:
        return _error("SOCKET_TIMEOUT", request_id)
    except (FileNotFoundError, ConnectionRefusedError, NotADirectoryError):
        return _error("SOCKET_UNAVAILABLE", request_id)
    except (OSError, UnicodeDecodeError):
        return _error("TRANSPORT_ERROR", request_id)
    except json.JSONDecodeError:
        return _error("MALFORMED_RESPONSE", request_id)
    except ValueError as exc:
        return _error(str(exc), request_id)
