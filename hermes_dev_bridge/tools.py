"""Fixed acknowledgement-only bridge tool."""
from __future__ import annotations

import json
import secrets
import socket
from typing import Any

SOCKET_PATH = "/run/hermes-dev-bridge/bridge.sock"
_CONNECT_TIMEOUT = 2.0
_READ_TIMEOUT = 2.0
_MAX_FRAME = 4096
_REQUEST_ID_BYTES = 16


def _error(code: str) -> str:
    return json.dumps({"success": False, "error": code}, separators=(",", ":"))


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
            conn.settimeout(_CONNECT_TIMEOUT)
            conn.connect(SOCKET_PATH)
            conn.settimeout(_READ_TIMEOUT)
            conn.sendall(json.dumps(request, separators=(",", ":")).encode("ascii") + b"\n")
            response = json.loads(_read_response(conn))
        return json.dumps(_validate_response(response, request_id), separators=(",", ":"))
    except socket.timeout:
        return _error("SOCKET_TIMEOUT")
    except (FileNotFoundError, ConnectionRefusedError, NotADirectoryError):
        return _error("SOCKET_UNAVAILABLE")
    except (OSError, UnicodeDecodeError):
        return _error("TRANSPORT_ERROR")
    except json.JSONDecodeError:
        return _error("MALFORMED_RESPONSE")
    except ValueError as exc:
        return _error(str(exc))
