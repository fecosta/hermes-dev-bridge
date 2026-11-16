#!/usr/bin/env python3
"""Authenticated, acknowledgement-only local transport for the U2 bridge."""
import json
import os
import socket
import struct
from typing import Any

import bridge
from ledger import Ledger, LedgerError, LedgerFull, RequestReuse

MAX_FRAME = 4096
MAX_CONNECTIONS = 16
READ_TIMEOUT = 2.0
LEDGER_PATH = "/var/lib/hermes-fixture/runs.sqlite3"  # operator-controlled; never request data

# Explicit operator-controlled default. Deployment must replace this value deliberately.
AUTHORIZED_PEER_UIDS = frozenset({997})

REQUEST_FIELDS = {"request_id", "operation", "repository", "timeout_seconds"}


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
    if value.get("operation") not in bridge.ALLOWED_OPERATIONS:
        return None
    if value.get("repository") != bridge.ALLOWED_REPOSITORY:
        return None
    timeout = value.get("timeout_seconds", bridge.DEFAULT_TIMEOUT)
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= 30:
        return None
    return value


def _peer_uid(conn: socket.socket) -> int | None:
    try:
        raw = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        return struct.unpack("3i", raw)[0]
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
                else:
                    try:
                        ledger = ledger or Ledger(LEDGER_PATH)
                        row = ledger.reserve(request)
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
