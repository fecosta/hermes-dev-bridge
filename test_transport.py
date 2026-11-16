import json
import os
import socket
import struct
import tempfile
import threading
import unittest
from unittest import mock

import transport
from ledger import Ledger


class TransportTests(unittest.TestCase):
    def exchange(self, payload, uid=None, ledger=None):
        left, right = socket.socketpair()
        uid = os.getuid() if uid is None else uid
        with mock.patch.object(transport, "_peer_uid", return_value=uid):
            thread = threading.Thread(target=transport.handle_connection, args=(right, frozenset({os.getuid()}), ledger))
            thread.start()
            try:
                left.sendall(payload)
            except BrokenPipeError:
                pass
            result = left.recv(4096)
            thread.join()
        left.close()
        return json.loads(result)

    def test_ack_never_dispatches(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(b'{"request_id":"r1","operation":"smoke","repository":"fixture"}\n', ledger=ledger)
            self.assertEqual(result["status"], "acknowledged")
            self.assertEqual(result["state"], "acknowledged")
            self.assertIn("not executed", result["evidence"])
            self.assertEqual(ledger.get("r1")["state"], "acknowledged")
            ledger.close()

    def test_partial_input(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            result = self.exchange(b'{"request_id":"r1","operation":"status","repository":"fixture"}\n', ledger=ledger)
            self.assertEqual(result["request_id"], "r1")
            ledger.close()

    def test_oversized_and_multiple_frames_rejected(self):
        result = self.exchange(b"x" * transport.MAX_FRAME + b"\n")
        self.assertEqual(result["error"], "INVALID_FRAME")
        result = self.exchange(b'{}\n{}\n')
        self.assertEqual(result["error"], "INVALID_FRAME")

    def test_malformed_and_invalid_requests(self):
        self.assertEqual(self.exchange(b"not-json\n")["error"], "INVALID_JSON")
        self.assertEqual(self.exchange(b'{"request_id":"r1","operation":"exec","repository":"fixture"}\n')["error"], "INVALID_REQUEST")
        self.assertEqual(self.exchange(b'{"request_id":"r1","operation":"smoke","repository":"fixture","command":"x"}\n')["error"], "INVALID_REQUEST")

    def test_authorized_uid_default_uses_constant(self):
        self.assertIs(transport.handle_connection.__defaults__[0], transport.AUTHORIZED_PEER_UIDS)
        self.assertIs(transport.serve.__defaults__[0], transport.AUTHORIZED_PEER_UIDS)

    def test_peer_credentials_extract_uid_not_pid_or_gid(self):
        conn = mock.Mock()
        conn.getsockopt.return_value = struct.pack("3i", 4242, 1001, 988)
        self.assertEqual(transport._peer_uid(conn), 1001)

    def test_peer_credentials_missing_or_malformed_fail_closed(self):
        for credentials in (b"", struct.pack("2i", 4242, 1001), struct.pack("3q", 4242, 1001, 988)):
            conn = mock.Mock()
            conn.getsockopt.return_value = credentials
            self.assertIsNone(transport._peer_uid(conn))

    def test_authorized_uid_1001_accepted(self):
        with mock.patch.object(transport, "_peer_uid", return_value=1001):
            left, right = socket.socketpair()
            thread = threading.Thread(target=transport.handle_connection, args=(right, frozenset({1001})))
            thread.start()
            left.sendall(b"{}\n")
            result = json.loads(left.recv(4096))
            thread.join()
            left.close()
        self.assertEqual(result["error"], "INVALID_REQUEST")

    def test_unauthorized_uids_rejected(self):
        for uid in (997, 1002):
            result = self.exchange(b"{}\n", uid=uid)
            self.assertEqual(result, {"status": "rejected", "error": "UNAUTHORIZED_PEER"})

    def test_peer_rejection(self):
        result = self.exchange(b"{}\n", uid=12345)
        self.assertEqual(result, {"status": "rejected", "error": "UNAUTHORIZED_PEER"})

    def test_connection_cleanup(self):
        left, right = socket.socketpair()
        with mock.patch.object(transport, "_peer_uid", return_value=os.getuid()):
            thread = threading.Thread(target=transport.handle_connection, args=(right, frozenset({os.getuid()})))
            thread.start(); left.shutdown(socket.SHUT_WR)
            response = left.recv(4096)
            thread.join()
        self.assertEqual(json.loads(response)["error"], "INVALID_FRAME")
        self.assertEqual(left.recv(1), b"")
        left.close()


if __name__ == "__main__":
    unittest.main()
