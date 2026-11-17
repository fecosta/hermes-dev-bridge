"""Read-only PFY preflight for U5.2."""
import contextlib
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import transport
from ledger import Ledger


class PFYPreflightTests(unittest.TestCase):
    def exchange(self, payload, uid=None, ledger=None, pfy_path=None, secure=True):
        uid = os.getuid() if uid is None else uid
        left, right = socket.socketpair()
        mocks = [
            mock.patch.object(transport, "_trusted_chain", return_value=True),
            mock.patch.object(transport, "_peer_uid", return_value=uid),
        ]
        if secure is not None:
            mocks.append(mock.patch.object(transport, "_secure_file", return_value=secure))
        if pfy_path is not None:
            mocks.append(mock.patch.object(transport, "PFY_READINESS_PATH", pfy_path))
        with contextlib.ExitStack() as stack:
            for m in mocks:
                stack.enter_context(m)
            thread = threading.Thread(
                target=transport.handle_connection,
                args=(right, frozenset({os.getuid()}), ledger),
            )
            thread.start()
            try:
                left.sendall(payload)
            except BrokenPipeError:
                pass
            result = left.recv(4096)
            thread.join()
        left.close()
        return json.loads(result)

    def make_ledger(self, d):
        return Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())

    def write_pfy(self, path, **kw):
        data = {
            "generated_at": time.time(),
            "source_revision": "abc123",
            "ready": True,
            "blockers": [],
            "max_age_seconds": 3600,
        }
        data.update(kw)
        path.write_text(json.dumps(data))

    def test_valid_ready_report(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy)
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "acknowledged")
            self.assertEqual(result["state"], "ready")
            self.assertTrue(result["ready"])
            self.assertEqual(result["source_revision"], "abc123")
            self.assertEqual(result["blockers"], [])
            self.assertEqual(ledger.get("p1")["state"], "acknowledged")
            ledger.close()

    def test_blocked_readiness_report(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy, ready=False, blockers=["dirty working tree"])
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["state"], "blocked")
            self.assertFalse(result["ready"])
            self.assertEqual(result["blockers"], ["dirty working tree"])
            self.assertEqual(result["error_code"], "PFY_READINESS_BLOCKED")
            self.assertEqual(ledger.get("p1")["state"], "blocked")
            ledger.close()

    def test_missing_metadata_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertFalse(result["ready"])
            self.assertEqual(result["error_code"], "PFY_READINESS_UNVERIFIED")
            ledger.close()

    def test_invalid_json_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            pfy.write_text("not json")
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["error_code"], "PFY_READINESS_INVALID")
            ledger.close()

    def test_invalid_schema_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            pfy.write_text('{"ready":true}')
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["error_code"], "PFY_READINESS_INVALID")
            ledger.close()

    def test_ready_with_blockers_is_invalid(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy, ready=True, blockers=["tooling not verified"])
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["error_code"], "PFY_READINESS_INVALID")
            ledger.close()

    def test_stale_metadata_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy, generated_at=time.time() - 7200, max_age_seconds=3600)
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["error_code"], "PFY_READINESS_STALE")
            ledger.close()

    def test_duplicate_request_id_re_evaluates_freshness(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            ledger = self.make_ledger(d)
            self.write_pfy(pfy, generated_at=time.time(), max_age_seconds=3600)
            first = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertTrue(first["ready"])
            self.write_pfy(pfy, generated_at=time.time() - 7200, max_age_seconds=3600)
            second = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertFalse(second["ready"])
            self.assertEqual(second["error_code"], "PFY_READINESS_STALE")
            ledger.close()

    def test_unverified_file_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy)
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
                secure=False,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["error_code"], "PFY_READINESS_UNVERIFIED")
            ledger.close()

    def test_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            real = Path(d) / "real.json"
            link = Path(d) / "pfy-readiness.json"
            self.write_pfy(real)
            link.symlink_to(real)
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=link,
            )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["error_code"], "PFY_READINESS_UNVERIFIED")
            ledger.close()

    def test_unauthorized_peer_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                uid=12345,
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result, {"status": "rejected", "error": "UNAUTHORIZED_PEER"})
            ledger.close()

    def test_execute_true_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy)
            ledger = self.make_ledger(d)
            result = self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture","execute":true}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(result, {"status": "rejected", "error": "INVALID_REQUEST"})
            ledger.close()

    def test_pfy_does_not_consume_smoke_grant(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy)
            ledger = self.make_ledger(d)
            ledger.load_grants([{"grant_id": "g1", "operation": "smoke"}])
            self.exchange(
                b'{"request_id":"p1","operation":"pfy/preflight","repository":"fixture"}\n',
                ledger=ledger,
                pfy_path=pfy,
            )
            self.assertEqual(ledger.consume_grant("smoke", "s1"), "g1")
            ledger.close()

    def test_pfy_does_not_block_smoke_execution(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            self.write_pfy(pfy)
            ledger = self.make_ledger(d)
            ledger.load_grants([{"grant_id": "g1", "operation": "smoke"}])
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "passed", "exit_code": 0, "evidence": "fixture-ok"},
            ) as m:
                result = self.exchange(
                    b'{"request_id":"s1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    pfy_path=pfy,
                )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["state"], "passed")
            self.assertEqual(m.call_count, 1)
            ledger.close()

    def test_response_size_bounds_and_secrets_excluded(self):
        with tempfile.TemporaryDirectory() as d:
            pfy = Path(d) / "pfy-readiness.json"
            long_blocker = "x" * 1000
            secret_blocker = "api_key=super-secret"
            many = [f"blocker-{i}" for i in range(100)]
            self.write_pfy(
                pfy,
                ready=False,
                blockers=many + [long_blocker, secret_blocker],
            )
            with mock.patch.object(transport, "_trusted_chain", return_value=True), \
                 mock.patch.object(transport, "_secure_file", return_value=True):
                result = transport._read_pfy_readiness(pfy)
            self.assertFalse(result["ready"])
            blockers = result["blockers"]
            self.assertLessEqual(len(blockers), transport.MAX_PFY_BLOCKERS)
            self.assertTrue(all(len(b) <= transport.MAX_PFY_BLOCKER_LEN for b in blockers))
            self.assertNotIn("super-secret", json.dumps(blockers))
            self.assertNotIn(long_blocker, blockers)
            response = transport._pfy_response("req-1", result)
            encoded = json.dumps(response, separators=(",", ":")).encode("utf-8")
            self.assertLess(len(encoded), transport.MAX_FRAME)


if __name__ == "__main__":
    unittest.main()
