"""Conditional fixture-execution dispatch path for U4.2.1."""
import json
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import lifecycle
import transport
from ledger import Ledger


class ExecutionDispatchTests(unittest.TestCase):
    def exchange(self, payload, uid=None, ledger=None, execution_ops=frozenset()):
        left, right = socket.socketpair()
        uid = os.getuid() if uid is None else uid
        with mock.patch.object(transport, "_peer_uid", return_value=uid):
            thread = threading.Thread(
                target=transport.handle_connection,
                args=(right, frozenset({os.getuid()}), ledger, execution_ops),
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

    def test_ack_without_execute_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture"}\n',
                    ledger=ledger,
                )
            self.assertEqual(result["status"], "acknowledged")
            self.assertEqual(result["state"], "acknowledged")
            self.assertIn("not executed", result["evidence"])
            self.assertEqual(ledger.get("r1")["state"], "acknowledged")

    def test_execute_false_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":false}\n',
                    ledger=ledger,
                )
            self.assertEqual(result["status"], "acknowledged")
            self.assertEqual(ledger.get("r1")["state"], "acknowledged")

    def test_execute_disabled_returns_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                )
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["state"], "blocked")
            self.assertEqual(result["error_code"], "EXECUTION_DISABLED")
            self.assertEqual(ledger.get("r1")["state"], "blocked")

    def test_execute_allowed_runs_once(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())

            def fake_execute(req):
                self.assertEqual(req["run_id"], "r1")
                self.assertEqual(req["operation"], "smoke")
                self.assertEqual(req["repository"], "fixture")
                return {"status": "passed", "exit_code": 0, "evidence": "fixture-ok"}

            with mock.patch.object(transport.bridge, "execute", side_effect=fake_execute) as m:
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["state"], "passed")
            self.assertEqual(result["exit_code"], 0)
            self.assertEqual(ledger.get("r1")["state"], "passed")
            self.assertEqual(m.call_count, 1)

    def test_execute_failure_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "failed", "exit_code": 1, "evidence": "boom", "error": "FIXTURE_FAILED"},
            ):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["state"], "failed")
            self.assertEqual(result["exit_code"], 1)
            self.assertEqual(result["error_code"], "FIXTURE_FAILED")
            self.assertEqual(ledger.get("r1")["state"], "failed")

    def test_execute_cleanup_failure_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={
                    "status": "failed",
                    "exit_code": 125,
                    "evidence": "cleanup failed",
                    "error": "WORKSPACE_CLEANUP_FAILED",
                    "cleanup": "failed",
                    "fixture_exit_code": 0,
                    "artifacts": {"workspace": "hermes-fixture-xyz"},
                },
            ):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(result["state"], "failed")
            self.assertEqual(result["error_code"], "WORKSPACE_CLEANUP_FAILED")
            row = ledger.get("r1")
            self.assertEqual(row["cleanup_status"], "failed")
            self.assertIn("hermes-fixture-xyz", row["evidence"])

    def test_execute_timeout_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "failed", "exit_code": 124, "evidence": "TIMEOUT; child group terminated"},
            ):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(result["state"], "failed")
            self.assertEqual(result["exit_code"], 124)
            self.assertEqual(result["error_code"], "EXECUTION_TIMEOUT")

    def test_execute_uncertain_exception_recovery_required(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(transport.bridge, "execute", side_effect=RuntimeError("unexpected")):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["state"], "recovery-required")
            self.assertEqual(result["error_code"], "EXECUTION_UNCERTAIN")
            self.assertEqual(ledger.get("r1")["state"], "recovery-required")

    def test_bridge_blocked_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "blocked", "error": "IDENTITY_MISMATCH", "evidence": "fixture not executed"},
            ):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(result["state"], "blocked")
            self.assertEqual(result["error_code"], "IDENTITY_MISMATCH")

    def test_duplicate_request_id_not_replayed(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "passed", "exit_code": 0, "evidence": "ok"},
            ) as m:
                self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            self.assertEqual(m.call_count, 1)
            self.assertEqual(result["state"], "passed")

    def test_concurrent_dispatch_executes_once(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "runs.sqlite3")
            barrier = threading.Barrier(2)
            results = []
            calls = []

            def fake_execute(req):
                calls.append(req["run_id"])
                return {"status": "passed", "exit_code": 0, "evidence": "ok"}

            def run():
                ledger = Ledger(path, os.getuid(), os.getgid())
                left, right = socket.socketpair()
                thread = threading.Thread(
                    target=transport.handle_connection,
                    args=(right, frozenset({os.getuid()}), ledger, frozenset({"smoke"})),
                )
                thread.start()
                barrier.wait()
                try:
                    left.sendall(b'{"request_id":"same","operation":"smoke","repository":"fixture","execute":true}\n')
                except BrokenPipeError:
                    pass
                data = left.recv(4096)
                thread.join()
                left.close()
                ledger.close()
                results.append(json.loads(data))

            with mock.patch.object(transport.bridge, "execute", side_effect=fake_execute) as m, \
                 mock.patch.object(transport, "_peer_uid", return_value=os.getuid()):
                threads = [threading.Thread(target=run) for _ in range(2)]
                [t.start() for t in threads]
                [t.join() for t in threads]
            self.assertEqual(len(calls), 1)
            self.assertEqual(m.call_count, 1)
            states = {r["state"] for r in results}
            self.assertTrue(states <= {"passed", "running"})

    def test_crash_recovery_marks_running_recovery_required(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            req = {
                "request_id": "r1",
                "operation": "smoke",
                "repository": "fixture",
                "timeout_seconds": 5,
                "execute": True,
            }
            ledger.reserve(req)
            claimed = ledger.dispatch_running("r1")
            self.assertIsNotNone(claimed)
            ledger.close()

            ledger2 = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            lifecycle.recover_acknowledgements(ledger2)
            row = ledger2.get("r1")
            self.assertEqual(row["state"], "recovery-required")
            self.assertEqual(row["error_code"], "SERVICE_INTERRUPTED")
            self.assertIn("outcome unknown", row["evidence"])
            self.assertNotIn("not executed", row["evidence"])
            ledger2.close()

    def test_execute_unauthorized_operation_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"preflight"}),
                )
            self.assertEqual(result["state"], "blocked")
            self.assertEqual(result["error_code"], "EXECUTION_DISABLED")

    def test_unauthorized_after_execution_returns_terminal(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "passed", "exit_code": 0, "evidence": "ok"},
            ):
                self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"preflight"}),
                )
            self.assertEqual(result["state"], "passed")
            self.assertNotIn("error", result)

    def test_execute_changed_payload_reuse(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            with mock.patch.object(
                transport.bridge,
                "execute",
                return_value={"status": "passed", "exit_code": 0, "evidence": "ok"},
            ):
                self.exchange(
                    b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"smoke"}),
                )
            with mock.patch.object(transport.bridge, "execute", side_effect=AssertionError):
                result = self.exchange(
                    b'{"request_id":"r1","operation":"status","repository":"fixture","execute":true}\n',
                    ledger=ledger,
                    execution_ops=frozenset({"status"}),
                )
            self.assertEqual(result["error"], "REQUEST_ID_REUSE")

    def test_execute_invalid_value_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            ledger = Ledger(os.path.join(d, "runs.sqlite3"), os.getuid(), os.getgid())
            result = self.exchange(
                b'{"request_id":"r1","operation":"smoke","repository":"fixture","execute":"yes"}\n',
                ledger=ledger,
            )
            self.assertEqual(result["error"], "INVALID_REQUEST")

    def test_load_execution_allowlist_missing_is_empty(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(transport.load_execution_allowlist(Path(d) / "missing.json"), frozenset())

    def test_load_execution_allowlist_rejects_unverified(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "execution.json"
            p.write_text('{"execute_operations":["smoke"]}')
            os.chmod(p, 0o666)
            with mock.patch.object(transport, "_trusted_chain", return_value=True):
                with self.assertRaises(RuntimeError):
                    transport.load_execution_allowlist(p)

    def test_load_execution_allowlist_valid(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "execution.json"
            p.write_text('{"execute_operations":["smoke","status"]}')
            os.chmod(p, 0o600)
            with mock.patch.object(transport, "_trusted_chain", return_value=True), \
                 mock.patch.object(transport, "_secure_file", return_value=True):
                self.assertEqual(
                    transport.load_execution_allowlist(p),
                    frozenset({"smoke", "status"}),
                )

    def test_load_execution_allowlist_rejects_extra_key(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "execution.json"
            p.write_text('{"execute_operations":["smoke"],"extra":true}')
            with mock.patch.object(transport, "_trusted_chain", return_value=True), \
                 mock.patch.object(transport, "_secure_file", return_value=True):
                with self.assertRaises(RuntimeError):
                    transport.load_execution_allowlist(p)

    def test_load_execution_allowlist_rejects_invalid_operation(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "execution.json"
            p.write_text('{"execute_operations":["smoke","exec"]}')
            with mock.patch.object(transport, "_trusted_chain", return_value=True), \
                 mock.patch.object(transport, "_secure_file", return_value=True):
                with self.assertRaises(RuntimeError):
                    transport.load_execution_allowlist(p)

    def test_load_execution_allowlist_rejects_non_object(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "execution.json"
            p.write_text('["smoke"]')
            with mock.patch.object(transport, "_trusted_chain", return_value=True), \
                 mock.patch.object(transport, "_secure_file", return_value=True):
                with self.assertRaises(RuntimeError):
                    transport.load_execution_allowlist(p)


if __name__ == "__main__":
    unittest.main()
