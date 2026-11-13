import json
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import bridge


class BridgeTests(unittest.TestCase):
    def req(self, **kw):
        request = {"run_id": "run-1", "operation": "smoke", "repository": "fixture"}
        request.update(kw)
        return request

    def test_valid_operations_block_without_worker(self):
        for i, operation in enumerate(("preflight", "status", "smoke")):
            self.assertEqual(bridge.execute(self.req(run_id=f"ok-{i}", operation=operation))["status"], "blocked")

    def test_invalid_inputs_and_policy_not_in_payload(self):
        self.assertEqual(bridge.execute(self.req(repository="pfy"))["error"], "UNSUPPORTED_REPOSITORY")
        self.assertEqual(bridge.execute(self.req(operation="exec"))["error"], "UNSUPPORTED_OPERATION")
        for field in ("command", "path", "env", "model", "uid", "gid", "policy"):
            self.assertEqual(bridge.execute(self.req(**{field: "x"}))["error"], "UNSUPPORTED_FIELD")

    def test_duplicate_and_limits(self):
        bridge.execute(self.req(run_id="dup"))
        self.assertEqual(bridge.execute(self.req(run_id="dup"))["error"], "DUPLICATE_RUN_ID")
        self.assertEqual(bridge.execute(self.req(timeout_seconds=0))["error"], "INVALID_TIMEOUT")
        self.assertEqual(bridge.execute(self.req(run_id="bad space"))["error"], "INVALID_RUN_ID")

    def test_local_fixture_success_and_workspace_containment(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with mock.patch.object(bridge, "_workspace_root", return_value=root):
                code, evidence = bridge._run_fixture("smoke", 5)
            self.assertEqual((code, evidence), (0, "fixture-ok"))
            self.assertEqual(list(root.iterdir()), [])

    def test_timeout_kills_descendant_process_group(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            with mock.patch.object(bridge, "_workspace_root", return_value=root):
                code, evidence = bridge._run_fixture("_timeout", 1)
            self.assertEqual(code, 124)
            self.assertEqual(evidence, "TIMEOUT; child group terminated")
            self.assertEqual(list(root.iterdir()), [])

    def _descendant_script(self, pidfile, hold=True):
        return ("import subprocess,sys,time\n"
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
                f"open({str(pidfile)!r},'w').write(str(p.pid))\n"
                + ("time.sleep(60)" if hold else "print('done')"))

    def _alive(self, pid):
        try:
            return Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()[0] != "Z"
        except OSError:
            return False

    def test_real_descendant_is_dead_after_timeout_and_after_normal_exit(self):
        for hold, expect in ((True, 124), (False, 0)):
            with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
                pidfile = Path(out) / "pid"
                with mock.patch.object(bridge, "_workspace_root", return_value=Path(root)), \
                     mock.patch.object(bridge, "_fixture_script", return_value=self._descendant_script(pidfile, hold)):
                    code, _ = bridge._run_fixture("smoke", 1)
                self.assertEqual(code, expect)
                pid = int(pidfile.read_text())
                self.assertFalse(self._alive(pid))

    def test_group_leader_unreaped_when_signaled(self):
        # Leader must still own its PID (alive or zombie) at killpg time, else the PGID could be reused.
        seen = []
        real = os.killpg
        def spy(pgid, sig):
            seen.append(Path(f"/proc/{pgid}/stat").exists())
            return real(pgid, sig)
        for op, timeout, expect in (("smoke", 5, 0), ("_timeout", 1, 124)):
            with tempfile.TemporaryDirectory() as root, \
                 mock.patch.object(bridge, "_workspace_root", return_value=Path(root)), \
                 mock.patch.object(bridge.os, "killpg", side_effect=spy):
                code, _ = bridge._run_fixture(op, timeout)
            self.assertEqual(code, expect)
        self.assertEqual(seen, [True, True])

    def test_killpg_error_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(bridge, "_workspace_root", return_value=Path(root)), \
                 mock.patch.object(bridge, "_terminate_group", side_effect=PermissionError("EPERM")):
                with self.assertRaises(bridge.CleanupError) as error:
                    bridge._run_fixture("smoke", 1)
            self.assertEqual(str(error.exception), "PROCESS_GROUP_CLEANUP_FAILED")
            self.assertEqual(list(Path(root).iterdir()), [])
            self.assertFalse(error.exception.artifacts.get("process_group_pids"))

    def test_surviving_group_member_fails_closed(self):
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(bridge, "_workspace_root", return_value=Path(root)), \
             mock.patch.object(bridge, "KILL_WAIT", 0.1), \
             mock.patch.object(bridge, "_group_pids", return_value=[4242]):
            with self.assertRaises(bridge.CleanupError) as error:
                bridge._run_fixture("smoke", 5)
        self.assertEqual(error.exception.artifacts["process_group_pids"], [4242])

    def test_public_execute_cleanup_failure_reports_artifacts(self):
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(bridge, "isolation_status", return_value=(True, "IDENTITY_VERIFIED")), \
             mock.patch.object(bridge, "_workspace_root", return_value=Path(root)), \
             mock.patch.object(bridge.shutil, "rmtree", side_effect=OSError("denied")):
            result = bridge.execute(self.req(run_id="cleanup-fail"))
            leftover = [p.name for p in Path(root).iterdir()]
        self.assertEqual((result["status"], result["error"], result["cleanup"]), ("failed", "WORKSPACE_CLEANUP_FAILED", "failed"))
        self.assertEqual(result["fixture_exit_code"], 0)
        self.assertEqual(result["artifacts"]["workspace"], leftover[0])
        self.assertEqual(bridge.RUNS["cleanup-fail"], result)

    def test_run_registry_limit(self):
        with mock.patch.object(bridge, "MAX_RUNS", len(bridge.RUNS)):
            self.assertEqual(bridge.execute(self.req(run_id="over-limit"))["error"], "RUN_REGISTRY_FULL")
        self.assertNotIn("over-limit", bridge.RUNS)

    def test_workspace_chain_rejects_symlinks_and_loose_parents(self):
        with tempfile.TemporaryDirectory() as base:
            base = Path(base)
            me, grp = os.getuid(), os.getgid()
            (base / "a").mkdir(mode=0o755); (base / "a" / "ws").mkdir(mode=0o700)
            ws = base / "a" / "ws"
            self.assertTrue(bridge._chain_ok(ws, me, grp, me, base))
            (base / "link").symlink_to(base / "a")
            self.assertFalse(bridge._chain_ok(base / "link" / "ws", me, grp, me, base))
            os.chmod(base / "a", 0o775)
            self.assertFalse(bridge._chain_ok(ws, me, grp, me, base))
            os.chmod(base / "a", 0o755); os.chmod(ws, 0o750)
            self.assertFalse(bridge._chain_ok(ws, me, grp, me, base))
            os.chmod(ws, 0o700)
            self.assertFalse(bridge._chain_ok(ws, me + 1, grp, me, base))
            self.assertFalse(bridge._chain_ok(ws, me, grp, me + 1, base))

    def test_cleanup_failure_is_observable(self):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.object(bridge, "_workspace_root", return_value=Path(root)), \
                 mock.patch.object(bridge.shutil, "rmtree", side_effect=OSError("denied")):
                with self.assertRaises(bridge.CleanupError) as error:
                    bridge._run_fixture("smoke", 5)
            self.assertEqual(str(error.exception), "WORKSPACE_CLEANUP_FAILED")

    def test_trusted_policy_checks_numeric_identity(self):
        self.assertNotIn("HERMES_BRIDGE_WORKER_USER", bridge.isolation_status.__code__.co_names)

    def test_sanitization(self):
        value = bridge._sanitize("api_key=secret-value")
        self.assertIn("[REDACTED]", value)
        self.assertNotIn("secret-value", value)


if __name__ == "__main__":
    unittest.main()
