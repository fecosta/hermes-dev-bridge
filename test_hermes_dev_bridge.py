import json
import unittest
from unittest.mock import patch

from hermes_dev_bridge import tools


class Conn:
    def __init__(self, response): self.response, self.sent = response, None
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def settimeout(self, value): pass
    def connect(self, path): self.path = path
    def sendall(self, data): self.sent = data
    def recv(self, size):
        data, self.response = self.response, b""
        return data


class SmokeTests(unittest.TestCase):
    def call(self, response):
        conn = Conn(response)
        with patch.object(tools.socket, "socket", return_value=conn), patch.object(tools.secrets, "token_hex", return_value="a" * 32):
            result = json.loads(tools.bridge_smoke({}))
        return result, conn

    def test_acknowledgement_and_fixed_request(self):
        result, conn = self.call(b'{"status":"acknowledged","request_id":"hermes-dev-bridge-' + b'a'*32 + b'","operation":"smoke","repository":"fixture","state":"acknowledged","evidence":"not executed; dispatch unavailable"}\n')
        self.assertEqual(result["executed"], False)
        self.assertEqual(json.loads(conn.sent), {"request_id": "hermes-dev-bridge-" + "a"*32, "operation": "smoke", "repository": "fixture", "timeout_seconds": 5})

    def test_rejected_and_malformed(self):
        for response, error in [(b'{"status":"rejected","error":"UNAUTHORIZED_PEER"}\n', "SERVER_REJECTED"), (b"nope\n", "MALFORMED_RESPONSE")]:
            self.assertEqual(self.call(response)[0]["error"], error)

    def test_unavailable(self):
        with patch.object(tools.socket, "socket", side_effect=FileNotFoundError):
            self.assertEqual(json.loads(tools.bridge_smoke({}))["error"], "SOCKET_UNAVAILABLE")

    def test_oversized(self):
        self.assertEqual(self.call(b"x" * tools._MAX_FRAME)[0]["error"], "OVERSIZED_RESPONSE")

    def test_no_args_or_overrides(self):
        from hermes_dev_bridge import _ACK_SCHEMA
        self.assertEqual(_ACK_SCHEMA["parameters"]["additionalProperties"], False)
        with open("hermes_dev_bridge/tools.py") as source:
            self.assertNotIn("subprocess", source.read())


class ExecuteSmokeTests(unittest.TestCase):
    def call(self, response):
        conn = Conn(response)
        with patch.object(tools.socket, "socket", return_value=conn), patch.object(tools.secrets, "token_hex", return_value="a" * 32):
            result = json.loads(tools.bridge_execute_smoke({}))
        return result, conn

    def _base_response(self, status, state, **extra):
        response = {
            "status": status,
            "request_id": "hermes-dev-bridge-" + "a" * 32,
            "operation": "smoke",
            "repository": "fixture",
            "state": state,
            "evidence": "fixture run",
        }
        response.update(extra)
        return (json.dumps(response, separators=(",", ":")) + "\n").encode("ascii")

    def test_successful_execution(self):
        result, conn = self.call(self._base_response("completed", "passed", exit_code=0))
        self.assertTrue(result["success"])
        self.assertTrue(result["executed"])
        self.assertEqual(result["state"], "passed")
        self.assertEqual(result["request_id"], "hermes-dev-bridge-" + "a" * 32)
        self.assertEqual(json.loads(conn.sent), {
            "request_id": "hermes-dev-bridge-" + "a" * 32,
            "operation": "smoke",
            "repository": "fixture",
            "timeout_seconds": 5,
            "execute": True,
        })

    def test_operator_execution_disabled(self):
        result, _ = self.call(self._base_response("completed", "blocked", error_code="EXECUTION_DISABLED"))
        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["error"], "EXECUTION_DISABLED")
        self.assertEqual(result["state"], "blocked")

    def test_grant_exhausted(self):
        result, _ = self.call(self._base_response("completed", "blocked", error_code="GRANT_EXHAUSTED"))
        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["error"], "GRANT_EXHAUSTED")
        self.assertEqual(result["state"], "blocked")

    def test_unauthorized_blocked_operation(self):
        result, _ = self.call(self._base_response("completed", "blocked", error_code="IDENTITY_MISMATCH"))
        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["error"], "IDENTITY_MISMATCH")
        self.assertEqual(result["state"], "blocked")

    def test_execution_failure(self):
        result, _ = self.call(self._base_response("completed", "failed", exit_code=1, error_code="FIXTURE_FAILED"))
        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["error"], "FIXTURE_FAILED")
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["exit_code"], 1)

    def test_execution_timeout(self):
        result, _ = self.call(self._base_response("completed", "failed", exit_code=124))
        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["error"], "EXECUTION_TIMEOUT")
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["exit_code"], 124)

    def test_recovery_required(self):
        result, _ = self.call(self._base_response("completed", "recovery-required", error_code="EXECUTION_UNCERTAIN"))
        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertEqual(result["error"], "EXECUTION_UNCERTAIN")
        self.assertEqual(result["state"], "recovery-required")

    def test_malformed_or_oversized_response(self):
        for response in [b"not-json\n", b"x" * tools._MAX_FRAME]:
            result, _ = self.call(response)
            self.assertFalse(result["success"])
            self.assertIn(result["error"], {"MALFORMED_RESPONSE", "OVERSIZED_RESPONSE"})

    def test_mismatched_request_id(self):
        response = self._base_response("completed", "passed", exit_code=0)
        response = response.replace(b"hermes-dev-bridge-" + b"a" * 32, b"hermes-dev-bridge-other")
        result, _ = self.call(response)
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "MALFORMED_RESPONSE")

    def test_connection_failure(self):
        with patch.object(tools.socket, "socket", side_effect=FileNotFoundError):
            result = json.loads(tools.bridge_execute_smoke({}))
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "SOCKET_UNAVAILABLE")

    def test_no_retries_or_repeated_dispatch(self):
        conn = Conn(self._base_response("completed", "passed", exit_code=0))
        with patch.object(tools.socket, "socket", return_value=conn), patch.object(tools.secrets, "token_hex", return_value="a" * 32):
            json.loads(tools.bridge_execute_smoke({}))
        self.assertEqual(conn.sent.count(b"execute\":true"), 1)

    def test_rejected_response_from_bridge(self):
        result, _ = self.call(b'{"status":"rejected","error":"REQUEST_ID_REUSE"}\n')
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "REQUEST_ID_REUSE")

    def test_no_args_or_overrides(self):
        from hermes_dev_bridge import _EXEC_SCHEMA
        self.assertEqual(_EXEC_SCHEMA["parameters"]["additionalProperties"], False)


if __name__ == "__main__": unittest.main()
