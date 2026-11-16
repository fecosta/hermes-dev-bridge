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
        from hermes_dev_bridge import _SCHEMA
        self.assertEqual(_SCHEMA["parameters"]["additionalProperties"], False)
        with open("hermes_dev_bridge/tools.py") as source:
            self.assertNotIn("subprocess", source.read())


if __name__ == "__main__": unittest.main()
