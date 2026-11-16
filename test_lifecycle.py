import json, os, socket, stat, tempfile, threading, unittest
from pathlib import Path
from unittest import mock
import lifecycle
from ledger import Ledger

class LifecycleTests(unittest.TestCase):
    def test_peer_config_requires_root_secure_file(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'transport.json'; p.write_text('{"hermes_uid":123}')
            os.chmod(p,0o600)
            with mock.patch.object(lifecycle,'_secure_file',return_value=False):
                with self.assertRaises(lifecycle.StartupError): lifecycle.load_peer_uid(p)
    def test_peer_config_valid(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'transport.json'; p.write_text('{"hermes_uid":123}')
            os.chmod(p,0o600)
            with mock.patch.object(lifecycle,'_secure_file',return_value=True): self.assertEqual(lifecycle.load_peer_uid(p),frozenset({123}))
    def test_identity_and_socket_validation(self):
        with self.assertRaises(lifecycle.StartupError): lifecycle.validate_identity(1,2,[2],3,4)
        with mock.patch.dict(os.environ, {'LISTEN_PID':str(os.getpid()),'LISTEN_FDS':'0'}):
            with self.assertRaises(lifecycle.StartupError): lifecycle.activated_listener()
    def test_recovery_marks_only_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            l=Ledger(Path(d)/'runs.sqlite3',os.getuid(),os.getgid()); r={'request_id':'r','operation':'smoke','repository':'fixture','timeout_seconds':5}; l.reserve(r)
            lifecycle.recover_acknowledgements(l)
            self.assertEqual(l.get('r')['state'],'recovery-required'); self.assertEqual(l.get('r')['error_code'],'SERVICE_INTERRUPTED'); l.close()
    def test_trusted_chain_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            real=Path(d)/'real'; real.write_text('x'); link=Path(d)/'link'; link.symlink_to(real)
            self.assertFalse(lifecycle._trusted_chain(link))

    def test_policy_identity_validation(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'policy'; p.write_text('{"uid":1,"gid":2,"workspace":"/tmp"}'); os.chmod(p,0o600)
            with mock.patch.object(lifecycle,'_secure_file',return_value=True), mock.patch.object(lifecycle,'_trusted_chain',return_value=True):
                with self.assertRaises(lifecycle.StartupError): lifecycle.validate_policy(p,3,4)

    def test_startup_validates_trusted_code_before_ledger(self):
        with mock.patch.object(lifecycle,'validate_identity'), mock.patch.object(lifecycle,'validate_application_files',side_effect=lifecycle.StartupError('TRUSTED_CODE_UNVERIFIED')), mock.patch('lifecycle.Ledger',side_effect=AssertionError):
            with self.assertRaises(lifecycle.StartupError): lifecycle.startup(Path('/unused'),997,988)

    def test_no_execution_in_lifecycle_path(self):
        with tempfile.TemporaryDirectory() as d:
            l=Ledger(Path(d)/'runs.sqlite3',os.getuid(),os.getgid())
            with mock.patch('transport.bridge.execute',side_effect=AssertionError):
                lifecycle.recover_acknowledgements(l)
            l.close()

    def test_shutdown_with_blocked_client_is_bounded(self):
        left, right = socket.socketpair(); stop=threading.Event()
        listener = mock.Mock(); listener.settimeout.return_value=None; listener.close.return_value=None
        calls=[(right, None)]
        def accept():
            if calls: return calls.pop()
            raise socket.timeout()
        listener.accept.side_effect=accept
        thread=threading.Thread(target=lifecycle.serve_activated, args=(listener, frozenset({os.getuid()}), mock.Mock(), stop))
        left.sendall(b'{"request_id":"blocked","operation":"smoke"')
        thread.start(); stop.set(); thread.join(3)
        self.assertFalse(thread.is_alive()); left.close()

if __name__=='__main__': unittest.main()
