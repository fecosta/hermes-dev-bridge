"""Fail-closed lifecycle checks for the systemd-activated acknowledgement service."""
from __future__ import annotations
import json, os, signal, socket, stat, struct, threading
from pathlib import Path
from typing import Any
import transport
from ledger import Ledger, LedgerError

CONFIG_PATH = Path('/etc/hermes-dev-bridge/transport.json')
SOCKET_FDS = 1

class StartupError(RuntimeError): pass

def _secure_file(path: Path, owner: int = 0) -> bool:
    try:
        s = path.lstat()
        return stat.S_ISREG(s.st_mode) and s.st_uid == owner and not s.st_mode & 0o022
    except OSError: return False

def load_peer_uid(path: Path = CONFIG_PATH) -> frozenset[int]:
    if not _secure_file(path): raise StartupError('PEER_CONFIG_UNVERIFIED')
    try:
        value = json.loads(path.read_text())
        if set(value) != {'hermes_uid'} or not isinstance(value['hermes_uid'], int) or value['hermes_uid'] < 1:
            raise StartupError('PEER_CONFIG_UNVERIFIED')
        return frozenset({value['hermes_uid']})
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise StartupError('PEER_CONFIG_UNVERIFIED') from exc

def validate_identity(uid: int, gid: int, groups: list[int], worker_uid: int, worker_gid: int) -> None:
    if (uid, gid, groups) != (worker_uid, worker_gid, [worker_gid]):
        raise StartupError('IDENTITY_UNVERIFIED')

def activated_listener(fd: int = 3) -> socket.socket:
    try:
        if int(os.environ.get('LISTEN_PID', '0')) != os.getpid() or int(os.environ.get('LISTEN_FDS', '0')) != SOCKET_FDS:
            raise StartupError('SOCKET_ACTIVATION_UNVERIFIED')
        s = socket.socket(fileno=fd)
        if s.family != socket.AF_UNIX or s.type != socket.SOCK_STREAM:
            raise StartupError('SOCKET_ACTIVATION_UNVERIFIED')
        return s
    except (OSError, ValueError) as exc:
        raise StartupError('SOCKET_ACTIVATION_UNVERIFIED') from exc

def _trusted_chain(path: Path) -> bool:
    try:
        current = path
        if current.is_symlink() or not current.is_file(): return False
        while True:
            s = current.lstat()
            if current != path and (s.st_uid != 0 or s.st_mode & 0o022): return False
            if current == current.parent: return True
            current = current.parent
    except OSError: return False


def validate_policy(policy_path: Path, worker_uid: int, worker_gid: int) -> Path:
    if not _secure_file(policy_path) or not _trusted_chain(policy_path):
        raise StartupError('POLICY_UNVERIFIED')
    try:
        value = json.loads(policy_path.read_text())
        if set(value) != {'uid', 'gid', 'workspace'}: raise StartupError('POLICY_UNVERIFIED')
        if value['uid'] != worker_uid or value['gid'] != worker_gid or worker_uid == 0 or worker_gid == 0:
            raise StartupError('POLICY_UNVERIFIED')
        workspace = Path(value['workspace'])
        if not workspace.is_absolute() or not transport.bridge._chain_ok(workspace, worker_uid, worker_gid):
            raise StartupError('POLICY_UNVERIFIED')
        return workspace
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        raise StartupError('POLICY_UNVERIFIED') from exc


def validate_application_files() -> None:
    for module in (Path(transport.__file__), Path(__file__), Path(transport.bridge.__file__), Path(__import__('ledger').__file__)):
        if not _trusted_chain(module) or not _secure_file(module):
            raise StartupError('TRUSTED_CODE_UNVERIFIED')


def startup(ledger_path: Path, worker_uid: int, worker_gid: int, config_path: Path = CONFIG_PATH, policy_path: Path = transport.bridge.POLICY_PATH, execution_config_path: Path = transport.EXECUTION_CONFIG_PATH):
    validate_identity(os.geteuid(), os.getegid(), os.getgroups(), worker_uid, worker_gid)
    validate_application_files()
    validate_policy(policy_path, worker_uid, worker_gid)
    peers = load_peer_uid(config_path)
    execution_grants = transport.load_execution_allowlist(execution_config_path)
    ledger = Ledger(ledger_path, worker_uid, worker_gid)
    ledger.load_grants(execution_grants)
    return peers, ledger, execution_grants

def recover_acknowledgements(ledger: Ledger) -> None:
    # U3.3 acknowledgements are terminal. Only genuinely incomplete future
    # states are marked; this service never dispatches old work.
    for row in ledger.incomplete():
        if row['state'] == 'running':
            evidence = 'running request interrupted; execution outcome unknown; operator recovery required'
        else:
            evidence = 'accepted request interrupted before dispatch; not executed; operator recovery required'
        ledger.transition(row['request_id'], 'recovery-required', error_code='SERVICE_INTERRUPTED', evidence=evidence)

def serve_activated(listener: socket.socket, authorized_uids: frozenset[int], ledger: Ledger, stop=None):
    listener.settimeout(0.2)
    while not stop or not stop.is_set():
        try: conn, _ = listener.accept()
        except socket.timeout: continue
        transport.handle_connection(conn, authorized_uids, ledger)
    listener.close()


def main() -> int:
    stop = threading.Event()
    def stop_now(signum, frame): stop.set()
    signal.signal(signal.SIGTERM, stop_now)
    signal.signal(signal.SIGINT, stop_now)
    peers, ledger, _execution_grants = startup(Path(transport.LEDGER_PATH), 997, 988)
    try:
        recover_acknowledgements(ledger)
        listener = activated_listener()
        serve_activated(listener, peers, ledger, stop)
    finally:
        ledger.close()
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
