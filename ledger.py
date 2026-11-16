"""Restart-safe, bounded SQLite ledger for the acknowledgement-only transport."""
from __future__ import annotations
import hashlib, json, os, secrets, sqlite3, stat, time
from pathlib import Path
from typing import Any

MAX_RECORDS = 1000
MAX_EVIDENCE = 16384
MAX_STORAGE = 4 * 1024 * 1024
CAPACITY_RESERVE = 64 * 1024  # conservative headroom for one SQLite transaction
RETENTION_SECONDS = 30 * 86400
STATES = {"accepted", "acknowledged", "running", "passed", "failed", "blocked", "recovery-required"}
TERMINAL = {"acknowledged", "passed", "failed", "blocked", "recovery-required"}
TRANSITIONS = {"accepted": {"running", "acknowledged", "recovery-required"}, "running": TERMINAL}

class LedgerError(Exception): pass
class LedgerFull(LedgerError): pass
class RequestReuse(LedgerError): pass
class InvalidTransition(LedgerError): pass

_SCHEMA = """CREATE TABLE IF NOT EXISTS runs (
 request_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, operation TEXT NOT NULL,
 repository TEXT NOT NULL, timeout_seconds INTEGER NOT NULL, state TEXT NOT NULL,
 submitted_at REAL NOT NULL, started_at REAL, ended_at REAL, exit_code INTEGER,
 evidence TEXT, error_code TEXT, cleanup_status TEXT, boot_id TEXT NOT NULL,
 CHECK(state IN ('accepted','acknowledged','running','passed','failed','blocked','recovery-required'))
)"""


def canonical_hash(request: dict[str, Any]) -> str:
    body = {k: request[k] for k in ("request_id", "operation", "repository", "timeout_seconds") if k in request}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _safe_text(value: Any) -> str | None:
    if value is None: return None
    return str(value)[:MAX_EVIDENCE]

class Ledger:
    def __init__(self, path: str | Path, owner_uid: int | None = None, owner_gid: int | None = None):
        self.path = Path(path)
        self.owner_uid, self.owner_gid = owner_uid, owner_gid
        self._check_path()
        self.boot_id = secrets.token_hex(16)
        try:
            old_umask = os.umask(0o077)
            try:
                self.db = sqlite3.connect(self.path, timeout=2, isolation_level=None, check_same_thread=False)
            finally:
                os.umask(old_umask)
            os.chmod(self.path, 0o600)
            self.db.execute("PRAGMA busy_timeout=2000")
            self.db.execute("PRAGMA journal_mode=DELETE")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute(_SCHEMA)
        except (OSError, sqlite3.Error) as exc:
            raise LedgerError("LEDGER_UNAVAILABLE") from exc
        self._check_capacity()

    def _check_path(self):
        try:
            parent = self.path.parent
            st = parent.lstat()
            if not stat.S_ISDIR(st.st_mode) or st.st_mode & 0o022: raise LedgerError("LEDGER_PATH_UNSAFE")
            if self.owner_uid is not None and (st.st_uid != self.owner_uid or (self.owner_gid is not None and st.st_gid != self.owner_gid)):
                raise LedgerError("LEDGER_PATH_UNSAFE")
            if self.path.exists():
                s = self.path.lstat()
                if not stat.S_ISREG(s.st_mode) or s.st_mode & 0o077: raise LedgerError("LEDGER_FILE_UNSAFE")
                if self.owner_uid is not None and (s.st_uid != self.owner_uid or (self.owner_gid is not None and s.st_gid != self.owner_gid)):
                    raise LedgerError("LEDGER_FILE_UNSAFE")
        except OSError as exc: raise LedgerError("LEDGER_UNAVAILABLE") from exc

    def _files_size(self):
        return sum(p.stat().st_size for p in self.path.parent.glob(self.path.name + "*") if p.is_file())
    def _check_capacity(self):
        if self._files_size() + CAPACITY_RESERVE > MAX_STORAGE: raise LedgerFull("LEDGER_FULL")
        if self.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0] >= MAX_RECORDS:
            self.retain()
        if self.db.execute("SELECT COUNT(*) FROM runs").fetchone()[0] >= MAX_RECORDS: raise LedgerFull("LEDGER_FULL")

    def _check_growth(self):
        if self._files_size() + CAPACITY_RESERVE > MAX_STORAGE: raise LedgerFull("LEDGER_FULL")

    def retain(self, now=None):
        now = time.time() if now is None else now
        cutoff = now - RETENTION_SECONDS
        try:
            self.db.execute("DELETE FROM runs WHERE state IN ('acknowledged','passed','failed','blocked') AND ended_at < ?", (cutoff,))
            # DELETE journaling avoids WAL growth. Do not VACUUM: its temporary
            # copy can exceed the configured storage ceiling. Reclaiming pages
            # is deferred to an operator-controlled maintenance window.
        except sqlite3.Error as exc: raise LedgerError("LEDGER_UNAVAILABLE") from exc

    def reserve(self, request: dict[str, Any]) -> dict[str, Any]:
        h = canonical_hash(request)
        self._check_capacity()
        try:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute("SELECT * FROM runs WHERE request_id=?", (request["request_id"],)).fetchone()
            if row:
                self.db.execute("COMMIT")
                if row[1] != h: raise RequestReuse("REQUEST_ID_REUSE")
                return self._row(row)
            self._check_growth()
            self.db.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (request["request_id"], h, request["operation"], request["repository"], request.get("timeout_seconds", 5), "accepted", time.time(), None, None, None, None, None, None, self.boot_id))
            self.db.execute("COMMIT")
            return self.get(request["request_id"])
        except RequestReuse: raise
        except (sqlite3.Error, OSError) as exc:
            try: self.db.execute("ROLLBACK")
            except sqlite3.Error: pass
            raise LedgerError("LEDGER_UNAVAILABLE") from exc

    def transition(self, request_id, state, **fields):
        if state not in STATES: raise InvalidTransition("INVALID_STATE")
        row = self.get(request_id)
        if state not in TRANSITIONS.get(row["state"], set()): raise InvalidTransition("INVALID_TRANSITION")
        now = time.time(); started = now if state == "running" else row["started_at"]
        ended = now if state in TERMINAL else row["ended_at"]
        evidence = _safe_text(fields.get("evidence", row["evidence"]))
        error = _safe_text(fields.get("error_code", row["error_code"]))
        cleanup = _safe_text(fields.get("cleanup_status", row["cleanup_status"]))
        try:
            self.db.execute("UPDATE runs SET state=?,started_at=?,ended_at=?,exit_code=?,evidence=?,error_code=?,cleanup_status=? WHERE request_id=?", (state, started, ended, fields.get("exit_code", row["exit_code"]), evidence, error, cleanup, request_id))
            return self.get(request_id)
        except sqlite3.Error as exc: raise LedgerError("LEDGER_UNAVAILABLE") from exc

    def get(self, request_id):
        try:
            row = self.db.execute("SELECT * FROM runs WHERE request_id=?", (request_id,)).fetchone()
        except sqlite3.Error as exc: raise LedgerError("LEDGER_UNAVAILABLE") from exc
        if not row: raise LedgerError("REQUEST_NOT_FOUND")
        return self._row(row)

    def incomplete(self):
        try: return [self._row(r) for r in self.db.execute("SELECT * FROM runs WHERE state IN ('accepted','running') ORDER BY submitted_at").fetchall()]
        except sqlite3.Error as exc: raise LedgerError("LEDGER_UNAVAILABLE") from exc

    @staticmethod
    def _row(r):
        keys = ("request_id","request_hash","operation","repository","timeout_seconds","state","submitted_at","started_at","ended_at","exit_code","evidence","error_code","cleanup_status","boot_id")
        return dict(zip(keys, r))
    def close(self): self.db.close()
