import os, tempfile, threading, unittest
from pathlib import Path
from ledger import Ledger, LedgerError, RequestReuse

class LedgerTests(unittest.TestCase):
    def make(self, d):
        p=Path(d)/'runs.sqlite3'; return Ledger(p, os.getuid(), os.getgid()), p
    def req(self, **kw):
        x={'request_id':'r1','operation':'smoke','repository':'fixture','timeout_seconds':5}; x.update(kw); return x
    def test_persist_restart_and_idempotency(self):
        with tempfile.TemporaryDirectory() as d:
            l,p=self.make(d); row=l.reserve(self.req()); self.assertEqual(row['state'],'accepted'); l.transition('r1','running'); l.transition('r1','passed',exit_code=0,evidence='fixture-ok'); l.close()
            l,_=self.make(d); self.assertEqual(l.reserve(self.req())['exit_code'],0); l.close()
    def test_reuse_and_transitions(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.make(d); l.reserve(self.req())
            with self.assertRaises(RequestReuse): l.reserve(self.req(operation='status'))
            with self.assertRaises(Exception): l.transition('r1','passed')
            l.close()
    def test_incomplete_and_sanitized_bound(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.make(d); l.reserve(self.req()); self.assertEqual(len(l.incomplete()),1); l.transition('r1','running'); l.transition('r1','failed',evidence='x'*20000); self.assertLessEqual(len(l.get('r1')['evidence']),16384); l.close()
    def test_unsafe_permissions(self):
        with tempfile.TemporaryDirectory() as d:
            os.chmod(d,0o775)
            with self.assertRaises(LedgerError): self.make(d)

    def test_load_grants_marks_consumed_unavailable(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.make(d)
            l.load_grants([{"grant_id":"g1","operation":"smoke"}])
            self.assertEqual(l.consume_grant("smoke","r1"),"g1")
            l.load_grants([{"grant_id":"g1","operation":"smoke"}])
            self.assertIsNone(l.consume_grant("smoke","r2"))
            l.close()

    def test_load_grants_removes_unconfigured_grants(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.make(d)
            l.load_grants([{"grant_id":"g1","operation":"smoke"},{"grant_id":"g2","operation":"smoke"}])
            self.assertEqual(l.consume_grant("smoke","r1"),"g1")
            l.load_grants([{"grant_id":"g2","operation":"smoke"}])
            self.assertEqual(l.consume_grant("smoke","r2"),"g2")
            self.assertIsNone(l.consume_grant("smoke","r3"))
            l.close()

    def test_consume_grant_atomic(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'runs.sqlite3'
            l=Ledger(path,os.getuid(),os.getgid())
            l.load_grants([{"grant_id":"g1","operation":"smoke"}])
            l.close()
            results=[]
            def attempt():
                lx=Ledger(path,os.getuid(),os.getgid())
                results.append(lx.consume_grant("smoke", "rx"))
                lx.close()
            threads=[threading.Thread(target=attempt) for _ in range(4)]
            [t.start() for t in threads]
            [t.join() for t in threads]
            self.assertEqual(sum(1 for r in results if r is not None),1)
            self.assertEqual(sum(1 for r in results if r == "g1"),1)

if __name__=='__main__': unittest.main()
