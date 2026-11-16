import os, tempfile, threading, unittest
from pathlib import Path
from ledger import Ledger, RequestReuse

class ConcurrentLedgerTests(unittest.TestCase):
    def test_same_id_serializes_once_and_changed_payload_reuses(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'runs.sqlite3'; barrier=threading.Barrier(2); results=[]
            def reserve(req):
                l=Ledger(path,os.getuid(),os.getgid()); barrier.wait()
                try: results.append(('ok',l.reserve(req)['state']))
                except Exception as e: results.append((type(e).__name__,str(e)))
                finally: l.close()
            req={'request_id':'same','operation':'smoke','repository':'fixture','timeout_seconds':5}
            ts=[threading.Thread(target=reserve,args=(req,)) for _ in range(2)]
            [t.start() for t in ts]; [t.join() for t in ts]
            self.assertEqual(results.count(('ok','accepted')),2)
            l=Ledger(path,os.getuid(),os.getgid())
            with self.assertRaises(RequestReuse): l.reserve({**req,'operation':'status'})
            self.assertEqual(l.db.execute('select count(*) from runs').fetchone()[0],1); l.close()

if __name__=='__main__': unittest.main()
