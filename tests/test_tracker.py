import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from orchestration.tracker import Tracker
from orchestration.artifacts import seal, verify


class TrackerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.tracker = Tracker(self.root / 'tracker.sqlite')
        self.addCleanup(self.tracker.db.close)
        self.tracker.job('job', {'test': True})

    def accept(self, bit, start=10, stop=20, config='a'):
        self.tracker.accept('job', [('address', bit, config, start, stop)], {'verified_empty': True})

    def test_flags_empty_and_duplicates(self):
        for bit, expected in [(1, 1), (2, 3), (4, 7), (4, 7)]:
            self.accept(bit)
            self.assertEqual(self.tracker.status('address', 12, {1:'a',2:'a',4:'a'}), expected)

    def test_interval_overlap_and_gap(self):
        self.accept(1, 10, 15)
        self.accept(1, 17, 22)
        self.assertEqual(list(self.tracker.missing('address',1,'a',9,25)), [(9,10),(15,17),(22,25)])
        self.accept(1, 14, 18)
        self.assertEqual(list(self.tracker.missing('address',1,'a',10,22)), [])
        self.assertEqual(self.tracker.db.execute('SELECT count(*) FROM coverage').fetchone()[0],1)

    def test_configuration_isolation(self):
        self.accept(1)
        self.accept(2)
        self.assertEqual(self.tracker.status('address',12,{1:'changed',2:'a',4:'a'}),2)

    def test_failed_job_does_not_complete(self):
        self.tracker.stage('job', 'failed', 'transfer failed')
        self.assertEqual(self.tracker.status('address',12,{1:'a',2:'a',4:'a'}),0)

    def test_atomic_failure(self):
        with self.assertRaises(ValueError):
            self.tracker.accept('job',[('address',1,'a',10,20),('address',7,'a',10,20)],{})
        self.assertEqual(self.tracker.db.execute('SELECT count(*) FROM coverage').fetchone()[0],0)

    def test_invalidation_preserves_sides(self):
        self.accept(1)
        self.tracker.invalidate('address',12,15,'reorg')
        self.assertEqual(list(self.tracker.missing('address',1,'a',10,20)),[(12,15)])

    def test_restart_and_backup(self):
        self.accept(4)
        self.tracker.backup(self.root/'backup.sqlite')
        restored=Tracker(self.root/'backup.sqlite')
        try:
            self.assertEqual(restored.status('address',12,{4:'a'}),4)
        finally:
            restored.db.close()

    def test_large_range_is_one_interval(self):
        self.accept(1,1,30_000_000)
        self.assertEqual(self.tracker.db.execute('SELECT count(*) FROM coverage').fetchone()[0],1)

    def test_two_connections_preserve_bits(self):
        other=Tracker(self.root/'tracker.sqlite')
        try:
            self.accept(1)
            other.accept('job',[('address',2,'a',10,20)],{})
            self.assertEqual(self.tracker.status('address',12,{1:'a',2:'a'}),3)
        finally:
            other.db.close()

    def test_artifacts_empty_and_tampered(self):
        root=self.root/'artifacts';root.mkdir()
        seal(root,{'kind':'events'},{'10':'0xabc'})
        self.assertEqual(verify(root)['files'],[])
        (root/'extra.parquet').write_bytes(b'bad')
        with self.assertRaises(ValueError):verify(root)


if __name__=='__main__':unittest.main()
