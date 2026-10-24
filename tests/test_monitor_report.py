import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from monitor.report import contract_report, read_source, safe_error
from orchestration.tracker import Tracker


def target():
    return {'address':'addr','protocol':'uniswap-v2','name':'Test','creation_block':10,'start':10,'stop':20,
            'segments':[{'start':10,'stop':20,'configs':{'1':'e','2':'s','4':'r'}}]}


class MonitorReportTests(unittest.TestCase):
    def test_all_empty_but_checked_is_complete(self):
        c=contract_report(target(),{('addr',1,'e'):[(10,20)],('addr',2,'s'):[(10,20)],('addr',4,'r'):[(10,20)]})
        self.assertEqual((c['percent'],c['contiguous']), (100,19))

    def test_intersection_and_contiguous_gap(self):
        c=contract_report(target(),{('addr',1,'e'):[(10,20)],('addr',2,'s'):[(10,12),(14,20)],('addr',4,'r'):[(10,20)]})
        self.assertEqual(c['complete'],8)
        self.assertEqual(c['contiguous'],11)
        self.assertEqual(c['intervals'],[[10,12,7],[12,14,5],[14,20,7]])

    def test_old_configuration_does_not_count(self):
        c=contract_report(target(),{('addr',1,'old'):[(1,100)]})
        self.assertEqual(c['datasets']['1'],0)

    def test_abi_boundary_uses_each_configuration(self):
        t=target();t['segments']=[dict(t['segments'][0],stop=15),{'start':15,'stop':20,'configs':{'1':'new','2':'s','4':'r'}}]
        c=contract_report(t,{('addr',1,'e'):[(10,20)]})
        self.assertEqual(c['datasets']['1'],5)

    def test_overlap_never_exceeds_denominator(self):
        c=contract_report(target(),{('addr',1,'e'):[(0,100),(10,15)]})
        self.assertEqual(c['datasets']['1'],10)

    def test_export_does_not_mutate_database(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'tracker.sqlite';t=Tracker(p)
            manifest={'run_key':'base','start':10,'stop':20,'targets':[target()]}
            with t.db:t.db.execute('INSERT INTO monitor_runs(id,manifest) VALUES(?,?)',('r',json.dumps(manifest)))
            t.db.close();before=hashlib.sha256(p.read_bytes()).hexdigest()
            result=read_source({'db':str(p),'id':'test','label':'Test'})
            self.assertEqual(result['runs'][0]['percent'],0)
            self.assertEqual(hashlib.sha256(p.read_bytes()).hexdigest(),before)

    def test_missing_database_is_not_started(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'absent.sqlite'
            self.assertEqual(read_source({'db':str(p),'id':'p','label':'P'})['state'],'not_started')
            self.assertFalse(p.exists())

    def test_legacy_progress_is_unavailable(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'old.sqlite';sqlite3.connect(p).close()
            self.assertEqual(read_source({'db':str(p),'id':'p','label':'P'})['state'],'legacy')

    def test_error_redaction(self):
        self.assertNotIn('secret',safe_error('failed: password=secret /private/file'))


if __name__=='__main__':unittest.main()
