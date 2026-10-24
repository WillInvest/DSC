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

    def test_activity_uses_all_jobs_and_exact_contract_applicability(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'tracker.sqlite'; tracker=Tracker(p); tracker.run_id='r'
            targets=[target(), dict(target(),address='other'), dict(target(),address='future',creation_block=30,start=30,stop=40), dict(target(),address='older',creation_block=50,start=50,stop=60)]
            manifest={'start':10,'stop':40,'targets':targets}
            with tracker.db: tracker.db.execute('INSERT INTO monitor_runs(id,manifest) VALUES(?,?)',('r',json.dumps(manifest)))
            for i in range(105):
                tracker.job(str(i),{'kind':'events','address':'other','start':10,'stop':20})
                with tracker.db: tracker.db.execute("UPDATE jobs SET stage='accepted',updated='2026-10-24 12:00:00' WHERE id=?",(str(i),))
            tracker.job('old',{'kind':'state','address':'older','start':50,'stop':60})
            with tracker.db: tracker.db.execute("UPDATE jobs SET stage='accepted',updated='2026-10-23 12:00:00' WHERE id='old'")
            tracker.job('archive',{'kind':'archive','start':10,'stop':20});tracker.stage('archive','extracting')
            tracker.job('selection',{'kind':'reverted','contracts':['addr'],'start':10,'stop':20});tracker.stage('selection','validating')
            tracker.db.close()
            result=read_source({'db':str(p),'id':'test','label':'Test'})['runs'][0]
            contracts={c['address']:c for c in result['contracts']}
            self.assertEqual(len(result['jobs']),100)
            self.assertEqual(contracts['older']['updated'],'2026-10-23 12:00:00')
            self.assertEqual({j['kind'] for j in contracts['addr']['active_jobs']},{'archive','reverted'})
            self.assertEqual({j['kind'] for j in contracts['other']['active_jobs']},{'archive'})
            self.assertEqual(contracts['future']['active_jobs'],[])
            self.assertIsNone(contracts['future']['updated'])

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
