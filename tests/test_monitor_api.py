from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path
import time
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
try:
    from fastapi.testclient import TestClient
    from monitor import app as module
except ImportError:
    module=None


@unittest.skipIf(module is None,'Install monitor requirements and httpx for API tests')
class MonitorApiTests(unittest.TestCase):
    def setUp(self):
        self.run={'id':'p:r','contracts':[{'address':'addr','creation_block':10,'start':10,'stop':12,'intervals':[[10,11,7],[11,12,3]]}], 'jobs':[], 'job_counts':{}}
        module.snapshot={'sources':[{'runs':[self.run],'heartbeat':None}], 'registry':[]}
        module.last_success=time.time();module.collection_error=False
        self.client=TestClient(module.app)

    def test_lanes_running_sort_and_missing_peer(self):
        stamp=datetime.now(timezone.utc).isoformat()
        source=module.snapshot['sources'][0]
        source['lanes']={'events-state':{'heartbeat':{'state':'running','timestamp':stamp},'progress':{'run_id':'r','start':10,'stop':12}},
                         'reverted':{'heartbeat':None,'progress':None}}
        self.run['local_id']='r'
        base=self.run['contracts'][0]
        self.run['contracts']=[dict(base,address='z',updated='2026-10-24 10:00:00'),
            dict(base,address='b',updated='2026-10-24 10:00:00'),
            dict(base,address='a',updated='2026-10-23 10:00:00',active_jobs=[{'kind':'state','lane':'events-state','start':10,'stop':12}])]
        result=self.client.get('/api/overview').json()['sources'][0]
        self.assertEqual(result['runner'],'Needs attention')
        self.assertEqual([c['address'] for c in result['runs'][0]['contracts']],['a','b','z'])
        self.assertTrue(result['runs'][0]['contracts'][0]['running'])
        source['lanes']['events-state']['heartbeat']['timestamp']=(datetime.now(timezone.utc)-timedelta(seconds=61)).isoformat()
        cards=self.client.get('/api/contracts',params={'run':'p:r'}).json()
        self.assertEqual([c['address'] for c in cards],['b','z','a'])
        self.assertFalse(any(c['running'] for c in cards))

    def test_other_run_or_batch_does_not_mark_old_active_jobs_running(self):
        source=module.snapshot['sources'][0]
        source['lanes']={'events-state':{'heartbeat':{'state':'running','timestamp':datetime.now(timezone.utc).isoformat()},'progress':{'run_id':'another','start':10,'stop':12}},
                         'reverted':{'heartbeat':{'state':'complete'},'progress':None}}
        self.run['local_id']='r'
        self.run['contracts'][0]['active_jobs']=[{'kind':'events','lane':'events-state','start':10,'stop':12}]
        self.assertFalse(self.client.get('/api/contracts',params={'run':'p:r'}).json()[0]['running'])
        source['lanes']['events-state']['progress']={'run_id':'r','start':12,'stop':15}
        self.assertFalse(self.client.get('/api/contracts',params={'run':'p:r'}).json()[0]['running'])

    def test_bounded_grid_and_not_applicable(self):
        r=self.client.get('/api/coverage',params={'run':'p:r','addresses':'addr','start':9,'count':4})
        self.assertEqual([x['values'][0] for x in r.json()['rows']],['N/A',7,3,'Outside run'])

    def test_oversized_request_rejected(self):
        r=self.client.get('/api/coverage',params={'run':'p:r','addresses':'addr','start':0,'count':1001})
        self.assertEqual(r.status_code,422)

    def test_post_cannot_control_extraction(self):
        self.assertEqual(self.client.post('/api/jobs',json={'action':'run'}).status_code,405)

    def test_stale_snapshot_retained_on_failure(self):
        old=module.snapshot;module.last_success=time.time()-100
        with patch.dict('os.environ',{'MONITOR_SSH_KEY':'test'}),patch('monitor.app.subprocess.run',side_effect=OSError('down')):
            module.collect()
        self.assertIs(module.snapshot,old)
        self.assertTrue(self.client.get('/api/overview').json()['stale'])
        self.assertTrue(module.collection_error)


if __name__=='__main__':unittest.main()
