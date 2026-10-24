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
