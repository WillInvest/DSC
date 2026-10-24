from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from orchestration.coordinator import Coordinator
from orchestration.tracker import Tracker
from orchestration.artifacts import seal, verify
from orchestration.worker import archive_store
from reverted.extract_reverted import extract_block
from test_reverted import Node


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.c = Coordinator.__new__(Coordinator)
        self.c.args = SimpleNamespace(cleanup=False)
        self.c.tracker = Tracker(self.root/'tracker.sqlite')
        self.addCleanup(self.c.tracker.db.close)
        self.c.failed = False
        self.c.profiles = []

    def test_accepted_empty_dataset_skips_on_restart(self):
        row={'protocol':'uniswap-v2','address':'0x'+'12'*20,'creation_block':1}
        self.c.extraction=Mock(return_value={'verified_empty':True,'headers':{'10':'hash'}})
        self.c.dataset(row,'events',1,10,11,{'10':'hash'})
        self.c.dataset(row,'events',1,10,11,{'10':'hash'})
        self.assertEqual(self.c.extraction.call_count,1)

    def test_restart_after_transfer_reuses_local_receipt(self):
        self.c.root = self.root
        self.c.servers = {'node': {'root':'/unused'}}
        spec={'kind':'events','protocol':'uniswap-v2','address':'0x'+'12'*20,'start':10,'stop':11}
        self.c.tracker.job('batch',spec)
        seal(self.root/'incoming'/'batch',spec,{'10':'hash'})
        with patch('orchestration.coordinator.remote') as remote:
            receipt=self.c.extraction(spec,{'10':'hash'},'batch')
            remote.assert_not_called()
        self.assertTrue(Path(receipt['decoded']['path']).exists())

    def test_acceptance_failure_does_not_set_flag(self):
        self.c.job({'kind':'events','start':10,'stop':11}, Mock(side_effect=ValueError('checksum mismatch')), [('addr',1,'cfg',10,11)])
        self.assertTrue(self.c.failed)
        self.assertEqual(self.c.tracker.status('addr',10,{1:'cfg'}),0)

    def test_missing_aave_profile_never_dispatches(self):
        self.c.extraction=Mock()
        row={'protocol':'aave-v2','address':'0x'+'12'*20}
        self.c.dataset(row,'state',2,10,11,{'10':'hash'})
        self.assertTrue(self.c.failed)
        self.c.extraction.assert_not_called()

    def test_archive_reuse_across_different_bounds(self):
        source=self.root/'batch';store=self.root/'retained'
        node=Node()
        extract_block(10,source,1,node,1,'test')
        spec={'kind':'archive','start':10,'stop':11,'config':'test'}
        seal(source,spec,{'10':'0xabc'})
        archive_store({'action':'archive_index','output':str(source),'store':str(store),'spec':spec,'headers':{'10':'0xabc'}})
        result=archive_store({'action':'archive_reuse','output':str(self.root/'partial'),'store':str(store),'spec':dict(spec,start=9),'headers':{'9':'other','10':'0xabc'}})
        self.assertEqual(result,{'available':1,'complete':False})
        result=archive_store({'action':'archive_reuse','output':str(self.root/'complete'),'store':str(store),'spec':spec,'headers':{'10':'0xabc'}})
        self.assertTrue(result['complete'])
        verify(self.root/'complete',spec,{'10':'0xabc'})


if __name__=='__main__':unittest.main()
