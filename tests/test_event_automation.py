import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from automation.common import select, paths


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.plan={'jobs':[{'address':'a'},{'address':'b'}]}
        self.received={'a':{'state':'complete'},'b':{'state':'complete'}}

    def test_new_encoded_data_precedes_reconstruction(self):
        phase,job=select(self.plan,self.received,{'a':{'decoded':True}},100)
        self.assertEqual((phase,job['address']),('decode','b'))

    def test_incomplete_transfer_is_not_read(self):
        phase,job=select(self.plan,{'a':{'state':'running'}},{},100)
        self.assertEqual((phase,job),('waiting',None))

    def test_failed_decode_does_not_allow_reconstruction(self):
        states={'a':{'decoded':True},'b':{'retry_after':200}}
        self.assertEqual(select(self.plan,self.received,states,100),('retry_wait',None))
        self.assertEqual(select(self.plan,self.received,states,201)[0],'decode')

    def test_resume_reconstruction_after_decoding(self):
        states={'a':{'decoded':True,'reconstructed':True},'b':{'decoded':True}}
        phase,job=select(self.plan,self.received,states,100)
        self.assertEqual((phase,job['address']),('reconstruct','b'))

    def test_paths_preserve_category_and_pool(self):
        raw,decoded,state=paths({'destination':'/data/dex/uniswap/v2/encoded/ABC-DEF-3000'})
        self.assertEqual(str(state),'/data/dex/uniswap/v2/reconstructed/ABC-DEF-3000')
        self.assertEqual(str(decoded),'/data/dex/uniswap/v2/decoded/ABC-DEF-3000')

if __name__=='__main__': unittest.main()
