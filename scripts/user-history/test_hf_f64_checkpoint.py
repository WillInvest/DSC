import os,tempfile,unittest
from pathlib import Path
import numpy as np
from hf_replay_checkpoint import restore,state_of
from shared.artifacts import digest

class CheckpointBoundary(unittest.TestCase):
    def test_checksum_failure_precedes_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'checkpoint.gz';p.write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError,'hash differs'):restore(None,None,None,25200000,25200001,p,'wrong')

    @unittest.skipUnless(os.environ.get('DSC_HF_REAL_INPUTS')=='1','Requires the verified frozen history image')
    def test_real_prefix_and_checkpoint_tail_are_identical(self):
        from native_batch import BatchEvaluator
        from hf_batch_f64 import FloatBatch
        from hf_f64_contract import CONTRACT
        import materialize_accounts as m
        root=Path('/home/dsc-data/runs/aave-user-history-v3/26407881');db=m.readonly(root/'accounting.sqlite');plan=m.load(root/'plan.json')
        cp=m.load('/var/lib/dsc-aave-hf-f64/20261116/checkpoints/before-25199900.json.receipt.json');block=25200000
        full=BatchEvaluator(root,db,plan,block,block+1);seeded=restore(root,db,plan,block,block+1,cp['path'],cp['sha256'])
        try:
            self.assertEqual(digest(state_of(full)),digest(state_of(seeded)));full.advance(block);seeded.advance(block)
            model=FloatBatch(m.owner_universe(root,26407882),plan['jobs']);a=model.evaluate(full,dust_cutoff=CONTRACT['dust_debt_base_exclusive']);b=model.evaluate(seeded,dust_cutoff=CONTRACT['dust_debt_base_exclusive'])
            np.testing.assert_array_equal(a['hf'].view(np.uint64),b['hf'].view(np.uint64));np.testing.assert_array_equal(a['debt'],b['debt'])
        finally:full.close();seeded.close();db.close()
if __name__=='__main__':unittest.main()
