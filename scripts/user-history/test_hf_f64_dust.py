import unittest
from unittest.mock import patch
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from test_hf_f64 import fixture
from hf_f64_vectors import encode_aggregates,ORIGIN

class DebtExclusionPolicy(unittest.TestCase):
    def test_below_cent_excluded_without_exact_recomputation(self):
        model,ev=fixture();ev.positions['alice']['vusdc']='9999'
        with patch('hf_batch_f64.exact_debts',side_effect=AssertionError('Dust need not be recomputed')):
            r=model.evaluate(ev,dust_cutoff=1_000_000)
        self.assertTrue(np.isposinf(r['hf'][0]));self.assertEqual(r['debt'][0],0);self.assertEqual(r['excluded_users'],1)
        self.assertLessEqual(abs(r['excluded_debt_base']-999900),1)  # One base unit is 1e-8 USD; this is diagnostic float64 evidence.
        self.assertNotIn(0,r['debt_user_ids'])
    def test_exactly_cent_is_included(self):
        model,ev=fixture();ev.positions['alice']['vusdc']='10000';ev.account=lambda owner:{'account':['200000000000','1000000','0','8000','7500',str(160000*10**18)]}
        with patch('hf_batch_f64.exact_debts',return_value=[(1,1_000_000.)]):r=model.evaluate(ev,dust_cutoff=1_000_000)
        self.assertEqual(r['debt'][0],1_000_000);self.assertEqual(r['hf'][0],160000);self.assertEqual(r['excluded_users'],0)
    def test_retained_hf_applies_same_cutoff(self):
        model,ev=fixture();ev.positions['alice']['vusdc']='9999';r=model.evaluate(ev,np.array([5.,0.,np.inf]),dust_cutoff=1_000_000)
        self.assertTrue(np.isposinf(r['hf'][0]));self.assertEqual(r['excluded_users'],1)
    def test_per_block_exclusion_evidence_is_protocol_scoped(self):
        raw=encode_aggregates([ORIGIN],[[0,0]],[[0,0]],[[0,0]],[0,1],[3],[123.])
        t=pq.read_table(pa.BufferReader(raw));self.assertEqual(t['excluded_users_vector'].to_pylist(),[None,[3]]);self.assertEqual(t['excluded_debt_base_vector'].to_pylist(),[None,[123.]])
if __name__=='__main__':unittest.main()
