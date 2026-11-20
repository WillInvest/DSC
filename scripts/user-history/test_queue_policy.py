"""Queue behavior checks: focus order, deferral, and identity preservation."""
import unittest
from run import selected_jobs

class QueuePolicyTest(unittest.TestCase):
    def setUp(self):
        self.plan={'end_block':99,'end_hash':'pinned','jobs':[
            {'address':'lido','asset':'wstETH'},
            {'address':'usdt-debt','asset':'USDT'},
            {'address':'weth','asset':'WETH'},
            {'address':'usdc','asset':'USDC'},
            {'address':'usdt-supply','asset':'USDT'},
            {'address':'btc','asset':'WBTC'},
        ]}
    def test_four_asset_focus_preserves_all_jobs_and_identity(self):
        original=repr(self.plan)
        result=selected_jobs(self.plan,{'priority_assets':['WETH','USDC','USDT','WBTC'],'defer_other_assets':True})
        self.assertEqual([j['address'] for j in result],['weth','usdc','usdt-debt','usdt-supply','btc'])
        self.assertEqual(repr(self.plan),original)
    def test_default_and_resumption_keep_nonpriority_jobs(self):
        self.assertEqual(selected_jobs(self.plan,{}),self.plan['jobs'])
        focused=selected_jobs(self.plan,{'priority_assets':['USDT'],'defer_other_assets':False})
        self.assertEqual(len(focused),6)
        self.assertEqual([j['address'] for j in focused[:2]],['usdt-debt','usdt-supply'])
    def test_invalid_focus_fails_instead_of_silently_skipping(self):
        for p in [{'priority_assets':[],'defer_other_assets':True},{'priority_assets':['unknown']},{'priority_assets':['USDC','USDC']}]:
            with self.assertRaises(ValueError):selected_jobs(self.plan,p)

if __name__=='__main__':unittest.main()
