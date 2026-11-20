import unittest
from types import SimpleNamespace
import numpy as np
from hf_batch_f64 import FloatBatch
from hf_aggregates import aggregate

def fixture(profile='16800000'):
    def cfg(dec,threshold,category=0):return str((dec<<48)|(threshold<<16)|(category<<168)|7500)
    jobs=[dict(address='aeth',asset='eth',kind='aToken'),dict(address='veth',asset='eth',kind='variableDebtToken'),dict(address='ausdc',asset='usdc',kind='aToken'),dict(address='vusdc',asset='usdc',kind='variableDebtToken')]
    ev=SimpleNamespace(block=16800000,profile=profile,positions={'alice':{'aeth':str(10**18),'vusdc':str(1000*10**6)},'bob':{'veth':str(10**17)}},active={'eth','usdc'},assets=['eth','usdc'],asset_prices=[str(2000*10**8),str(10**8)],collateral={'alice':{'eth'}},emodes={},ghoborrow={},modes={},reserves={'eth':{'id':0,'configuration':cfg(18,8000)},'usdc':{'id':1,'configuration':cfg(6,8500)}},normalized={x:{'income':str(10**27),'debt':str(10**27)} for x in ['eth','usdc']})
    return FloatBatch([('alice',0),('bob',0),('charlie',0)],jobs),ev

class FloatHealthFactor(unittest.TestCase):
    def test_sparse_borrowers_and_no_debt(self):
        model,ev=fixture();result=model.evaluate(ev)
        np.testing.assert_allclose(result['hf'][:2],[1.6,0.0],rtol=1e-12);self.assertTrue(np.isposinf(result['hf'][2]));self.assertEqual(result['exact_fallback_users'],0)
    def test_protocol_zero_address_is_not_a_research_wallet(self):
        from native_hf import ZERO
        model,ev=fixture();ev.positions[ZERO]={'aeth':str(10**18)}
        self.assertEqual(len(model.evaluate(ev)['hf']),3)
    def test_reused_hf_rebuilds_only_debt_weights(self):
        model,ev=fixture();x=model.evaluate(ev,np.array([3.,0.,np.inf]))
        self.assertEqual(x['hf'][0],3);self.assertEqual(x['collateral'][0],0);self.assertEqual(x['debt'][0],1000*10**8)
    def test_debt_attribution_and_protocol_identity(self):
        model,ev=fixture();x=model.evaluate(ev);a=aggregate(x['hf'],x['debt_user_ids'],x['debt_reserve_ids'],x['debt_values'],2)
        np.testing.assert_allclose(a['hf'],[0,1.6]);self.assertAlmostEqual(a['protocol_hf'],4/3)
    def test_old_emode_uses_user_specific_price(self):
        model,ev=fixture();ev.emodes={'alice':1};ev.reserves['eth']['configuration']=str(int(ev.reserves['eth']['configuration'])|(1<<168));ev.modes={1:{'id':1,'threshold':9000,'priceSource':'usdc'}}
        self.assertAlmostEqual(model.evaluate(ev)['hf'][0],.0009)
    def test_new_emode_bitmap_retains_oracle_price(self):
        model,ev=fixture('20920979');ev.emodes={'alice':1};ev.modes={1:{'id':1,'threshold':9000,'collateralBitmap':1}}
        self.assertAlmostEqual(model.evaluate(ev)['hf'][0],1.8)
    def test_zero_weight_infinity_not_multiplied(self):
        a=aggregate(np.array([np.inf,2]),np.array([0,1]),np.array([0,1]),np.array([0,3.]),3)
        self.assertFalse(a['valid'][0]);self.assertEqual(a['protocol_hf'],2);self.assertTrue(np.all(np.isfinite(a['numerator'])))
    def test_empty_market_null(self):
        a=aggregate([],[],[],[],3);self.assertIsNone(a['protocol_hf']);self.assertFalse(np.any(a['valid']))
    def test_positive_debt_infinity_rejected(self):
        with self.assertRaises(ValueError):aggregate([np.inf],[0],[0],[1],1)
    def test_negative_debt_rejected(self):
        with self.assertRaises(ValueError):aggregate([1.],[0],[0],[-1.],1)
if __name__=='__main__':unittest.main()
