"""Behavioral checks for original-version Aave account kernels.

Set AAVE_NODE_MODULE_ROOT to the directory containing the pinned package.json /
node_modules used by this deployment. Stored node fixtures are comparison inputs,
not an event-reconstruction acceptance claim.
"""
import json,os,subprocess,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
ARTIFACTS=ROOT/'artifacts/aave-account-kernels-20261115'
PILOT=ROOT/'artifacts/aave-user-hf-svb-2026-11-13'
MAX=str(2**256-1)
ZERO='0x0000000000000000000000000000000000000000'
ASSET='0x0000000000000000000000000000000000000001'

class AccountKernelTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  modules=os.environ.get('AAVE_NODE_MODULE_ROOT')
  if not modules:raise unittest.SkipTest('AAVE_NODE_MODULE_ROOT must point to pinned installed dependencies')
  cls.worker=subprocess.Popen(['node',str(ROOT/'scripts/user-history/account_worker.mjs'),str(ARTIFACTS),modules],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 @classmethod
 def tearDownClass(cls):
  cls.worker.stdin.close();cls.worker.wait(timeout=20);cls.worker.stdout.close();cls.worker.stderr.close()
 def call(self,p):
  self.worker.stdin.write(json.dumps(p)+'\n');self.worker.stdin.flush();line=self.worker.stdout.readline();self.assertTrue(line,'Worker exited without response');return json.loads(line)
 def simple(self,profile,emode=0,config=3):
  cfg=8000+(8500<<16)+(10500<<32)+(18<<48)+(1<<168)
  return dict(operation='evaluate',profile=profile['profile'],block=profile['from_block'],timestamp=1700000000,accounts=[dict(id='simple',rows=[dict(asset=ASSET,id=0,configuration=str(cfg),liquidityIndex=str(10**27),liquidityRate='0',variableBorrowIndex=str(10**27),variableBorrowRate='0',updated=1700000000,scaledSupply=str(100*10**18),scaledDebt=str(50*10**18),stablePrincipal='0',stableRate='0',stableUpdated=0)],userConfiguration=str(config),emode=emode,modes=[dict(id=1,ltv='9000',threshold='9500',bonus='10100',priceSource=ZERO,collateralBitmap='1',borrowableBitmap='1',ltvzeroBitmap='0',isolated=False)],priceAssets=[ASSET],assetPrices=['100000000'])])
 def test_all_original_profiles_default_and_emode_and_no_state_leak(self):
  manifest=json.loads((ARTIFACTS/'manifest.json').read_text())
  self.assertEqual(len(manifest['profiles']),12)
  for p in manifest['profiles']:
   with self.subTest(profile=p['profile']):
    normal=self.call(self.simple(p));self.assertNotIn('error',normal);self.assertEqual(normal['results'][0]['account'],['10000000000','5000000000','3000000000','8500','8000','1700000000000000000'])
    mode=self.call(self.simple(p,emode=1));self.assertNotIn('error',mode);self.assertEqual(mode['results'][0]['account'],['10000000000','5000000000','4000000000','9500','9000','1900000000000000000'])
    empty=self.call(self.simple(p,config=0));self.assertEqual(empty['results'][0]['account'],['0','0','0','0','0',MAX])
 def test_current_isolated_emode_changes_ltv_not_other_collateral_threshold(self):
  p=json.loads((ARTIFACTS/'manifest.json').read_text())['profiles'][-1]
  req=self.simple(p,emode=1,config=11);account=req['accounts'][0]
  other=dict(account['rows'][0],asset='0x0000000000000000000000000000000000000002',id=1,scaledDebt='0')
  account['rows'].append(other);account['priceAssets'].append(other['asset']);account['assetPrices'].append('100000000')
  ordinary=self.call(req);self.assertEqual(ordinary['results'][0]['account'],['20000000000','5000000000','12000000000','9000','8500','3600000000000000000'])
  account['modes'][0]['isolated']=True
  isolated=self.call(req);self.assertEqual(isolated['results'][0]['account'],['20000000000','5000000000','4000000000','9000','4500','3600000000000000000'])
 def test_nominal_debt_is_distinct_from_scaled_debt_in_original_custom_branch(self):
  p=next(x for x in json.loads((ARTIFACTS/'manifest.json').read_text())['profiles'] if x['profile']=='21917056')
  req=self.simple(p);row=req['accounts'][0]['rows'][0];row['variableBorrowIndex']=str(11*10**26)
  # Non-virtual reserve: original GenericLogic calls balanceOf, not scaledBalanceOf.
  default=self.call(req);self.assertEqual(default['results'][0]['account'][1],'5500000000')
  row.update(hasDebtBalanceOverride=True,debtBalanceOverride=str(45*10**18))
  discounted=self.call(req);self.assertEqual(discounted['results'][0]['account'][1],'4500000000')
  # Virtual reserve still follows the original scaled/index branch.
  row['configuration']=str(int(row['configuration'])|(1<<252))
  virtual=self.call(req);self.assertEqual(virtual['results'][0]['account'][1],'5500000000')
 def test_reviewed_bounds_and_missing_inputs_fail_closed(self):
  p=json.loads((ARTIFACTS/'manifest.json').read_text())['profiles'][0]
  r=self.simple(p);r['block']=p['to_block_exclusive'];self.assertIn('outside reviewed',self.call(r)['error'])
  r=self.simple(p);r['accounts'][0]['userConfiguration']='12';self.assertIn('missing reserve id',self.call(r)['error'])
  r=self.simple(p);r['accounts'][0]['emode']=2;self.assertIn('Missing selected eMode',self.call(r)['error'])
  r=self.simple(p);r['accounts'][0]['assetPrices']=['0'];self.assertIn('Nonpositive',self.call(r)['error'])
  r=self.simple(p);r['timestamp']-=1;self.assertIn('Future',self.call(r)['error'])
  modern=json.loads((ARTIFACTS/'manifest.json').read_text())['profiles'][-1];r=self.simple(modern,emode=1);del r['accounts'][0]['modes'][0]['collateralBitmap'];self.assertIn('Missing historical eMode field',self.call(r)['error'])
 def test_saved_svb_final_node_accounts_all_six_fields(self):
  x=json.loads((PILOT/'inputs.json').read_text());b=x['stop'];timestamp=next(h['timestamp'] for h in x['headers'] if h['block']==b)
  states={a:dict(asset=a,id=r['id'],configuration=r['seed']['configuration'][0],liquidityIndex=r['seed']['liquidityIndex'],liquidityRate=r['seed']['currentLiquidityRate'],variableBorrowIndex=r['seed']['variableBorrowIndex'],variableBorrowRate=r['seed']['currentVariableBorrowRate'],updated=int(r['seed']['lastUpdateTimestamp'])) for a,r in x['reserves'].items()}
  headers={h['block']:h['timestamp'] for h in x['headers']}
  self.assertEqual(x['configurator_events'],[],'Fixture adapter intentionally needs no risk config replay in this window')
  for e in sorted(x['pool_events'],key=lambda e:(e['block'],e['transaction_index'],e['log_index'])):
   if e['name']=='ReserveDataUpdated':
    a=e['args'];states[a['reserve']].update(liquidityIndex=a['liquidityIndex'],liquidityRate=a['liquidityRate'],variableBorrowIndex=a['variableBorrowIndex'],variableBorrowRate=a['variableBorrowRate'],updated=headers[e['block']])
  modes=[dict(id=int(i),ltv=c['ltv'],threshold=c['liquidationThreshold'],bonus=c['liquidationBonus'],priceSource=c['priceSource']) for i,c in x['categories'].items()]
  prices=next(v['prices'] for v in x['prices'] if v['block']==b);accounts=[]
  for u in x['expected_end']:
   rows=[]
   for a in x['reserve_list']:
    p=u['positions'][a];rows.append(dict(states[a],scaledSupply=p['scaled_supply'],scaledDebt=p['scaledVariableDebt'],stablePrincipal=p['principalStableDebt'],stableRate=p['stableBorrowRate'],stableUpdated=int(p['stableRateLastUpdated'])))
   accounts.append(dict(id=u['user'],rows=rows,userConfiguration=u['configuration'],emode=u['emode'],modes=modes,priceAssets=x['price_assets'],assetPrices=[prices[a] for a in x['price_assets']]))
  output=self.call(dict(operation='evaluate',profile='16291127',block=b,timestamp=timestamp,accounts=accounts));self.assertNotIn('error',output)
  expected={v['user']:v['account'] for v in x['expected_accounts'] if v['block']==b};self.assertEqual(len(output['results']),57)
  for actual in output['results']:self.assertEqual(actual['account'],expected[actual['id']],actual['id'])

if __name__=='__main__':unittest.main()
