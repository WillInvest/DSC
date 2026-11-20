"""Persisted history boundaries and resume correctness, independent of math formulas."""
import json,sys,tempfile,unittest
from unittest.mock import patch
from pathlib import Path
sys.path[:0]=[str(Path(__file__).resolve().parent),str(Path(__file__).resolve().parents[1])]
from accounting_inputs import connect,import_file,indexed,gho_borrowing_flags,validate_pool_upgrades,START
from run_accounting import coverage,verify_accounts,candidate,status_inputs
class AccountingInputsTest(unittest.TestCase):
 def test_progress_counts_only_current_generation_receipts_and_reports(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);db=connect(root)
   for gen in ['old','new']:
    p=root/(gen+'.json');p.write_text(json.dumps(dict(generation=gen)))
    db.execute('insert into nominal_tokens values(?,?)',(gen,str(p)))
    db.execute('insert into validations values(?,?)',(gen,json.dumps(dict(generation=gen,kind='full_account_HF',exact=gen=='old'))))
   db.commit();reports,count=status_inputs(db,'new');self.assertEqual(count,1);self.assertEqual(len(reports),1);self.assertFalse(reports[0]['exact']);self.assertEqual(status_inputs(db,None),([],0));db.close()
 def test_initial_implementation_is_a_seed_but_subsequent_upgrades_are_required(self):
  profs=[dict(from_block=START,implementation='0xAa'),dict(from_block=START+10,implementation='0xBb')]
  validate_pool_upgrades(profs,{START+10:'0xbb'})
  validate_pool_upgrades(profs,{START:'0xaa',START+10:'0xbb'})
  for actual in [{},{START+10:'0xcc'},{START:'0xcc',START+10:'0xbb'},{START+10:'0xbb',START+20:'0xcc'}]:
   with self.assertRaises(ValueError):validate_pool_upgrades(profs,actual)
 def test_gho_flag_uses_protocol_configuration_not_positive_dust_shares(self):
  with tempfile.TemporaryDirectory() as d:
   db=connect(Path(d));b=START+1;token='0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b';asset='0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f'
   db.execute('insert into snapshots values(?,?,?,?)',('reserve',asset,START,json.dumps({'reserve':{'id':'33'}})))
   db.executemany('insert into gho_states values(?,?,?,?,?,?,?)',[(token,u,b,0,i,'1000000000000000000000000000','0') for i,u in enumerate(['u','v'])]);db.execute('insert into changes values(?,?,?,?,?,?)',(token,'u',b,0,0,'1'));db.commit()
   with patch('accounting_inputs.node',return_value=[{'result':[0],'block_hash':'0xa'},{'result':[(1<<255)|(1<<66)],'block_hash':'0xa'}]):
    gho_borrowing_flags(db,{},[{'key':str(START),'from_block':START,'abi':[]}],lambda *a,**k:None)
   states={u:json.loads(v) for u,v in db.execute("select key,data from snapshots where kind='gho_borrowing'")}
   self.assertFalse(states['u']['borrowing']);self.assertTrue(states['v']['borrowing']);self.assertEqual(states['v']['configuration'],str((1<<255)|(1<<66)));db.close()
 def test_failed_hf_comparison_is_retried_and_never_reused_as_pass(self):
  with tempfile.TemporaryDirectory() as d:
   r=Path(d);(r/'accounting-validations').mkdir();db=connect(r);block=16291128;job=dict(address='t',kind='variableDebtToken',start_block=16291127)
   db.execute('insert into changes values(?,?,?,?,?,?)',('t','u',block,0,0,'1'));db.commit()
   class Ev:
    generation='test-generation'
    value='1'
    def account(self,user,b):return dict(user=user,account=[self.value]*6)
   ev=Ev();profs=[dict(key='16291127',from_block=16291127,abi=[])]
   with patch('run_accounting.node',return_value=[dict(result=['0']*6)]) as ref:
    with self.assertRaisesRegex(ValueError,'HF mismatch'):verify_accounts(db,r,dict(end_block=block),profs,ev,block,[job])
    self.assertFalse(json.loads(db.execute('select data from validations').fetchone()[0])['exact'])
    ev.value='0';verify_accounts(db,r,dict(end_block=block),profs,ev,block,[job]);self.assertEqual(ref.call_count,2)
    self.assertTrue(json.loads(db.execute('select data from validations').fetchone()[0])['exact']);self.assertEqual(len(list((r/'accounting-validations').glob('*.json'))),2)
   db.close()
 def test_timestamp_gap_does_not_hide_conflicting_block(self):
  with tempfile.TemporaryDirectory() as d:
   r=Path(d);db=connect(r);p=r/'headers';p.write_text('first');import_file(db,p,'headers',[dict(block_number=10,timestamp=100,block_hash='0xa')]);p.write_text('changed')
   with self.assertRaisesRegex(ValueError,'identity changed'):indexed(db,p)
   with self.assertRaisesRegex(ValueError,'source changed'):import_file(db,p,'headers',[dict(block_number=10,timestamp=101,block_hash='0xb')])
   self.assertEqual(db.execute('select timestamp from headers where block=10').fetchone()[0],100);db.close()
 def test_complete_account_prefix_stops_before_missing_asset(self):
  plan={'end_block':100,'jobs':[{'address':'a','start_block':10},{'address':'b','start_block':50}]}
  through,missing=coverage(plan,[plan['jobs'][0]]);self.assertEqual(through,49);self.assertEqual(missing,[plan['jobs'][1]])
  self.assertEqual(coverage(plan,plan['jobs']),(100,[]))
 def test_same_block_multiple_transfers_keep_end_of_block_order(self):
  with tempfile.TemporaryDirectory() as d:
   r=Path(d);db=connect(r);p=r/'shares';p.write_text('fixture');import_file(db,p,'changes',[('t','u',10,1,3,'5'),('t','u',10,1,4,'7'),('t','u',11,0,1,'9')]);value=db.execute('select scaled from changes where owner=? and token=? and block<=? order by block desc,tx desc,log desc limit 1',('u','t',10)).fetchone()[0];self.assertEqual(value,'7');db.close()
if __name__=='__main__':unittest.main()
