"""Coverage/user-universe/disk semantics of the numerical materializer."""
import importlib.util,sqlite3,tempfile,unittest,json,hashlib,sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
spec=importlib.util.spec_from_file_location('numeric',Path(__file__).with_name('materialize_accounts.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class MaterializationTests(unittest.TestCase):
 def test_abrupt_worker_exit_reports_failure_instead_of_waiting_forever(self):
  from concurrent.futures.process import BrokenProcessPool
  with m.ProcessPoolExecutor(max_workers=1,mp_context=m.multiprocessing.get_context('spawn')) as pool:
   future=pool.submit(m.os._exit,7)
   with self.assertRaises(BrokenProcessPool):future.result(timeout=15)
 def test_unaccepted_orphan_is_preserved_and_can_be_recomputed(self):
  with tempfile.TemporaryDirectory() as tmp:
   directory=Path(tmp);output=directory/'accounts.parquet';output.write_bytes(b'partial receipt boundary')
   m.recover_unaccepted_output(directory,directory/'receipt.json')
   self.assertFalse(output.exists());orphans=list(directory.glob('orphan-*-accounts.parquet'))
   self.assertEqual(len(orphans),1);self.assertEqual(orphans[0].read_bytes(),b'partial receipt boundary')
 def test_accepted_output_is_never_quarantined(self):
  with tempfile.TemporaryDirectory() as tmp:
   directory=Path(tmp);output=directory/'accounts.parquet';output.write_bytes(b'accepted');receipt=directory/'receipt.json';receipt.write_text('{}')
   with self.assertRaises(ValueError):m.recover_unaccepted_output(directory,receipt)
   self.assertEqual(output.read_bytes(),b'accepted');self.assertFalse(list(directory.glob('orphan-*')))
 def test_priority_does_not_drop_or_overlap_blocks(self):
  stop=26407882;jobs=m.layout(m.START,stop);self.assertEqual(jobs[0],dict(id='16800000-16800000',start=16800000,stop=16800001))
  ordered=sorted(jobs,key=lambda j:j['start']);self.assertEqual(ordered[0]['start'],m.START);self.assertEqual(ordered[-1]['stop'],stop)
  self.assertTrue(all(a['stop']==b['start'] for a,b in zip(ordered,ordered[1:])));self.assertTrue(all(0<j['stop']-j['start']<=100 for j in jobs))
  self.assertEqual(sum(j['stop']-j['start'] for j in jobs),10116755)
 def test_all_seen_users_including_zero_balance_users_are_counted(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'position-owners.sqlite';d=sqlite3.connect(p);d.execute('create table owners(asset,owner,first_block,last_block)');d.executemany('insert into owners values(?,?,?,?)',[('asset1','0xABC',10,20),('asset2','0xabc',8,22),('asset1','0xdef',12,12),('asset1',m.ZERO,1,30),('asset1','0xfuture',30,30)]);d.commit();d.close()
   owners=m.owner_universe(tmp,15);self.assertEqual(owners,[('0xabc',8),('0xdef',12)]);self.assertEqual(m.row_count(owners,10,15),8)
 def test_accepted_coverage_counts_ranges_not_factors_or_prefix(self):
  receipts=[dict(start=20,stop=23,rows=40,bytes=100,validated_samples=3),dict(start=10,stop=12,rows=7,bytes=20,validated_samples=1)]
  c=m.counts(receipts,10,30);self.assertEqual(c['completed_blocks'],5);self.assertEqual(c['contiguous_through_block'],11);self.assertEqual(c['processed_rows'],47)
  with self.assertRaises(ValueError):m.counts(receipts+[dict(start=11,stop=15,rows=0,bytes=0,validated_samples=0)],10,30)
 def test_factor_prefix_requires_current_generation_and_checked_receipt(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);plan=dict(end_block=m.START+1000)
   self.assertIsNone(m.factor_input(root,plan,'new')[0])
   receipt=root/'factor.json';receipt.write_text(json.dumps(dict(start=m.START,stop=m.START+101,fixed_cutoff=plan['end_block'],generation='new')))
   status=dict(state='waiting_for_inputs',completed_blocks=101,generation='new',receipt=str(receipt),receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest())
   (root/'hf-input-progress.json').write_text(json.dumps(status))
   ready,waiting=m.factor_input(root,plan,'new');self.assertIsNone(waiting);self.assertEqual(ready['through'],m.START+100)
   self.assertIsNone(m.factor_input(root,plan,'old')[0])
   receipt.write_text(receipt.read_text()+' ')
   with self.assertRaises(ValueError):m.factor_input(root,plan,'new')
 def test_disk_projection_preserves_primary_pipeline_reserve(self):
  free=60*2**30
  with patch.object(m.shutil,'disk_usage',return_value=type('Disk',(),{'free':free})()):
   self.assertEqual(m.ensure_capacity('.',10*2**30),free)
   with self.assertRaises(m.CapacityWait):m.ensure_capacity('.',11*2**30)

if __name__=='__main__':unittest.main()
