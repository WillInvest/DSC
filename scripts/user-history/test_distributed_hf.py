"""Distributed execution preserves job identity, receipts and consistent snapshots."""
import importlib.util,sqlite3,tempfile,unittest,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
spec=importlib.util.spec_from_file_location('distributed_hf',Path(__file__).with_name('distributed_hf.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class DistributedTests(unittest.TestCase):
 @unittest.skipUnless(importlib.util.find_spec('dask') and importlib.util.find_spec('fsspec'),'deployed Dask runtime required')
 def test_file_transfer_finishes_while_original_worker_is_still_busy(self):
  import time,dask,fsspec
  from distributed import Client,LocalCluster
  with tempfile.TemporaryDirectory() as tmp,LocalCluster(n_workers=2,threads_per_worker=1,processes=True,host='127.0.0.1',dashboard_address='127.0.0.1:0') as cluster,Client(cluster) as client:
   addresses=list(client.scheduler_info(n_workers=-1)['workers']);busy=client.submit(time.sleep,8,workers=[addresses[0]],pure=False)
   for _ in range(30):
    if busy.key in client.processing()[addresses[0]]:break
    time.sleep(.1)
   source=Path(tmp)/'source';target=Path(tmp)/'received';source.write_bytes(b'preserved output bytes'*1024)
   workers={a:dict(name='node-'+str(i),resources={'compute':1}) for i,a in enumerate(addresses)}
   peers=m.delivery_workers(workers,dict(role='node',worker=addresses[0]))
   fs=fsspec.filesystem('dask',target_protocol='file',client=None,skip_instance_cache=True)
   with dask.annotate(workers=peers,allow_other_workers=False):fs.get_file(str(source),str(target))
   self.assertEqual(target.read_bytes(),source.read_bytes());self.assertFalse(busy.done())
   busy.result(timeout=10)
 def test_delivery_uses_same_host_peers_while_original_worker_is_busy(self):
  workers={'busy':{'name':'node-0','resources':{'compute':1}},'idle':{'name':'node-1','resources':{'compute':1}},'other':{'name':'web-0','resources':{'compute':1}},'validation':{'name':'validation','resources':{'validator':1}}}
  self.assertEqual(set(m.delivery_workers(workers,dict(role='node',worker='busy'))),{'busy','idle'})
 def test_delivery_never_falls_back_to_another_hosts_output(self):
  with self.assertRaises(ValueError):m.delivery_workers({'web':{'name':'web-0','resources':{'compute':1}}},dict(role='node',worker='gone'))
 def test_snapshot_includes_committed_wal_and_is_readable_without_sidecars(self):
  with tempfile.TemporaryDirectory() as tmp:
   src=Path(tmp)/'live.sqlite';dst=Path(tmp)/'snapshot.sqlite';db=sqlite3.connect(src)
   db.execute('pragma journal_mode=wal');db.execute('create table t(value)');db.execute('insert into t values(37)');db.commit()
   m.backup_sqlite(src,dst)
   ro=sqlite3.connect('file:'+str(dst)+'?mode=ro',uri=True)
   self.assertEqual(ro.execute('select value from t').fetchone()[0],37)
   self.assertEqual(ro.execute('pragma journal_mode').fetchone()[0],'delete');ro.close();db.close()
 def test_delivery_cannot_publish_tampered_or_wrong_job_result(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);receipt=root/'accepted.json';out=root/'accounts.parquet';out.write_bytes(b'bad')
   result={'generation':'g','job_id':'1-1','start':1,'stop':2,'output':str(out),'sha256':'wrong'}
   with self.assertRaises(ValueError):m.publish_result(result,receipt,{'id':'1-1','start':1,'stop':2},'g',root,check=lambda *a: (_ for _ in ()).throw(ValueError('tampered')))
   self.assertFalse(receipt.exists())
 def test_delivery_preserves_existing_receipt_and_requires_post_transfer_check(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);receipt=root/'accepted.json';receipt.write_text('{"immutable":true}')
   with self.assertRaises(FileExistsError):m.publish_result({},receipt,{},'g',root,check=lambda *a: {})
   self.assertEqual(receipt.read_text(),'{"immutable":true}')
 def test_worker_roles_are_explicit_and_total_capacity_is_bounded(self):
  config={'hosts':{'data':{'workers':4},'node':{'workers':14},'web':{'workers':6},'revert':{'workers':8}}}
  self.assertEqual(m.worker_count(config),32)
  with self.assertRaises(ValueError):m.worker_count({'hosts':{'data':{'workers':33}}})
 def test_worker_attestation_rejects_old_snapshot_or_wrong_role_inventory(self):
  config={'hosts':{'data':{'workers':1},'web':{'workers':1}},'snapshot_manifest_sha256':'new','factor_generation':'f'}
  good={'a':{'role':'data','manifest_sha256':'new','factor_generation':'f'},'b':{'role':'web','manifest_sha256':'new','factor_generation':'f'}}
  m.verify_attestations(config,good)
  with self.assertRaises(ValueError):m.verify_attestations(config,{**good,'b':{**good['b'],'manifest_sha256':'old'}})
  with self.assertRaises(ValueError):m.verify_attestations(config,{'a':good['a']})
 def test_bounded_runtime_recycles_without_changing_account_values(self):
  constructed=[]
  class Reference:
   generation='same-math'
   def __init__(self,root,db,plan):self.calls=0;self.closed=False;constructed.append(self)
   def account(self,user,block,**kw):
    self.calls+=1
    if self.calls>2:raise MemoryError('Long-lived runtime exceeded its test budget')
    return {'user':user,'block':block,'mode':kw.get('include_balances')}
   def close(self):self.closed=True
  evaluator=m.RecyclingEvaluator(Reference,'root','db',{'scope':'fixed'},limit=2)
  expected=[{'user':'owner'+str(i),'block':100+i,'mode':False} for i in range(7)]
  actual=[evaluator.account('owner'+str(i),100+i,include_balances=False) for i in range(7)]
  self.assertEqual(actual,expected);self.assertEqual(evaluator.generation,'same-math');self.assertEqual(len(constructed),4)
  evaluator.close();self.assertTrue(all(x.closed for x in constructed))
 def test_rolling_dispatch_refills_before_a_slow_job_finishes(self):
  import threading
  from concurrent.futures import ThreadPoolExecutor
  from types import SimpleNamespace
  started=threading.Event();release=threading.Event();accepted=[]
  def task(item):
   if item=='slow':
    started.set()
    if not release.wait(2):raise TimeoutError('No replacement job was dispatched')
   elif item=='fast':self.assertTrue(started.wait(1))
   else:release.set()
   return item
  with ThreadPoolExecutor(max_workers=2) as pool:
   def submit(item):
    f=pool.submit(task,item);return SimpleNamespace(future=f,result=f.result,payload=item)
   m.rolling_jobs(submit,['slow','fast','replacement'],2,accepted.append,lambda p:None)
  self.assertCountEqual(accepted,['slow','fast','replacement']);self.assertEqual(accepted[0],'fast')
 def test_fresh_generation_calibrates_one_job_and_preserves_svb_priority(self):
  early={'id':'early','start':16291127};svb1={'id':'first','start':16800000};svb2={'id':'second','start':16800100}
  ready=[early,svb1,svb2]
  self.assertEqual(m.submission_phase(ready,[]),[svb1])
  self.assertEqual(m.submission_phase(ready,[{'rows':0}]),[svb1])
  self.assertEqual(m.submission_phase(ready,[{'rows':10}]),[svb1,svb2])
  self.assertEqual(m.submission_phase([early],[{'rows':10}]),[early])
 @unittest.skipUnless(importlib.util.find_spec('dask') and importlib.util.find_spec('fsspec'),'deployed Dask runtime required')
 def test_delivery_checks_actual_bytes_before_any_remote_file_read(self):
  from types import SimpleNamespace
  from unittest.mock import patch
  import materialize_accounts as original
  with tempfile.TemporaryDirectory() as tmp:
   response={'role':'data','receipt':{'bytes':11*2**30,'validation':'v.json'}}
   future=SimpleNamespace(result=lambda:response)
   wrapped=m.DeliveredFuture(SimpleNamespace(client=None),future,{'directory':tmp})
   with patch.object(original.shutil,'disk_usage',return_value=SimpleNamespace(free=60*2**30)),patch('fsspec.filesystem') as transfer:
    with self.assertRaises(original.CapacityWait):wrapped.result()
    transfer.assert_not_called()
if __name__=='__main__':unittest.main()
