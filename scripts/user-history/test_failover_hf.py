"""Failover keeps coverage exact and each interval owned by one surviving host."""
import unittest
from failover_hf import split_job,assign_jobs,merge_inventories

class FailoverTests(unittest.TestCase):
 def test_split_bounds_rows_without_omitting_a_block(self):
  jobs=split_job(dict(start=10,stop=25,id='10-24'),[('a',10),('b',12),('c',15)],10)
  self.assertEqual(jobs[0]['start'],10);self.assertEqual(jobs[-1]['stop'],25)
  self.assertTrue(all(a['stop']==b['start'] for a,b in zip(jobs,jobs[1:])))
  self.assertTrue(all(sum(max(0,j['stop']-max(j['start'],first)) for _,first in [('a',10),('b',12),('c',15)])<=10 for j in jobs))
 def test_survivor_assignment_is_disjoint_and_deterministic(self):
  jobs=[dict(id=str(i),start=i,stop=i+1,expected_rows=100+i) for i in range(20)]
  a=assign_jobs(jobs,dict(node=5,revert=14,web=11));b=assign_jobs(jobs,dict(node=5,revert=14,web=11))
  self.assertEqual(a,b);self.assertEqual(len(a),20);self.assertEqual(set(a),{j['id'] for j in jobs});self.assertEqual(set(a.values()),{'node','revert','web'})
 def test_duplicate_recovery_requires_identical_output_identity(self):
  receipt=dict(job_id='1-1',start=1,stop=2,rows=2,sha256='same',generation='old',owner_first_seen_sha256='owners')
  inputs=[dict(role='node',valid=[dict(receipt=receipt,path='/node/r.json')]),dict(role='web',valid=[dict(receipt=receipt,path='/web/r.json')])]
  merged=merge_inventories(inputs);self.assertEqual(len(merged),1);self.assertEqual(len(merged['1-1']['replicas']),2)
  inputs[1]['valid'][0]['receipt']=dict(receipt,sha256='different')
  with self.assertRaises(ValueError):merge_inventories(inputs)

if __name__=='__main__':unittest.main()
