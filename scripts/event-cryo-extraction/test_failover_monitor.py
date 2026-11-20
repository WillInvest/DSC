import time,unittest
from failover_monitor import aggregate,age_cached_report,with_recovery_notice

class FailoverMonitorTests(unittest.TestCase):
 def test_recovery_milestones_do_not_refresh_coverage_or_live_activity(self):
  stage=dict(generation='g',user_values_accepted=7,updated_at=1,activity_unavailable=True,active_compute_processes=None)
  notice=dict(generation='g',recorded_at=100,milestones=['Node copy verified'])
  value=with_recovery_notice(stage,notice)
  self.assertEqual(value['user_values_accepted'],7);self.assertEqual(value['updated_at'],1)
  self.assertTrue(value['activity_unavailable']);self.assertIsNone(value['active_compute_processes'])
  self.assertNotIn('recovery_notice',stage);self.assertEqual(value['recovery_notice']['milestones'],notice['milestones'])
  for invalid in [dict(notice,generation='other'),dict(notice,milestones='not a list'),None]:
   self.assertIs(with_recovery_notice(stage,invalid),stage)

 def independent_fixture(self,state='materializing'):
  now=time.time()
  config=dict(generation='g',manifest_sha256='m',format='hf-vector-f64-v2',sources={'data':'url','node':'url'},workers={'data':2,'node':4},total_jobs=10)
  reports={h:dict(role=h,generation='g',manifest_sha256='m',storage_role='local_then_node_then_drive',state=state,updated_at=now,local_producer_updated_at=now,workers_configured=6 if h=='data' else 12,active_compute_processes=3,user_values_accepted=5,reserve_blocks_accepted=3,protocol_blocks_accepted=3,continuous_end_block=16291126,highest_accepted_block=25200000,gap_jobs=8,bytes=10,local_buffered_bytes=25000000000,node_transferred_bytes=50000000000,node_accepted_jobs=2,local_transfer_target_bytes=125000000000,free_bytes=90000000000) for h in config['sources']}
  return config,reports

 def test_independent_flow_preserves_real_host_stages_and_slots(self):
  config,reports=self.independent_fixture()
  value,error=aggregate(reports,config)
  self.assertIsNone(error)
  self.assertEqual(value['storage_role'],'local_then_node_then_drive')
  self.assertEqual(value['host_flow']['data']['local_buffered_bytes'],25000000000)
  self.assertEqual(value['host_flow']['node']['node_accepted_jobs'],2)
  self.assertEqual(value['host_flow']['data']['local_transfer_target_bytes'],125000000000)
  self.assertEqual(value['distributed']['hosts'],{'data':6,'node':12})
  self.assertEqual(value['distributed']['active_by_host'],{'data':3,'node':3})

 def test_fresh_data_report_cannot_hide_stale_local_producer(self):
  config,reports=self.independent_fixture()
  reports['data']['local_producer_updated_at']=time.time()-120
  value,error=aggregate(reports,config)
  self.assertIn('data: stale local producer heartbeat',error)
  self.assertTrue(value['host_flow']['data']['producer_stale'])
  self.assertEqual(value['distributed']['online_hosts']['data'],0)

 def test_finished_independent_canaries_are_not_full_completion_or_stale(self):
  config,reports=self.independent_fixture('canary_complete')
  for report in reports.values():report['local_producer_updated_at']=0
  value,error=aggregate(reports,config)
  self.assertIsNone(error);self.assertEqual(value['state'],'canary_complete')
  self.assertFalse(value['host_flow']['data']['producer_stale'])

 def test_finished_producer_is_not_stale_while_catalog_finalizes(self):
  config,reports=self.independent_fixture('finalizing_catalog')
  for report in reports.values():
   report['local_producer_state']='complete';report['local_producer_updated_at']=0
  value,error=aggregate(reports,config)
  self.assertIsNone(error);self.assertEqual(value['state'],'materializing')
  self.assertFalse(value['host_flow']['data']['producer_stale'])

 def test_expired_cached_activity_is_unknown_without_changing_accepted_totals(self):
  config,reports=self.independent_fixture();value,error=aggregate(reports,config)
  for flow in value['host_flow'].values():flow['local_producer_updated_at']=100
  value['host_flow']['data']['local_producer_updated_at']=None
  value['updated_at']=100
  aged=age_cached_report(value,now=300)
  self.assertTrue(aged['activity_unavailable']);self.assertEqual(aged['state'],'waiting_for_inputs')
  self.assertIsNone(aged['active_compute_processes']);self.assertEqual(aged['reported_active_compute_processes'],6)
  self.assertEqual(aged['user_values_accepted'],value['user_values_accepted'])
  self.assertTrue(all(f['producer_stale'] for f in aged['host_flow'].values()))
  self.assertEqual(aged['distributed']['online_hosts'],{'data':0,'node':0})
  self.assertEqual(value['active_compute_processes'],6);self.assertFalse(value['host_flow']['data']['producer_stale'])

 def test_terminal_and_fresh_reports_are_not_aged_out(self):
  config,reports=self.independent_fixture('complete');value,error=aggregate(reports,config);value['updated_at']=0
  self.assertIs(age_cached_report(value,now=300),value)
  config,reports=self.independent_fixture();value,error=aggregate(reports,config)
  self.assertIs(age_cached_report(value),value)

 def test_three_disjoint_sources_sum_without_data_tracker(self):
  config=dict(generation='g',manifest_sha256='m',sources={h:'url' for h in ['node','revert','web']},workers=dict(node=14,revert=8,web=6),total_jobs=10)
  reports={h:dict(role=h,generation='g',manifest_sha256='m',state='materializing',updated_at=time.time(),processed_rows=100,completed_blocks=3,new_rows=20,recovered_rows=80) for h in config['sources']}
  value,error=aggregate(reports,config);self.assertIsNone(error);self.assertEqual(value['processed_rows'],300);self.assertEqual(value['completed_blocks'],9);self.assertTrue(value['failover'])
  reports['node']['manifest_sha256']='wrong';value,error=aggregate(reports,config);self.assertEqual(value['processed_rows'],200);self.assertIn('wrong execution identity',error);self.assertEqual(value['state'],'waiting_for_inputs')

 def test_float_trial_watermarks_are_not_summed(self):
  config=dict(generation='g',manifest_sha256='m',format='hf-vector-f64-v2',sources={'data':'url','node':'url'},workers={'data':2,'node':4},total_jobs=10)
  reports={h:dict(role=h,generation='g',manifest_sha256='m',state='canary_complete',updated_at=0,user_values_accepted=5,reserve_blocks_accepted=3,protocol_blocks_accepted=3,continuous_end_block=16291126,highest_accepted_block=25200000,gap_jobs=8,bytes=10,conditional_eta_seconds=100) for h in config['sources']}
  value,error=aggregate(reports,config);self.assertIsNone(error);self.assertEqual(value['state'],'canary_complete');self.assertEqual(value['user_values_accepted'],10);self.assertEqual(value['continuous_end_block'],16291126);self.assertEqual(value['gap_jobs'],8);self.assertIsNone(value['conditional_eta_seconds'])

if __name__=='__main__':unittest.main()
