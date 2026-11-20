import tempfile, unittest
from pathlib import Path
from hf_flow_queue import assignments, LocalLedger, FlowLedger, read_snapshot

class QueueTests(unittest.TestCase):
    def test_disjoint_complete_assignments_preserve_sources(self):
        jobs=[dict(id=str(i),start=i,stop=i+1,expected_rows=10+i) for i in range(20)]
        p=dict(jobs=jobs,source_roles={'3':'web','4':'revert'},workers={'data':2,'node':4,'revert':2,'web':2})
        a=assignments(p,{'0','1'})
        self.assertEqual(set().union(*map(set,a.values())),{str(i) for i in range(2,20)})
        self.assertEqual(sum(map(len,a.values())),18)
        self.assertIn('3',a['web']);self.assertIn('4',a['revert'])
        self.assertTrue(all(a.values()))
    def test_restart_uses_committed_journal_without_opening_old_parts(self):
        with tempfile.TemporaryDirectory() as d:
            f=Path(d)/'local.sqlite'
            with LocalLedger(f,'g') as db:db.commit('a',{'receipt':{'job_id':'a','bytes':17},'sha256':'x','path':'not-a-real-part','certificate':{}})
            with LocalLedger(f,'g') as db:
                self.assertEqual(db.completed(),{'a'});self.assertEqual(db.entries()[0]['bytes'],17)
                db.commit('a',{'receipt':{'job_id':'a','bytes':17},'sha256':'x','path':'not-a-real-part','certificate':{}})
                with self.assertRaises(ValueError):db.commit('a',{'receipt':{'job_id':'a','bytes':18},'sha256':'different','path':'other','certificate':{}})
    def test_flow_stages_are_transactional_and_restartable(self):
        with tempfile.TemporaryDirectory() as d:
            f=Path(d)/'flow.sqlite'
            with FlowLedger(f,'g') as db:
                db.observe('a','web',{'receipt':{'bytes':17},'sha256':'x'})
                db.observe('a','web',{'receipt':{'bytes':17},'sha256':'x'})
                self.assertEqual(db.pending_transfer('web')[0]['job_id'],'a')
                db.node_commit('a',{'sha256':'y','path':'node/path','receipt':{'bytes':17}})
            with FlowLedger(f,'g') as db:
                self.assertEqual(db.pending_transfer('web'),[])
                self.assertEqual(db.cloud_candidates()[0]['job_id'],'a')
                db.cloud_commit(['a'],{'id':'batch','verified_bytes':17})
                self.assertEqual(db.cloud_candidates(),[])

    def test_node_acceptance_sequence_tracks_a_late_earlier_local_row(self):
        with tempfile.TemporaryDirectory() as d:
            with FlowLedger(Path(d)/'flow.sqlite','g') as db:
                first={'receipt':{'bytes':1},'sha256':'a'};second={'receipt':{'bytes':1},'sha256':'b'}
                db.observe('early-local','web',first);db.observe('late-local','web',second)
                db.node_commit('late-local',second)
                watermark=db.db.execute('select max(sequence) from node_acceptance').fetchone()[0]
                db.node_commit('early-local',first)
                self.assertEqual(db.db.execute('select job_id from node_acceptance where sequence>?',(watermark,)).fetchall(),[('early-local',)])
                db.node_commit('early-local',first)
                self.assertEqual(db.db.execute('select count(*) from node_acceptance').fetchone()[0],2)

    def test_inflight_batches_keep_identity_across_restart(self):
        with tempfile.TemporaryDirectory() as d:
            f=Path(d)/'flow.sqlite'
            with FlowLedger(f,'g') as db:
                db.prepare_operation('b','transfer','web',{'keys':['a']})
            with FlowLedger(f,'g') as db:
                self.assertEqual(db.pending_operations('transfer')[0]['payload'],{'keys':['a']})
                with self.assertRaises(ValueError):db.prepare_operation('b','transfer','web',{'keys':['a','new']})
                db.finish_operation('b',{'all_exact':True})
                self.assertEqual(db.pending_operations('transfer'),[])

    def test_incremental_read_pins_sqlite_snapshot_while_writer_commits(self):
        with tempfile.TemporaryDirectory() as d:
            f=Path(d)/'local.sqlite'
            with LocalLedger(f,'g') as writer:
                writer.commit('a',{'receipt':{'bytes':1},'sha256':'a'})
                with read_snapshot(f) as reader:
                    self.assertEqual(reader.execute('select count(*) from done').fetchone()[0],1)
                    writer.commit('b',{'receipt':{'bytes':2},'sha256':'b'})
                    self.assertEqual(reader.execute('select count(*) from done').fetchone()[0],1)
                with read_snapshot(f) as reader:
                    self.assertEqual(reader.execute('select count(*) from done').fetchone()[0],2)

if __name__=='__main__':unittest.main()
