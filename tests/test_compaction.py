import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import pyarrow as pa
import pyarrow.parquet as pq
from pipeline.compaction.store import Store, Fragment
from pipeline.compaction.reader import snapshot
from pipeline.compaction.run import compact_completed
from pipeline.tracker import Tracker,STAGES


class CompactionTests(unittest.TestCase):
    def parquet(self,path,start,stop):
        path.parent.mkdir(parents=True,exist_ok=True)
        pq.write_table(pa.table({'block_number':pa.array(range(start,stop),type=pa.uint64()),
                                'state_json':[json.dumps({'reserve':n}) for n in range(start,stop)]}),path)
        return Fragment(path,start,stop,retire=True)

    def test_partial_tail_grows_on_original_alignment_and_seals(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'source/a.parquet',13,17)
            with Store(root/'store') as store:
                store.compact(16,{'pool/state':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[a])})
                first=store.files('pool/state');self.assertEqual([(x['start'],x['stop'],x['sealed']) for x in first],[(13,17,0)])
                b=self.parquet(root/'source/b.parquet',17,26)
                store.compact(25,{'pool/state':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[b])})
                self.assertEqual([(x['start'],x['stop'],x['sealed']) for x in store.files('pool/state')],[(13,23,1),(23,26,0)])
                values=[r['block_number'] for f in store.files('pool/state') for r in pq.read_table(root/'store'/f['path']).to_pylist()]
                self.assertEqual(values,list(range(13,26)))
                self.assertFalse(a.path.exists());self.assertFalse(b.path.exists())
                self.assertFalse((root/'store'/first[0]['path']).exists())
                store.compact(25,{})
                self.assertEqual(len(store.files('pool/state')),2)

    def test_write_failure_keeps_sources_and_previous_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'a.parquet',13,17)
            with Store(root/'store') as store:
                store.compact(16,{'s':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[a])})
                before=store.files('s');b=self.parquet(root/'b.parquet',17,20)
                with patch('pipeline.compaction.parquet.pq.write_table',side_effect=OSError('disk full')):
                    with self.assertRaises(OSError):store.compact(19,{'s':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[b])})
                self.assertEqual(store.files('s'),before);self.assertTrue(b.path.exists())
                self.assertEqual(store.head,16)

    def test_jsonl_preserves_complete_nested_records_and_original_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source.gz'
            records=[dict(receipt_summary=dict(block_number=n),identity='a' if n<18 else 'b',
                          failed=[dict(trace=dict(type='CALL',calls=[dict(error='execution reverted')]))]) for n in range(15,26)]
            with gzip.open(source,'wt') as stream:
                for row in records:stream.write(json.dumps(row)+'\n')
            with Store(root/'store') as store:
                store.compact(25,{'traces':dict(format='jsonl',anchor=5,size=10,dense=True,
                                              fragments=[Fragment(source,15,26,retire=True)])})
                self.assertEqual([(x['start'],x['stop']) for x in store.files('traces')],[(15,25),(25,26)])
                result=[]
                for f in store.files('traces'):
                    with gzip.open(root/'store'/f['path'],'rt') as stream:result.extend(json.loads(line) for line in stream)
                self.assertEqual(result,records);self.assertFalse(source.exists())

    def test_historical_seed_is_preserved_and_schema_streams_remain_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'historical.parquet',13,17);a.retire=False
            b=self.parquet(root/'new.parquet',17,19)
            with Store(root/'store') as store:
                store.compact(18,{'schema-v1':dict(format='parquet',anchor=3,size=10,dense=False,fragments=[a]),
                                  'schema-v2':dict(format='parquet',anchor=3,size=10,dense=False,fragments=[b])})
                self.assertTrue(a.path.exists());self.assertFalse(b.path.exists())
                self.assertEqual(len(store.files()),2)

    def test_restart_finishes_retirement_after_catalog_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'source.parquet',13,17)
            with Store(root/'store') as store:
                with patch.object(store,'cleanup',side_effect=[None,RuntimeError('interrupted')]):
                    with self.assertRaises(RuntimeError):store.compact(16,{'s':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[a])})
                self.assertEqual(store.head,16);self.assertTrue(a.path.exists())
            with Store(root/'store') as store:
                store.compact(16,{})
                self.assertFalse(a.path.exists());self.assertEqual(len(store.files('s')),1)

    def test_overlapping_inputs_are_not_silently_duplicated(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'a.parquet',13,17);b=self.parquet(root/'b.parquet',16,20)
            with Store(root/'store') as store:
                with self.assertRaisesRegex(ValueError,'Overlapping'):
                    store.compact(19,{'s':dict(format='parquet',anchor=3,size=10,dense=False,fragments=[a,b])})
                self.assertIsNone(store.head);self.assertTrue(a.path.exists());self.assertTrue(b.path.exists())

    def test_unexpected_target_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'source.parquet',13,17)
            target=root/'store/s/13-16.parquet';target.parent.mkdir(parents=True);target.write_bytes(b'keep this file')
            with Store(root/'store') as store:
                with self.assertRaisesRegex(ValueError,'target conflict'):
                    store.compact(16,{'s':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[a])})
                self.assertIsNone(store.head);self.assertTrue(a.path.exists())
                self.assertEqual(target.read_bytes(),b'keep this file')

    def test_replaced_source_is_preserved_during_retirement_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'source.parquet',13,17)
            with Store(root/'store') as store:
                with patch.object(store,'cleanup',side_effect=[None,RuntimeError('interrupted')]):
                    with self.assertRaises(RuntimeError):store.compact(16,{'s':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[a])})
                replacement=root/'replacement.parquet';replacement.write_bytes(a.path.read_bytes());replacement.replace(a.path)
            with Store(root/'store') as store:
                with self.assertRaisesRegex(ValueError,'identity changed'):store.compact(16,{})
                self.assertTrue(a.path.exists());self.assertEqual(len(store.files('s')),1)

    def test_catalog_snapshot_reads_only_accepted_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);a=self.parquet(root/'source.parquet',13,17)
            with Store(root/'store') as store:
                store.compact(16,{'s':dict(format='parquet',anchor=3,size=10,dense=True,fragments=[a])})
            with snapshot(root/'store') as files:
                self.assertEqual(len(files),1)
                self.assertEqual(pq.read_table(files[0]['absolute_path']).column('block_number').to_pylist(),[13,14,15,16])

    def test_request_uses_sql_watermark_and_only_new_cycles(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);tracker=Tracker(root/'pipeline');tracker.baseline(100,'h100')
            for end in (110,120):
                c=tracker.begin(end,'h'+str(end))
                for stage in STAGES:tracker.stage(c,stage,'complete',{})
                tracker.finish(c)
            tracker.db.execute('insert into compactions values(?,?,?)',(110,'{}',0));tracker.db.commit()
            plan=root/'baseline/plan.json';plan.parent.mkdir();plan.write_text(json.dumps(dict(end_block=100,jobs=[])))
            new=tracker.root/'cycles/111-120/events/plan.json';new.parent.mkdir(parents=True);new.write_text(json.dumps(dict(jobs=[])))
            (tracker.root/'compaction.json').write_text(json.dumps(dict(state='complete',through=120)))
            roots={name:root/name for name in ('node','revert','data')}
            for p in roots.values():p.mkdir();(p/'progress.json').write_text(json.dumps(dict(state='complete',through=120)))
            with patch('pipeline.compaction.run.ROOTS',roots),patch('pipeline.compaction.run.transfer'),patch('pipeline.compaction.run.run'),patch('pipeline.compaction.run.remote_json',return_value=dict(state='complete',through=120)):
                compact_completed(tracker,plan)
            request=json.loads((tracker.root/'compaction-request.json').read_text())
            self.assertEqual([c['id'] for c in request['cycles']],['111-120'])
            self.assertEqual(tracker.db.execute('select max(through) from compactions').fetchone()[0],120)
            tracker.close()


if __name__=='__main__':unittest.main()
