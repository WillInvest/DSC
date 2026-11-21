import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import main as entrypoint
from pipeline.compaction.node_store import NodeCollector
from pipeline.compaction.store import Fragment


class NodeStoreTests(unittest.TestCase):
    def test_epoch_preserves_sources_and_keeps_distinct_local_archive_families(self):
        collector = NodeCollector(dict(baseline=dict(end_block=8, jobs=[]),
                                       pipeline_root='/node/work', cycles=[]), 8)
        spec = dict(anchor=1, size=1000, format='jsonl', dense=True, metadata={'historical_before': 5})
        source = Fragment(Path('/node/accepted.jsonl.gz'), 9, 10, retire=True)
        collector.add('receipts', spec, source)
        saved = collector.specs['receipts']
        self.assertEqual((saved['anchor'], saved['metadata']['historical_before']), (9, 9))
        self.assertFalse(saved['fragments'][0].retire)
        self.assertTrue(source.retire)
        collector.current_cycle = '9-9'
        with patch.object(collector, 'json_archive') as archive:
            collector.archives(False)
        self.assertEqual([call.args[1] for call in archive.call_args_list], ['receipts', 'traces', 'selected'])
        self.assertEqual(archive.call_args_list[1].kwargs['hash_field'], 'archive_sha256')
        self.assertEqual(archive.call_args_list[1].args[0],
                         Path('/node/work/cycles/9-9/datasets/reverted/archive'))

    def test_public_node_once_recovers_catalog_without_replaying_accepted_stages(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            args = ['scripts/main.py', 'once', '--root', directory, '--end-block', '10', '--baseline-plan',
                    str(root/'plan'), '--profiles', str(root/'profiles'), '--node-verification', str(root/'gate')]
            stage = Mock()
            for name in ('profiles', 'events', 'decode', 'reconstruct', 'receipts',
                         'receipt_delivery', 'traces', 'selected_delivery'):
                getattr(stage, name+'_stage').return_value = {'accepted': True}
            stage_calls = []

            def initialize(tracker, *_):
                tracker.baseline(9, 'hash9')

            def hash_at(_method, params):
                height = 10 if params[0] == 'finalized' else int(params[0], 16)
                return dict(number=hex(height), hash=f'hash{height}')

            def compact(tracker, _plan):
                stage_calls.append(tracker.position()[0])
                if stage_calls == [9, 10]:
                    raise RuntimeError('catalog interrupted')

            with (
                patch.object(sys, 'argv', args),
                patch('pipeline.main.NODE_LOCAL', True),
                patch('pipeline.main.initialize_node', side_effect=initialize),
                patch('pipeline.main.Stages', return_value=stage),
                patch('pipeline.main.rpc', side_effect=hash_at),
                patch('pipeline.main.signal.signal'),
                patch('pipeline.compaction.node_store.compact_node', side_effect=compact),
            ):
                with self.assertRaisesRegex(RuntimeError, 'catalog interrupted'):
                    entrypoint.main()
                entrypoint.main()
            self.assertEqual(stage_calls, [9, 10, 10])
            self.assertEqual(stage.traces_stage.call_count, 1)
            self.assertEqual(json.loads((root/'progress.json').read_text())['completed_through'], 10)
