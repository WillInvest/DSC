import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import main as entrypoint
from pipeline.tracker import Tracker
from shared.artifacts import checksum, write_json


class PipelineMainTests(unittest.TestCase):
    def test_node_continuous_requires_the_fenced_handoff_before_opening_a_tracker(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root/'accepted/plan.json'
            handoff = baseline.with_name('handoff.json')
            authorization = root/'fence.json'
            coordinator = root/'coordinator'
            write_json(baseline, {'end_block': 110})
            write_json(handoff, {'identity': {'through': 110}, 'baseline_sha256': checksum(baseline)})
            args = ['scripts/main.py', 'continuous', '--root', str(coordinator), '--baseline-plan', str(baseline),
                    '--profiles', str(root/'profiles'), '--node-verification', str(root/'replay')]
            with (
                patch('pipeline.main.NODE_LOCAL', True),
                patch('pipeline.main.Tracker') as tracked,
                patch.object(sys, 'argv', args),
            ):
                with self.assertRaises(SystemExit):
                    entrypoint.main()
                args.extend(['--node-production-fence', str(authorization)])
                fence = dict(format='dsc-node-production-fence-v1', legacy_writer='dsc-pipeline.service',
                             legacy_state='masked', coordinator_root=str(coordinator.resolve()), through=110,
                             handoff_sha256=checksum(handoff))
                write_json(authorization, dict(fence, coordinator_root=str(root/'other')))
                with self.assertRaisesRegex(ValueError, 'authorization differs'):
                    entrypoint.main()
                tracked.assert_not_called()
                self.assertFalse(coordinator.exists())

                write_json(authorization, fence)
                tracked.side_effect = RuntimeError('tracker reached')
                with self.assertRaisesRegex(RuntimeError, 'tracker reached'):
                    entrypoint.main()
                tracked.assert_called_once_with(coordinator)

    def test_once_resumes_the_pinned_cycle_and_skips_accepted_stages(self):
        stage_order = [
            'prepare', 'profiles', 'events', 'decode', 'reconstruct',
            'receipts', 'receipt_delivery', 'traces', 'selected_delivery',
        ]
        calls = []
        interrupted = False

        def run_stage(stage, cycle):
            nonlocal interrupted
            self.assertEqual((cycle['start'], cycle['end'], cycle['hash']), (101, 110, 'hash110'))
            calls.append(stage)
            if stage == 'decode' and not interrupted:
                interrupted = True
                raise RuntimeError('worker interrupted')
            return {'accepted': stage}

        def workers(root, cycle, previous_plan, previous_profiles, heartbeat, *, verification):
            self.assertFalse(verification)
            stages = Mock()
            stages.prepare.side_effect = lambda: run_stage('prepare', cycle)
            for name in stage_order[1:]:
                getattr(stages, f'{name}_stage').side_effect = (
                    lambda stage=name: run_stage(stage, cycle)
                )
            return stages

        with TemporaryDirectory() as directory:
            root = Path(directory)
            tracker = Tracker(root)
            tracker.baseline(100, 'hash100')
            tracker.close()

            def rpc(method, params):
                height = 110 if params[0] == 'finalized' else int(params[0], 16)
                return {'number': hex(height), 'hash': f'hash{height}'}

            with (
                patch.object(sys, 'argv', ['scripts/main.py', 'once', '--root', directory]),
                patch('pipeline.main.Stages', side_effect=workers),
                patch('pipeline.main.rpc', side_effect=rpc) as pinned_rpc,
                patch('pipeline.compaction.run.compact_completed'),
                patch('pipeline.main.signal.signal'),
            ):
                with self.assertRaisesRegex(RuntimeError, 'worker interrupted'):
                    entrypoint.main()
                failure = json.loads((root / 'progress.json').read_text())
                self.assertEqual((failure['stage'], failure['state']), ('decode', 'error'))
                self.assertEqual(failure['completed_through'], 100)
                self.assertEqual(calls, stage_order[:4])

                entrypoint.main()

            self.assertEqual(calls, stage_order[:4] + ['prepare'] + stage_order[3:])
            self.assertEqual(pinned_rpc.call_count, 2)
            result = json.loads((root / 'progress.json').read_text())
            self.assertEqual((result['state'], result['completed_through']), ('complete', 110))
            self.assertTrue(all(stage['state'] == 'complete' for stage in result['stages']))
