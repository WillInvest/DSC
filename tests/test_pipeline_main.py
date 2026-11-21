import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import main as entrypoint
from pipeline.tracker import Tracker


class PipelineMainTests(unittest.TestCase):
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
