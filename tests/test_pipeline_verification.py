import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline import verify
from pipeline.stages import Stages
from pipeline.tracker import STAGES
from pipeline.verify import compare_archives


class VerificationTests(unittest.TestCase):
    def records(self,root,reason):
        root.mkdir()
        row=dict(receipt_summary=dict(block_number=10),failed=[dict(receipt=dict(transactionHash='tx'),
                 trace=dict(type='CALL',output='0x08c379a0',error='execution reverted',revertReason=reason))])
        with gzip.open(root/'10-10.jsonl.gz','wt') as stream:stream.write(json.dumps(row)+'\n')

    def test_different_decoded_reason_cannot_pass_exact_replay_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.records(root/'old','Too little received');self.records(root/'new','WRONG REASON')
            with self.assertRaisesRegex(ValueError,'Trace differs'):compare_archives(root/'old',root/'new',10,11)
            self.assertTrue((root/'trace-mismatch.json').exists())

    def test_identical_receipt_and_trace_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.records(root/'old','same');self.records(root/'new','same')
            self.assertEqual(compare_archives(root/'old',root/'new',10,11),dict(exact_traces=1))

    def test_node_replay_compares_downloaded_archives_against_local_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = root/'reference'
            output = root/'cycles/10-10/datasets/reverted'
            old.mkdir()
            output.mkdir(parents=True)
            self.records(old/'archive','same')
            self.records(output/'archive','same')
            for path in [old/'selected/chunks',output/'chunks']:
                path.mkdir(parents=True)
                with gzip.open(path/'10-10.jsonl.gz','wt') as out:
                    out.write(json.dumps(dict(block_number=10,failed=[
                        dict(receipt=dict(transactionHash='tx'),matched_contracts=['pool']),
                    ])))
            plan=root/'plan.json'
            plan.write_text('{"jobs":[]}')
            with patch.object(verify,'copy_legacy_revert_inputs',side_effect=AssertionError):
                result=verify.compare(plan,plan,root,10,11,old)
            self.assertEqual(result['traces'],dict(exact_traces=1))
            self.assertEqual(result['selection_comparison'],'exact')

    def test_replay_cli_leaves_live_pointer_and_exact_gate_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            production = root / 'production'
            production.mkdir()
            pointer = production / 'current.json'
            pointer.write_text('production pointer')
            output = root / 'verification'
            output.mkdir()
            gate = output / 'verification.json'
            gate.write_text('previous exact verification')
            baseline = root / 'baseline.json'
            baseline.write_text(json.dumps(dict(end_block=20, end_hash='0x20', jobs=[])))
            report = {'state': 'passed', 'start': 20, 'stop': 21}

            def complete(tracker, cycle, *_):
                self.assertEqual(tracker.purpose, 'verification')
                for stage in STAGES:
                    tracker.stage(cycle, stage, 'complete', {})
                tracker.finish(cycle)

            with (
                patch.object(sys, 'argv', ['verify.py', '--root', str(output), '--blocks', '1']),
                patch.object(verify, 'DEFAULT', production),
                patch.object(verify, 'HISTORICAL', baseline),
                patch.object(verify, 'rpc', side_effect=lambda m, p: {'hash': '0x20' if p[0] == '0x14' else '0x19'}),
                patch.object(verify, 'run_cycle', side_effect=complete),
                patch.object(verify, 'compare', return_value=report),
                patch('builtins.print'),
            ):
                verify.main()

            self.assertEqual(pointer.read_text(), 'production pointer')
            self.assertEqual(gate.read_text(), 'previous exact verification')
            current = json.loads((output / 'isolated/20-20/current.json').read_text())
            self.assertEqual(current['cycle']['hash'], '0x20')

    def test_verification_separates_all_root_paths_and_rejects_unreviewed_abi(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cycle = dict(start=10, end=20, id='10-20')
            args = (cycle, root / 'prior.json', root / 'profiles', lambda: None)
            production = Stages(root / 'production', *args)
            replay = Stages(root / 'replay', *args, verification=True)
            other = Stages(root / 'another-replay', *args, verification=True)
            self.assertNotEqual(replay.node, production.node)
            self.assertNotEqual(replay.revert, production.revert)
            self.assertNotEqual(replay.node, other.node)
            self.assertNotEqual(replay.revert, other.revert)
            self.assertTrue(replay.data.is_relative_to(replay.root))
            self.assertEqual(replay.env['DSC_AAVE_INPUTS_NODE_ROOT'], replay.node + '/aave-inputs')
            replay.execute = Mock()
            with self.assertRaisesRegex(ValueError, 'requires reviewed aave-v2 coverage'):
                replay.profiles_stage()
            replay.execute.assert_not_called()


if __name__=='__main__':unittest.main()
