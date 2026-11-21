import gzip
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from hf_incremental_boundary import FORMAT
from hf_incremental_chain import accepted_terminal_checks, verify_successor
from hf_incremental_run import run
from hf_incremental_token_inputs import log_identity
from shared.artifacts import checksum, digest, write_json


class IncrementalChainTests(unittest.TestCase):
    def test_close_requires_an_exact_reference_at_the_actual_terminal_block(self):
        acceptance = dict(coverage={'structural_exact': True}, validation={
            'within_error_budget': True,
            'exact_reference_node_report': {'all_exact': True, 'checks': [{'block': 7, 'exact': True}]},
        })
        self.assertEqual(accepted_terminal_checks(acceptance, 7), [{'block': 7, 'exact': True}])
        with self.assertRaisesRegex(ValueError, 'no exact account validation'):
            accepted_terminal_checks(acceptance, 8)

    def test_successor_verifies_the_wallet_and_state_bytes_together(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            wallets, state = directory/'wallets.json', directory/'state.json.gz'
            write_json(wallets, [['wallet', 7]])
            identity = {'wallet_file_sha256': checksum(wallets), 'accepted_result_sha256': 'result'}
            generation = digest(identity)
            payload = dict(format=FORMAT, release_generation=generation, exclusive_before_block=9,
                           predecessor_hash='block-hash', input_identity=identity)
            with gzip.open(state, 'wt') as stream:
                json.dump(payload, stream)
            manifest = dict(format=FORMAT, release_generation=generation, next_block=9,
                            predecessor_hash='block-hash', input_identity=identity,
                            checkpoint={'sha256': checksum(state)})
            write_json(directory/'boundary.json', manifest)
            self.assertEqual(verify_successor(directory, identity), manifest)
            with self.assertRaisesRegex(ValueError, 'different HF result'):
                verify_successor(directory, {**identity, 'accepted_result_sha256': 'different'})
            write_json(wallets, [['wrong-wallet', 7]])
            with self.assertRaisesRegex(ValueError, 'wallet dictionary checksum differs'):
                verify_successor(directory, identity)

    def test_log_comparison_includes_payload_address_and_lineage(self):
        log = dict(blockNumber=6, blockHash='0x01', address='0xAB', transactionIndex=2, logIndex=4,
                   transactionHash='0xFF', topics=['0x11'], data='0x22')
        expected = (6, 2, 4, '0xab', '0xff', ('0x11',), '0x22')
        self.assertEqual(log_identity(log, {6: '0x01'}, {'0xab'}), expected)
        for height, address in (({6: '0x02'}, {'0xab'}), ({6: '0x01'}, {'0xff'})):
            with self.assertRaisesRegex(ValueError, 'address and block lineage'):
                log_identity(log, height, address)

    def test_public_runner_commits_only_after_the_successor_is_sealed(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            opening, work = root/'opening', root/'work'
            write_json(opening/'boundary.json', dict(format=FORMAT, next_block=9, predecessor_hash='block-hash',
                                                    release_generation='generation', checkpoint={'sha256': 'state'}))
            write_json(root/'plan.json', {'jobs': []})
            stages = Mock()

            def seal(*args):
                write_json(args[-1]/'boundary.json', {'next_block': 11})
                return {'next_block': 11}

            stages.compute.return_value = {'wallet_values': 3}
            stages.seal_successor.side_effect = seal
            names = ('collect_pool', 'collect_oracle', 'collect_raw_tokens', 'replay_tokens',
                     'collect_factors', 'compute', 'seal_successor')
            replacements = {name: getattr(stages, name) for name in names}
            with patch.multiple('hf_incremental_run', **replacements), \
                 patch('hf_incremental_run.pin_window', return_value={'window_sha256': 'window'}), \
                 patch('hf_incremental_run.read_boundary'), patch('hf_incremental_run.accounting_node.web'):
                evidence = run(opening, 10, root/'catalog', root, root/'cryo', work)
            self.assertEqual([call[0] for call in stages.mock_calls], list(names))
            self.assertEqual(evidence['next_block'], 11)
            progress = json.loads((work/'progress.json').read_text())
            self.assertEqual(progress['stage'], 'locally_verified')
            self.assertEqual(progress['successor_sha256'], checksum(work/'successor/boundary.json'))
