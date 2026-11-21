from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from hf_incremental_positions import GHO_TOKEN, UPGRADE_TOPIC, decode_logs, replay_events
from hf_incremental_token_inputs import read_token_inputs
from shared.artifacts import digest, write_json
from web3 import Web3


class IncrementalPositionTests(TestCase):
    def test_replay_hydrates_existing_sender_and_zero_address_balance(self):
        worker = Mock()
        zero = '0x' + '0' * 40
        event = dict(name='BalanceTransfer', arg__from='alice', arg__to=zero)
        state = dict(positions={'alice': {'atoken': '100'}, zero: {'atoken': '9'}})
        replay_events(worker, 'atoken', 'aToken', {}, [2], state, [event])
        worker.call.assert_called_once_with(dict(initial={'alice': '100', zero: '9'},
                                                 events=[event], modern_versions=[2], debt=False))

    def test_gho_uses_discount_worker_with_the_sealed_discount_state(self):
        worker = Mock()
        implementation = '0x9b2b73f9ddd830f82d61520388ccf4fc048f9953'
        profile = dict(implementation=implementation, version=6)
        event = dict(name='Burn', arg__from='alice')
        state = dict(positions={'alice': {GHO_TOKEN: '100'}}, gho={'alice': ['55', '4']})
        replay_events(worker, GHO_TOKEN, 'variableDebtToken', profile, [6], state, [event])
        worker.call.assert_called_once_with(dict(
            operation='replay', initial={'alice': dict(shares='100', previous_index='55', discount_percent='4')},
            events=[event], profiles={'6': implementation}, modern_versions=[6]))
        with self.assertRaisesRegex(ValueError, 'Opening GHO discount state missing'):
            replay_events(worker, GHO_TOKEN, 'variableDebtToken', profile, [6],
                          {**state, 'gho': {}}, [event])

    def test_rejects_a_token_upgrade_or_changed_block_lineage_before_decoding(self):
        row = dict(block_number=4, block_hash='hash', topic0=UPGRADE_TOPIC)
        with patch('hf_incremental_positions.PoolDecoder') as decoder:
            with self.assertRaisesRegex(ValueError, 'requires a reviewed profile'):
                decode_logs([row], 'token', 1, Path('abi'), {4: 'hash'})
            with self.assertRaisesRegex(ValueError, 'outside the pinned block lineage'):
                decode_logs([row], 'token', 1, Path('abi'), {4: 'wrong-hash'})
            decoder.return_value.decode_row.assert_not_called()

    def test_raw_manifest_rejects_registry_mismatch_and_escaped_payload(self):
        token = '0x' + '1' * 40
        window = dict(identity=dict(start=14, stop=16), window_sha256='window')
        jobs = {token: {}}
        evidence = dict(window_sha256='window', start=14, stop=16, addresses=1,
                        historical_registry_sha256='other', exact_reth_comparison=True,
                        events=1, files=[dict(path='../outside.parquet', sha256='sha', rows=1)])
        with TemporaryDirectory() as temporary:
            manifest = Path(temporary) / 'manifest.json'
            write_json(manifest, evidence)
            with self.assertRaisesRegex(ValueError, 'manifest scope differs'):
                read_token_inputs(window, manifest, jobs)
            write_json(manifest, {**evidence, 'historical_registry_sha256':
                                  digest([Web3.to_checksum_address(token)])})
            with self.assertRaisesRegex(ValueError, 'path or checksum differs'):
                read_token_inputs(window, manifest, jobs)
