from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.node_handoff import state_sources, verified_source, verify_boundary_archives
from pipeline.catalog_probe import archive_value, compare
from pipeline.stages import MODE
from reverted.trace_received import select
from shared.artifacts import checksum


class NodeHandoffTests(unittest.TestCase):
    def test_shadow_fingerprint_checks_all_three_archive_families(self):
        markets = {'encoded': {}, 'decoded': {}, 'state': {'pool': ['hash']}}
        archive = {'receipts': 'receipt', 'selected': 'selection', 'traces': 'trace'}
        node = dict(block=7, markets=markets, archives=archive)
        legacy_data = dict(block=7, markets=markets, archives={'selected': 'selection'})
        legacy_revert = dict(block=7, archives=archive)
        self.assertEqual(compare(node, legacy_data, legacy_revert, count=1)['state'], 'passed')
        with self.assertRaisesRegex(ValueError, 'traces or selection'):
            compare(node, legacy_data, dict(legacy_revert, archives=dict(archive, traces='different')), count=1)
        full = dict(receipt_summary={'block_number': 7}, failed=[
            dict(receipt={'transactionHash': 'tx'}, trace={'error': 'reverted'}, provider='provider')])
        self.assertEqual(archive_value('traces', full)['failed'][0],
                         dict(receipt={'transactionHash': 'tx'}, trace={'error': 'reverted'}))

    def test_requires_exactly_one_state_source_per_registered_contract(self):
        state = {'stream': f'state/aave-v3/address/{MODE}/schema'}
        self.assertEqual(state_sources([state, {'stream': 'receipts'}], {'address'}), {'address': state})
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            state_sources([state, {**state, 'stream': f'state/aave-v3/address/{MODE}/other'}], {'address'})
        with self.assertRaisesRegex(ValueError, 'does not cover'):
            state_sources([state], {'address', 'missing'})

    def test_used_catalog_payload_must_match_the_imported_checksum(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root/'partition'
            path.write_bytes(b'accepted')
            row = dict(path=path.name, sha256=checksum(path), start=1, stop=2, absolute_path=str(path))
            elements = {path.name: dict(path=path.name, sha256=row['sha256'], start=1, stop=2,
                                        bytes=path.stat().st_size)}
            self.assertEqual(verified_source(root, elements, row), path)
            path.write_bytes(b'altered!')
            with self.assertRaisesRegex(ValueError, 'does not match'):
                verified_source(root, elements, row)

    def test_boundary_checks_reth_receipts_complete_traces_and_both_selected_copies(self):
        receipt = {'transactionHash': 'tx'}
        scanned = dict(block_number=5, block_hash='hash', failed_receipts=[receipt])
        full = dict(receipt_summary=scanned, failed=[dict(receipt=receipt, trace={'to': 'target'})])
        expected = select(full, {'target'})
        args = (Path('data'), Path('revert'), {}, {}, [], [], {'target'}, 5, scanned)
        correct = [(scanned, {}), (full, {}), (expected, {}), (expected, {})]
        with patch('pipeline.node_handoff.archive_row', side_effect=correct):
            result = verify_boundary_archives(*args)
        self.assertEqual((result['failed_transactions'], result['selected_transactions']), (1, 1))
        with patch('pipeline.node_handoff.archive_row',
                   side_effect=[*correct[:3], (dict(expected, failed=[]), {})]):
            with self.assertRaisesRegex(ValueError, 'selection'):
                verify_boundary_archives(*args)
        with patch('pipeline.node_handoff.archive_row', side_effect=correct):
            with self.assertRaisesRegex(ValueError, 'disagree with RETH'):
                verify_boundary_archives(*args[:-1], dict(scanned, block_hash='changed'))
