from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import numpy as np

from accounting_inputs import connect
from hf_f64_contract import STOP
from hf_incremental_compute import initialize_database, wallet_registry, write_tiles
from shared.artifacts import digest
from vector_accept import node_check


class IncrementalComputeTests(unittest.TestCase):
    def test_wallet_ids_append_in_block_and_address_order(self):
        old = [('0xold', STOP-7)]
        with patch('hf_incremental_compute.REGISTRY', digest(old)):
            actual = wallet_registry(old, {'0xz': STOP+1, '0xb': STOP, '0xold': STOP, '0xa': STOP}, STOP, STOP+2)
            self.assertEqual(actual, old + [('0xa', STOP), ('0xb', STOP), ('0xz', STOP+1)])
            with self.assertRaisesRegex(ValueError, 'outside the current window'):
                wallet_registry(old, {'0xmiss': STOP-1}, STOP, STOP+2)

    def test_tile_roundtrip_preserves_infinity_and_new_wallet_first_block(self):
        with TemporaryDirectory() as directory:
            output = Path(directory)
            scope = dict(start=STOP, stop=STOP+2, window_sha256='0'*64, wallet_registry_sha256='1'*64)
            matrix = np.array([[np.inf, np.nan], [1.5, 2.0]])
            parts = list(write_tiles(output, STOP, matrix, [STOP-1, STOP+1], scope))
            self.assertEqual(parts[0]['rows'], 3)
            self.assertTrue((output/parts[0]['path']).is_file())

    def test_delta_database_reuses_opening_reserves_and_log_order(self):
        with TemporaryDirectory() as directory:
            db = connect(directory)
            window = dict(identity=dict(start=5), headers=[dict(block=5, timestamp=333, hash='h')])
            boundary = dict(reserves={'asset': {'id': 2, 'configuration': 'cfg'}})
            factors = dict(pool_events=[[5, 2, 6, 'UserEModeSet', '{}']], snapshots=[])
            positions = dict(tokens={'token': dict(history=[
                dict(owner='user', block=5, transaction_index=2, log_index=7, after='4'),
                dict(owner='user', block=5, transaction_index=1, log_index=5, after='3'),
            ])})
            try:
                initialize_database(db, window, boundary, factors, positions)
                self.assertEqual(db.execute('select scaled from changes order by block,tx,log').fetchall(), [('3',), ('4',)])
                self.assertEqual(db.execute('select block from snapshots').fetchone(), (4,))
            finally:
                db.close()

    def test_incremental_node_validation_uses_pinned_header_instead_of_old_database(self):
        sample = dict(user='wallet', block=STOP, profile='1', account=['3'], block_hash='0xabc')
        with patch('accounting_inputs.profiles', return_value=[dict(key='1', abi=[])]), \
             patch('vector_accept.m.load', return_value={}), \
             patch('vector_accept.m.readonly') as readonly, \
             patch('vector_accept.f.direct_node', return_value=[dict(result=[3], block_hash='0xabc')]):
            result = node_check([sample], dict(start=STOP, stop=STOP+1), expected_headers={STOP: '0xabc'})
            self.assertTrue(result['all_exact'])
            readonly.assert_not_called()
            with self.assertRaisesRegex(ValueError, 'Exact own-node comparison failed'):
                node_check([sample], dict(start=STOP, stop=STOP+1), expected_headers={STOP: '0xdef'})
