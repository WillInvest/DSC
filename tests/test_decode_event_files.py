"""Check historical routing and atomic failure in the parallel adapter."""
from pathlib import Path
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from eth_abi import encode
from web3 import Web3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from decoding.decode_event_files import decode_file


class DecodeFilesTest(unittest.TestCase):
    def fixture(self, root):
        address = '0x' + '11' * 20
        rows = []
        for block in (12, 22):
            rows.append({'chain_id': 1, 'address': address, 'block_number': block,
                         'block_hash': '0x' + f'{block:064x}', 'transaction_hash': '0x' + '22' * 32,
                         'transaction_index': 0, 'log_index': 0,
                         'topic0': Web3.to_hex(Web3.keccak(text='Transfer(address,address,uint256)')),
                         'topic1': '0x' + '00' * 12 + '33' * 20,
                         'topic2': '0x' + '00' * 12 + '44' * 20, 'topic3': None,
                         'data': '0x' + encode(['uint256'], [block]).hex()})
        source = root / 'ethereum__logs__00000012_to_00000031.parquet'
        pq.write_table(pa.Table.from_pylist(rows), source)
        profiles = [{'from_block': 12, 'to_block': 22, 'schema_version': 1, 'abi': 'UniswapV2Pair.json'},
                    {'from_block': 22, 'to_block': 32, 'schema_version': 2, 'abi': 'UniswapV2Pair.json'}]
        return source, address, profiles

    def test_profile_boundary_routes_and_resume_checks_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, address, profiles = self.fixture(root)
            args = (str(source), str(root / 'result'), 'uniswap-v2', address, profiles)
            result = decode_file(args)
            self.assertEqual(result['rows'], 2)
            for version, block in [(1, 12), (2, 22)]:
                table = pq.read_table(root / 'result' / f'schema-v{version}' / 'Transfer.parquet')
                self.assertEqual(table.column('block_number').to_pylist(), [block])
                self.assertEqual(table.column('arg__value').to_pylist(), [str(block)])
            self.assertEqual(decode_file(args)['rows'], 2)
            target = root / 'result' / 'schema-v1' / 'Transfer.parquet'
            target.write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                decode_file(args)

    def test_missing_profile_does_not_publish_partial_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, address, profiles = self.fixture(root)
            with self.assertRaisesRegex(ValueError, 'cover each event'):
                decode_file((str(source), str(root / 'result'), 'uniswap-v2', address, profiles[:1]))
            self.assertFalse((root / 'result').exists())


if __name__ == '__main__':
    unittest.main()
