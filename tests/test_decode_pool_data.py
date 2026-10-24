import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from eth_abi import encode
from eth_utils import keccak

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':  # Deployed tests live under scripts/tests.
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
SPEC = importlib.util.spec_from_file_location('decoder', ROOT / 'scripts/decoding/decode_pool_data.py')
DECODER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DECODER)
ABI = ROOT / 'scripts/protocols/dex/uniswap/v3/UniswapV3Pool.json'
POOL = '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
OWNER = '0x' + '12' * 20
SENDER = '0x' + '34' * 20


def hexword(kind, value):
    return '0x' + encode([kind], [value]).hex()


def mint():
    return dict(chain_id=1, address=POOL, block_number=13000999,
                block_hash='0x'+'ab'*32, transaction_hash='0x'+'cd'*32,
                transaction_index=2, log_index=7,
                topic0='0x'+keccak(text='Mint(address,address,int24,int24,uint128,uint256,uint256)').hex(),
                topic1=hexword('address', OWNER), topic2=hexword('int24', -120),
                topic3=hexword('int24', 120),
                data='0x'+encode(['address','uint128','uint256','uint256'], [SENDER, 99, 2**200, 3]).hex())


class DecodeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'input'
        self.source.mkdir()
        self.output = self.root / 'decoded'

    def run_decode(self, rows, kind='events'):
        pq.write_table(pa.Table.from_pylist(rows), self.source / 'batch.parquet')
        DECODER.run(kind, self.source, self.output, ABI, POOL)

    def test_mint_field_order_signed_ticks_and_large_integer(self):
        self.run_decode([mint()])
        row = pq.read_table(self.output / 'Mint.parquet').to_pylist()[0]
        self.assertEqual(row['arg__sender'], SENDER)
        self.assertEqual(row['arg__owner'], OWNER)
        self.assertEqual(row['arg__tickLower'], '-120')
        self.assertEqual(row['arg__amount0'], str(2**200))
        self.assertNotIn('data', row)
        self.assertEqual(row['block_hash'], mint()['block_hash'])
        self.assertTrue((self.source / 'batch.parquet').exists())
        before = (self.output / 'manifest.json').read_bytes()
        DECODER.run('events', self.source, self.output, ABI, POOL)
        self.assertEqual(before, (self.output / 'manifest.json').read_bytes())

    def test_state_tuple_uses_output_names_and_preserves_bool(self):
        row = dict(chain_id=1, block_number=13000999, contract_address=POOL,
                   call_data='0x3850c7bd', output_data='0x'+encode(
                       ['uint160','int24','uint16','uint16','uint16','uint8','bool'],
                       [2**96, -10, 2, 3, 4, 0, True]).hex())
        self.run_decode([row], 'state')
        decoded = pq.read_table(self.output / 'slot0.parquet').to_pylist()[0]
        self.assertEqual(decoded['arg__tick'], '-10')
        self.assertEqual(decoded['arg__sqrtPriceX96'], str(2**96))
        self.assertIs(decoded['arg__unlocked'], True)

    def test_null_state_fails_without_publishing(self):
        row = dict(chain_id=1, block_number=13000999, contract_address=POOL,
                   call_data='0x1a686502', output_data=None)
        with self.assertRaises(ValueError):
            self.run_decode([row], 'state')
        self.assertFalse(self.output.exists())
        self.assertTrue((self.source / 'batch.parquet').exists())

    def test_unknown_event_fails_without_publishing_partial_data(self):
        unknown = dict(mint(), topic0='0x'+'00'*32, log_index=8)
        with self.assertRaises(ValueError):
            self.run_decode([mint(), unknown])
        self.assertFalse(self.output.exists())

    def test_missing_indexed_topic_fails(self):
        with self.assertRaises(ValueError):
            self.run_decode([dict(mint(), topic3=None)])
        self.assertFalse(self.output.exists())

    def test_extra_event_data_fails(self):
        row = mint()
        row['data'] += '00' * 32
        with self.assertRaises(ValueError):
            self.run_decode([row])
        self.assertFalse(self.output.exists())

    def test_late_failure_closes_writers_and_removes_staging(self):
        pq.write_table(pa.Table.from_pylist([mint()]), self.source / 'a.parquet')
        pq.write_table(pa.Table.from_pylist([dict(mint(), topic0='0x'+'00'*32)]),
                       self.source / 'b.parquet')
        with self.assertRaises(ValueError):
            DECODER.run('events', self.source, self.output, ABI, POOL)
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.decoded-*')), [])
        self.assertEqual(len(list(self.source.glob('*.parquet'))), 2)

    def test_duplicate_event_fails(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            self.run_decode([mint(), mint()])

    def test_modified_source_is_not_silently_skipped(self):
        self.run_decode([mint()])
        with self.assertRaisesRegex(ValueError, 'different'):
            self.run_decode([dict(mint(), log_index=8)])

    def test_corrupt_output_is_not_accepted_on_resume(self):
        self.run_decode([mint()])
        (self.output / 'Mint.parquet').write_bytes(b'broken')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            DECODER.run('events', self.source, self.output, ABI, POOL)


if __name__ == '__main__':
    unittest.main()
