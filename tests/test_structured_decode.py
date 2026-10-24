"""Structured ABI compatibility, independent of protocol-specific fixtures."""
import sys
import unittest
from pathlib import Path
from eth_abi import encode
from eth_utils import event_abi_to_log_topic
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from decoding.pool_decode import EventDecoder, StateDecoder

ABI = [
    {'type': 'function', 'name': 'getReserveData', 'stateMutability': 'view',
     'inputs': [{'name': 'asset', 'type': 'address'}],
     'outputs': [{'name': 'reserve', 'type': 'tuple', 'components': [
         {'name': 'index', 'type': 'uint256'}, {'name': 'tokens', 'type': 'address[]'}]}]},
    {'type': 'event', 'name': 'Balances', 'anonymous': False,
     'inputs': [{'name': 'amounts', 'type': 'uint256[]', 'indexed': False}]},
]
ASSET = '0x' + '12' * 20


class StructuredDecodeTests(unittest.TestCase):
    def test_parameterized_tuple_return(self):
        data = Web3().eth.contract(abi=ABI).encode_abi('getReserveData', args=[ASSET])
        row = {'block_number': 100, 'call_data': data,
               'output_data': '0x' + encode(['(uint256,address[])'], [(2**200, [ASSET])]).hex()}
        name, fields, values, key = StateDecoder(ABI, ['getReserveData']).decode(row)
        self.assertEqual(name, 'getReserveData')
        self.assertEqual(fields[0]['name'], 'input_asset')
        self.assertEqual(values[0].lower(), ASSET)
        self.assertEqual(values[1], (2**200, (ASSET,)))
        self.assertEqual(key, (100, data))

    def test_dynamic_event(self):
        row = dict(address=ASSET, block_number=100, block_hash='0x'+'aa'*32,
                   transaction_hash='0x'+'bb'*32, transaction_index=0, log_index=0,
                   topic0='0x'+event_abi_to_log_topic(ABI[1]).hex(),
                   data='0x'+encode(['uint256[]'], [[1, 2**200]]).hex())
        self.assertEqual(EventDecoder(ABI).decode(row)[2][0], [1, 2**200])
        with self.assertRaises(ValueError):
            EventDecoder(ABI).decode(dict(row, data=row['data']+'00'*32))
