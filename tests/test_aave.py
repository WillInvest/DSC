import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from eth_abi import encode
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from protocols.protocol_config import load_protocol
from protocols.deployment import load_deployment, verify_deployment, check_block, save_profile
from extraction.extract_reserve_state import extract
from decoding.decode_pool_data import run
from validation.reserve_state import verify_reserves

ADDRESS = '0x' + '12' * 20
ASSETS = ['0x' + '34' * 20, '0x' + '56' * 20]
RESERVE_TYPE = '((uint256),uint128,uint128,uint128,uint128,uint128,uint40,uint16,address,address,address,address,uint128,uint128,uint128)'
RESERVE = ((123,), 2**100, 3, 4, 5, 6, 1700000000, 0, ADDRESS, ADDRESS, ADDRESS, ADDRESS, 7, 8, 9)


class AaveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = load_protocol('aave-v3')
        self.contract = Web3().eth.contract(abi=json.loads(self.config['abi'].read_text()))
        self.profile = dict(protocol='aave-v3', chain_id=1, address=ADDRESS,
                            from_block=100, to_block=102, implementation=ASSETS[0],
                            implementation_code_sha256=hashlib.sha256(b'code').hexdigest(),
                            abi_sha256=hashlib.sha256(self.config['abi'].read_bytes()).hexdigest())
        self.profile_path = self.root / 'profile.json'
        self.profile_path.write_text(json.dumps(self.profile))

    def fake_cryo(self, address, calls, blocks, output, label):
        output.mkdir(parents=True, exist_ok=True)
        heights = [100, 101] if ':' in blocks else [int(blocks)]
        rows = []
        for height in heights:
            for call in calls:
                function, args = self.contract.decode_function_input(call)
                if function.fn_name == 'getReservesList':
                    data = encode(['address[]'], [ASSETS if height == 100 else ASSETS[:1]])
                else:
                    data = encode([RESERVE_TYPE], [RESERVE])
                rows.append(dict(chain_id=1, contract_address=address, block_number=height,
                                 call_data=call, output_data='0x'+data.hex()))
        pq.write_table(pa.Table.from_pylist(rows), output / 'batch.parquet')

    def extracted(self):
        output = self.root / 'raw'
        with patch('extraction.extract_reserve_state.verify_deployment'), patch(
                'extraction.extract_reserve_state.cryo_calls', side_effect=self.fake_cryo):
            extract(self.config, ADDRESS, '100:102', output, self.profile, Mock())
        return output

    def test_reserve_discovery_per_block_and_structured_parquet(self):
        source = self.extracted()
        self.assertEqual(verify_reserves(source, self.config, ADDRESS, range(100,102)), 5)
        output = self.root / 'decoded'
        run('state', source, output, self.config['abi'], ADDRESS, 'aave-v3')
        rows = pq.read_table(output / 'getReserveData.parquet').to_pylist()
        self.assertEqual(len(rows), 3)
        self.assertEqual({r['arg__input_asset'] for r in rows if r['block_number'] == 100}, set(ASSETS))
        self.assertEqual(rows[0]['arg__output_0']['configuration'], {'data': '123'})
        self.assertEqual(rows[0]['arg__output_0']['liquidityIndex'], str(2**100))
        self.assertEqual(rows[0]['schema_version'], 1)
        lists = pq.read_table(output / 'getReservesList.parquet').to_pylist()
        self.assertEqual(lists[0]['arg__output_0'], ASSETS)
        run('state', source, output, self.config['abi'], ADDRESS, 'aave-v3')

    def test_missing_reserve_call_fails_decoding(self):
        source = self.extracted()
        path = source / 'reserves/100/batch.parquet'
        pq.write_table(pq.read_table(path).slice(0, 1), path)
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            run('state', source, self.root/'decoded', self.config['abi'], ADDRESS, 'aave-v3')
        self.assertFalse((self.root/'decoded').exists())

    def test_malformed_response_fails(self):
        source = self.extracted()
        path = source / 'reserves/100/batch.parquet'
        rows = pq.read_table(path).to_pylist()
        rows[0]['output_data'] = '0x00'
        pq.write_table(pa.Table.from_pylist(rows), path)
        with self.assertRaises(Exception):
            verify_reserves(source, self.config, ADDRESS, range(100,102))

    def test_profile_bounds_and_abi_pin(self):
        self.assertEqual(load_deployment(self.profile_path, self.config, ADDRESS), self.profile)
        with self.assertRaisesRegex(ValueError, 'outside'):
            check_block(self.profile, 102)
        self.profile_path.write_text(json.dumps(dict(self.profile, abi_sha256='00'*32)))
        with self.assertRaisesRegex(ValueError, 'ABI pin'):
            load_deployment(self.profile_path, self.config, ADDRESS)

    def test_proxy_upgrade_is_rejected(self):
        w3 = Mock()
        w3.eth.chain_id = 1
        w3.eth.get_storage_at.return_value = bytes.fromhex('00'*12 + ASSETS[0][2:])
        w3.eth.get_logs.return_value = []
        w3.eth.get_code.return_value = b'code'
        verify_deployment(w3, self.profile, range(100,102))
        w3.eth.get_logs.return_value = [{'blockNumber':100}]
        with self.assertRaisesRegex(ValueError, 'upgrade'):
            verify_deployment(w3, self.profile, range(100,102))

    def test_pool_supply_event(self):
        source = self.root/'events'
        save_profile(source, self.profile)
        signature = 'Supply(address,address,address,uint256,uint16)'
        row = dict(chain_id=1,address=ADDRESS,block_number=100,block_hash='0x'+'aa'*32,
                   transaction_hash='0x'+'bb'*32,transaction_index=0,log_index=0,
                   topic0='0x'+Web3.keccak(text=signature).hex(),
                   topic1='0x'+encode(['address'],[ASSETS[0]]).hex(),
                   topic2='0x'+encode(['address'],[ADDRESS]).hex(),
                   topic3='0x'+encode(['uint16'],[0]).hex(),
                   data='0x'+encode(['address','uint256'],[ADDRESS,2**200]).hex())
        pq.write_table(pa.Table.from_pylist([row]),source/'batch.parquet')
        run('events',source,self.root/'decoded',self.config['abi'],ADDRESS,'aave-v3')
        result=pq.read_table(self.root/'decoded/Supply.parquet').to_pylist()[0]
        self.assertEqual(result['arg__reserve'],ASSETS[0])
        self.assertEqual(result['arg__amount'],str(2**200))
