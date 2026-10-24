import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from eth_abi import encode
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from protocols.protocol_config import load_protocol, PROTOCOLS
from protocols.deployment import save_profile, load_deployment
from decoding.decode_pool_data import run

ADDRESS = '0x' + '12' * 20
ASSET = '0x' + '34' * 20


class AaveV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = load_protocol('aave-v2')
        self.profile = dict(protocol='aave-v2', chain_id=1, address=ADDRESS,
                            from_block=100, to_block=101, implementation=ASSET,
                            implementation_code_sha256='00'*32,
                            abi_sha256=hashlib.sha256(self.config['abi'].read_bytes()).hexdigest())

    def test_v2_reserve_schema_and_coverage(self):
        self.assertIn('aave-v2', PROTOCOLS)
        source = self.root/'state'
        save_profile(source, self.profile)
        (source/'extraction.json').write_text(json.dumps(dict(protocol='aave-v2',address=ADDRESS,blocks='100')))
        contract = Web3().eth.contract(abi=json.loads(self.config['abi'].read_text()))
        reserve_type = '((uint256),uint128,uint128,uint128,uint128,uint128,uint40,address,address,address,address,uint8)'
        value = ((123,), 2**100, 222, 333, 444, 555, 1700000000, ADDRESS, ADDRESS, ADDRESS, ADDRESS, 7)
        for folder, call, output in [
            ('discovery',contract.encode_abi('getReservesList'),encode(['address[]'],[[ASSET]])),
            ('reserves/100',contract.encode_abi('getReserveData',args=[ASSET]),encode([reserve_type],[value]))]:
            target=source/folder;target.mkdir(parents=True)
            row=dict(chain_id=1,contract_address=ADDRESS,block_number=100,call_data=call,output_data='0x'+output.hex())
            pq.write_table(pa.Table.from_pylist([row]),target/'batch.parquet')
        run('state',source,self.root/'decoded',self.config['abi'],ADDRESS,'aave-v2')
        row=pq.read_table(self.root/'decoded/getReserveData.parquet').to_pylist()[0]
        self.assertEqual(row['protocol'],'aave-v2')
        self.assertEqual(row['arg__input_asset'],ASSET)
        self.assertEqual(row['arg__output_0']['liquidityIndex'],str(2**100))
        self.assertEqual(row['arg__output_0']['variableBorrowIndex'],'222')
        self.assertEqual(row['arg__output_0']['currentLiquidityRate'],'333')
        self.assertEqual(row['arg__output_0']['id'],'7')
        self.assertNotIn('accruedToTreasury',row['arg__output_0'])

    def test_deposit_event_and_v3_profile_rejection(self):
        source=self.root/'events';save_profile(source,self.profile)
        row=dict(chain_id=1,address=ADDRESS,block_number=100,block_hash='0x'+'aa'*32,
                 transaction_hash='0x'+'bb'*32,transaction_index=0,log_index=0,
                 topic0='0x'+Web3.keccak(text='Deposit(address,address,address,uint256,uint16)').hex(),
                 topic1='0x'+encode(['address'],[ASSET]).hex(),
                 topic2='0x'+encode(['address'],[ADDRESS]).hex(),
                 topic3='0x'+encode(['uint16'],[0]).hex(),
                 data='0x'+encode(['address','uint256'],[ADDRESS,2**200]).hex())
        pq.write_table(pa.Table.from_pylist([row]),source/'batch.parquet')
        run('events',source,self.root/'decoded',self.config['abi'],ADDRESS,'aave-v2')
        result=pq.read_table(self.root/'decoded/Deposit.parquet').to_pylist()[0]
        self.assertEqual(result['arg__reserve'],ASSET)
        self.assertEqual(result['arg__amount'],str(2**200))
        with self.assertRaisesRegex(ValueError,'protocol'):
            load_deployment(source/'deployment.json',load_protocol('aave-v3'),ADDRESS)
