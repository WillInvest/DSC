import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from eth_abi import encode
from eth_utils import function_abi_to_4byte_selector, event_abi_to_log_topic

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from protocols.protocol_config import load_protocol
from decoding.decode_pool_data import run

ADDRESS = '0x' + '11' * 20  # Synthetic fixture, not a selected extraction contract.


class ProtocolTests(unittest.TestCase):
    def test_catalogs_match_official_abi(self):
        for name in ('uniswap-v2', 'uniswap-v3'):
            config = load_protocol(name)
            abi = json.loads(config['abi'].read_text())
            functions = {e['name']: e for e in abi if e['type'] == 'function'}
            lines = config['catalog'].read_text().splitlines()
            self.assertEqual([line.split('\t')[0] for line in lines], config['state_getters'])
            for line in lines:
                getter, selector, length = line.split('\t')
                fn = functions[getter]
                self.assertEqual(fn['inputs'], [])
                self.assertEqual(selector, '0x' + function_abi_to_4byte_selector(fn).hex())
                self.assertEqual(int(length), 32 * len(fn['outputs']))

    def test_v2_swap_and_reserves(self):
        config = load_protocol('uniswap-v2')
        abi = json.loads(config['abi'].read_text())
        swap = next(e for e in abi if e.get('name') == 'Swap')
        event = dict(chain_id=1, address=ADDRESS, block_number=100,
                     block_hash='0x'+'ab'*32, transaction_hash='0x'+'cd'*32,
                     transaction_index=1, log_index=2,
                     topic0='0x'+event_abi_to_log_topic(swap).hex(),
                     topic1='0x'+encode(['address'],[ADDRESS]).hex(),
                     topic2='0x'+encode(['address'],[ADDRESS]).hex(), topic3=None,
                     data='0x'+encode(['uint256']*4,[123,0,0,456]).hex())
        state = dict(chain_id=1, contract_address=ADDRESS, block_number=100,
                     call_data='0x0902f1ac', output_data='0x'+encode(
                         ['uint112','uint112','uint32'],[2**100,456,1700000000]).hex())
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for kind, row in [('events',event),('state',state)]:
                source=root/kind; source.mkdir()
                pq.write_table(pa.Table.from_pylist([row]),source/'batch.parquet')
                run(kind,source,root/(kind+'-decoded'),config['abi'],ADDRESS,'uniswap-v2')
            result=pq.read_table(root/'events-decoded/Swap.parquet').to_pylist()[0]
            self.assertEqual(result['arg__amount0In'],'123')
            self.assertEqual(result['arg__amount1Out'],'456')
            result=pq.read_table(root/'state-decoded/getReserves.parquet').to_pylist()[0]
            self.assertEqual(result['arg___reserve0'],str(2**100))
            manifest=json.loads((root/'state-decoded/manifest.json').read_text())
            self.assertEqual(manifest['config']['protocol'],'uniswap-v2')
            with self.assertRaisesRegex(ValueError,'different'):
                run('state',root/'state',root/'state-decoded',config['abi'],ADDRESS,'uniswap-v3')



if __name__ == '__main__':
    unittest.main()
