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

    def test_state_launchers_supply_correct_getters(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            mock=root/'cryo'
            mock.write_text('#!'+sys.executable+'\n'+"""import sys,json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
args=sys.argv[1:]
selectors=args[args.index('--call-data')+1:args.index('--blocks')]
out=Path(args[args.index('--output-dir')+1]); out.mkdir()
rows=[dict(chain_id=1,block_number=100,contract_address=args[args.index('--contract')+1],call_data=s,output_data='0x'+'00'*({'0x0902f1ac':96,'0x3850c7bd':224,'0x1ad8b03b':64}.get(s,32))) for s in selectors]
pq.write_table(pa.Table.from_pylist(rows),out/'batch.parquet')
(out/'args.json').write_text(json.dumps(args))
""")
            mock.chmod(0o755)
            for protocol in ('uniswap-v2','uniswap-v3'):
                out=root/protocol
                env={**os.environ,'CRYO_BIN':str(mock),'PYTHON_BIN':sys.executable,'OUTPUT_DIR':str(out)}
                subprocess.run(['bash',str(ROOT/'scripts/extraction/extract_pool_state.sh'),protocol,ADDRESS,'100'],env=env,check=True,capture_output=True)
                args=json.loads((out/'args.json').read_text())
                for line in load_protocol(protocol)['catalog'].read_text().splitlines():
                    self.assertIn(line.split('\t')[1],args)
                self.assertIn(protocol+'_'+ADDRESS+'_state_v1',args)

    def test_launchers_require_explicit_protocol_address(self):
        for script in ('extract_pool_events.sh','extract_pool_state.sh'):
            for args in (['13000000:13000010'],['uniswap-v1',ADDRESS,'1:2'],['uniswap-v2','invalid','1:2']):
                result=subprocess.run(['bash',str(ROOT/'scripts/extraction'/script),*args],capture_output=True)
                self.assertNotEqual(result.returncode,0)


if __name__ == '__main__':
    unittest.main()
