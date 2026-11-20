"""Real zero-address transfer regression and checkpoint hydration checks."""
import importlib.util
import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ZERO = '0x' + '0' * 40
OWNER = '0xf152581b8cb486b24d73ad51e23a3fd3e0222538'
OTHER = '0x' + '1' * 40
TOKEN = '0x4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8'
EVENT = dict(name='BalanceTransfer', block_number=24944649,
             transaction_index=207, log_index=597, schema_version=4,
             arg__from=OWNER, arg__to=ZERO, arg__value='26677',
             arg__index='1063636361641570013304473258')


class WorkerTransferTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.runtime = Path(cls.temp.name)
        cls.runtime.joinpath('math_worker.mjs').write_bytes(HERE.joinpath('math_worker.mjs').read_bytes())
        repo = HERE.parent.parent
        kernel = HERE / 'math-kernel.json'
        if not kernel.exists():
            kernel = repo / 'artifacts/aave-four-asset-positions-2026-11-13/math-kernel.json'
        cls.runtime.joinpath('math-kernel.json').write_bytes(kernel.read_bytes())
        modules = HERE / 'node_modules'
        if not modules.exists():
            modules = repo / 'artifacts/aave-oracle-event-replay-svb-2026-11-13/node_modules'
        cls.runtime.joinpath('node_modules').symlink_to(modules, target_is_directory=True)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def call(self, initial, events):
        result = subprocess.run(['node', str(self.runtime / 'math_worker.mjs')],
                                input=json.dumps(dict(initial=initial, events=events,
                                                     modern_versions=[4], debt=False)) + '\n',
                                capture_output=True, text=True, check=True, timeout=30)
        return json.loads(result.stdout)

    def test_real_transfer_credits_zero_address_instead_of_burning(self):
        result = self.call({OWNER: '26677', ZERO: '0'}, [EVENT])
        self.assertNotIn('error', result)
        self.assertEqual(result['balances'], {OWNER: '0', ZERO: '26677'})
        self.assertEqual(sum(int(v) for v in result['balances'].values()), 26677)

    def test_existing_zero_balance_is_accumulated(self):
        result = self.call({OWNER: '26677', ZERO: '100'}, [EVENT])
        self.assertNotIn('error', result)
        self.assertEqual(result['balances'][ZERO], '26777')

    def test_ordinary_transfer_self_transfer_and_negative_guard(self):
        ordinary = dict(EVENT, arg__to=OTHER)
        result = self.call({OWNER: '26677'}, [ordinary, dict(ordinary, arg__from=OTHER)])
        self.assertNotIn('error', result)
        self.assertEqual(result['balances'], {OWNER: '0', OTHER: '26677'})
        self.assertEqual(self.call({}, [ordinary])['error'], 'Negative scaled position')


@unittest.skipUnless(importlib.util.find_spec('pyarrow'), 'PyArrow required; run on dsc-data runtime')
class ReplayCheckpointTest(unittest.TestCase):
    def test_zero_address_balance_survives_next_input_chunk(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import replay_positions as replay
        from shared.artifacts import checksum
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'data'
            decoded = output / 'decoded-staging' / TOKEN
            (decoded / 'receipts').mkdir(parents=True)
            (root / 'receipts').mkdir()
            (root / 'token-profiles').mkdir()
            (root / 'token-profiles/abi.json').write_text(json.dumps([
                dict(type='function', name='mint', inputs=[dict(name='scaledAmount')])]))
            (root / 'token-profiles' / (TOKEN + '.profiles.json')).write_text(json.dumps(dict(
                profiles=[dict(abi='abi.json', version=4, block=1, log_index=-1)])))
            files = {}
            for index, event in enumerate([EVENT, dict(EVENT, block_number=24946792,
                                                       arg__from=OTHER, arg__value='5')]):
                name = 'chunk' + str(index) + '.parquet'
                target = decoded / 'BalanceTransfer' / name
                target.parent.mkdir(exist_ok=True)
                pq.write_table(pa.Table.from_pylist([event]), target)
                (decoded / 'receipts' / (name + '.json')).write_text(json.dumps(dict(outputs=[
                    dict(path='BalanceTransfer/' + name, sha256=checksum(target))])))
                files[name] = dict(start=24936792 + index * 10000, stop=24946792 + index * 10000)
            (root / 'receipts' / (TOKEN + '.json')).write_text(json.dumps(dict(files=files)))
            db = sqlite3.connect(root / 'scaled-positions.sqlite')
            db.execute('create table positions(token text,asset text,kind text,owner text,scaled text,last_block integer,primary key(token,owner))')
            db.executemany('insert into positions values(?,?,?,?,?,?)', [
                (TOKEN, 'WETH', 'aToken', OWNER, '26677', 24936791),
                (TOKEN, 'WETH', 'aToken', OTHER, '100', 24936791)])
            db.commit()
            job = dict(address=TOKEN, asset='WETH', kind='aToken', start_block=24936792)
            result = replay.replay_job(dict(output=str(output)), job, root)
            self.assertEqual(result['state'], 'scaled_replay_complete')
            self.assertEqual(db.execute('select scaled from positions where token=? and owner=?',
                                        (TOKEN, ZERO)).fetchone(), ('26682',))
            db.close()


if __name__ == '__main__':
    unittest.main()
