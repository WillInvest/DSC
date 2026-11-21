from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from hf_incremental_boundary import read_boundary, seal_record
from hf_incremental_window import pin_window
from hf_incremental_oracle import oracle_plan
from hf_incremental_pool import pool_files
from oracle_common import POOL


def block_hash(height):
    return f'0x{height:064x}'


class IncrementalHFTests(unittest.TestCase):
    def test_checkpoint_is_sealed_to_previous_block_and_generation(self):
        evaluator = SimpleNamespace(block=100, block_hash=block_hash(100), reserves={'a': {'id': 1}})
        replay = SimpleNamespace(state_of=lambda _: {'positions': {'wallet': {'token': '23'}},
                                                     'emodes': {'wallet': 1}})
        with TemporaryDirectory() as temporary, patch.dict(sys.modules, hf_replay_checkpoint=replay):
            path = Path(temporary)/'state.json.gz'
            result = seal_record(evaluator,'accepted-release',100,block_hash(100),{},path)
            again = seal_record(evaluator,'accepted-release',100,block_hash(100),{},path)
            self.assertEqual(result, again)
            record = read_boundary(path,result['sha256'],101,block_hash(100),'accepted-release')
            self.assertEqual(record['state']['positions']['wallet']['token'],'23')
            self.assertEqual(record['reserves'],evaluator.reserves)
            with self.assertRaisesRegex(ValueError,'does not join'):
                read_boundary(path,result['sha256'],102,block_hash(101),'accepted-release')

    def test_window_matches_finalized_lineage_and_rejects_a_broken_parent(self):
        def rpc(method, params):
            if method == 'eth_chainId':
                return '0x1'
            height = 103 if params[0] == 'finalized' else int(params[0],16)
            return dict(number=hex(height),hash=block_hash(height),
                        parentHash=block_hash(height-1),timestamp=hex(height+1000))

        options = dict(generation='accepted-release',checkpoint_sha256='accepted-checkpoint')
        plan = pin_window(rpc,101,104,block_hash(100),**options)
        self.assertEqual([r['block'] for r in plan['headers']], [101,102,103])
        self.assertEqual(plan['identity']['end_hash'], block_hash(103))
        with self.assertRaisesRegex(ValueError,'not finalized'):
            pin_window(rpc,101,105,block_hash(100),**options)

        def broken(method, params):
            result = rpc(method, params)
            if params and params[0] == hex(102):
                result['parentHash'] = block_hash(100)
            return result

        with self.assertRaisesRegex(ValueError,'lineage breaks'):
            pin_window(broken,101,104,block_hash(100),**options)

    def test_oracle_jobs_split_at_a_real_routing_change_and_pin_each_header(self):
        def rpc(method, params):
            if method == 'eth_chainId':
                return '0x1'
            height = 104 if params[0] == 'finalized' else int(params[0],16)
            return dict(number=hex(height),hash=block_hash(height),
                        parentHash=block_hash(height-1),timestamp=hex(height+1000))
        window = pin_window(rpc,101,105,block_hash(100),generation='parent',checkpoint_sha256='state')
        def route_at(block):
            return dict(oracle='first' if block<103 else 'second',provider='provider',
                        assets=['asset'],unit=100_000_000,block_hash=block_hash(block))
        plan = oracle_plan(window,route_at,Path('/output'),100_000_000)
        self.assertEqual([(job['start'],job['stop'],job['oracle']) for job in plan['jobs']],
                         [(101,103,'first'),(103,105,'second')])
        self.assertEqual(len(plan['routes']),4)
        with self.assertRaisesRegex(ValueError,'requires review'):
            oracle_plan(window,route_at,Path('/output'),1)

    def test_pool_input_selects_a_contiguous_cropped_catalog_range(self):
        stream = 'encoded/aave-v3/'+POOL.lower()+'/schema'
        first, second = [dict(stream=stream,start=a,stop=b)
                         for a,b in [(100,103),(103,110)]]
        self.assertEqual(pool_files([second,first],101,105), [first,second])
        with self.assertRaisesRegex(ValueError,'does not cover'):
            pool_files([first],101,105)
        with self.assertRaisesRegex(ValueError,'gap or overlap'):
            pool_files([first,{**second,'start':102}],101,105)
