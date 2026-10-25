import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from reconstruction.direct import replay_table
from reconstruction.core import initial,RAY


def event(name,n,log=0,**args):
    return dict(name=name,block_number=n,transaction_index=0,log_index=log,**{'arg__'+k:v for k,v in args.items()})


class DirectReplayTests(unittest.TestCase):
    def test_forward_fill_and_last_event(self):
        events=[event('Sync',12,1,reserve0='5',reserve1='7'),event('Sync',12,0,reserve0='2',reserve1='3')]
        table,state=replay_table('uniswap-v2','a',initial('uniswap-v2'),events,10,15)
        rows=table.to_pylist()
        self.assertEqual([r['block_number'] for r in rows],list(range(10,15)))
        self.assertEqual([json.loads(r['state_json'])['reserve0'] for r in rows],['0','0','5','5','5'])
        self.assertEqual(state,{'reserve0':5,'reserve1':7})

    def test_end_of_block_bootstrap_not_double_applied(self):
        seed={'a':dict(liquidityIndex=RAY,variableBorrowIndex=RAY,currentLiquidityRate=0,currentVariableBorrowRate=0,currentStableBorrowRate=0)}
        events=[event('ReserveInitialized',10,asset='a')]
        table,state=replay_table('aave-v3','pool',{},events,10,12,{'10':seed})
        self.assertEqual(state,seed)
        self.assertEqual(table.num_rows,2)

    def test_chunk_resume_equals_one_pass(self):
        events=[event('Sync',10,reserve0=2**112-1,reserve1=8),event('Sync',13,reserve0=9,reserve1=10)]
        full,_=replay_table('uniswap-v2','a',initial('uniswap-v2'),events,10,15)
        first,seed=replay_table('uniswap-v2','a',initial('uniswap-v2'),events[:1],10,12)
        second,_=replay_table('uniswap-v2','a',seed,events[1:],12,15)
        self.assertEqual(full.to_pylist(),first.to_pylist()+second.to_pylist())

    def test_unknown_aave_reserve_still_fails(self):
        with self.assertRaisesRegex(ValueError,'without seed'):
            replay_table('aave-v3','p',{},[event('ReserveDataUpdated',10,reserve='a')],10,11)

if __name__=='__main__': unittest.main()
