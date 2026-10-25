import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from reconstruction.core import apply, initial, replay, RAY


def event(name, n=10, log=0, **args):
    return dict(name=name,block_number=n,transaction_index=0,log_index=log,block_hash=f'h{n}',
                **{'arg__'+k:v for k,v in args.items()})


class CoreReplayTests(unittest.TestCase):
    def test_v2_order_empty_blocks_and_exact_checkpoint(self):
        events=[event('Sync',log=2,reserve0=5,reserve1=7),event('Sync',log=1,reserve0=2,reserve1=3)]
        result=list(replay('uniswap-v2',initial('uniswap-v2'),events,10,13,
                           {str(n):f'h{n}' for n in range(10,13)},{12:{'reserve0':5,'reserve1':7}}))
        self.assertEqual([s for _,s in result],[{'reserve0':5,'reserve1':7}]*3)
        with self.assertRaisesRegex(ValueError,'checkpoint mismatch'):
            list(replay('uniswap-v2',initial('uniswap-v2'),events,10,11,{'10':'h10'},
                        {10:{'reserve0':2,'reserve1':3}}))

    def test_v3_liquidity_ranges_and_swap_authoritative_state(self):
        s=initial('uniswap-v3')
        apply('uniswap-v3',s,event('Initialize',sqrtPriceX96=2**96,tick=-1))
        apply('uniswap-v3',s,event('Mint',tickLower=-10,tickUpper=0,amount=100))
        apply('uniswap-v3',s,event('Mint',tickLower=0,tickUpper=10,amount=500))
        apply('uniswap-v3',s,event('Burn',tickLower=-10,tickUpper=0,amount=25))
        self.assertEqual(s['liquidity'],75)
        apply('uniswap-v3',s,event('Swap',sqrtPriceX96=2**96+1,tick=1,liquidity=500))
        self.assertEqual(s,dict(sqrtPriceX96=2**96+1,tick=1,liquidity=500))

    def test_aave_initialization_update_drop_no_empty_block_accrual(self):
        s={}; protocol='aave-v3'; asset='0xabc'
        with self.assertRaisesRegex(ValueError,'without seed'):
            apply(protocol,s,event('ReserveDataUpdated',reserve=asset))
        apply(protocol,s,event('ReserveInitialized',asset=asset))
        self.assertEqual(s[asset]['liquidityIndex'],RAY)
        update=event('ReserveDataUpdated',reserve=asset,liquidityIndex=RAY+7,variableBorrowIndex=RAY+9,
                     liquidityRate=10,variableBorrowRate=20,stableBorrowRate=30)
        apply(protocol,s,update)
        rows=list(replay(protocol,s,[],10,12,{'10':'h10','11':'h11'},{11:s}))
        self.assertEqual(rows[0][1],rows[1][1])
        apply(protocol,s,event('ReserveDropped',asset=asset))
        self.assertEqual(s,{})

    def test_official_v3_tick_boundary_semantics(self):
        for tick, expected in [(-11, 0), (-10, 17), (9, 17), (10, 0)]:
            with self.subTest(tick=tick):
                state = initial('uniswap-v3')
                apply('uniswap-v3', state, event('Initialize', sqrtPriceX96=2**96, tick=tick))
                apply('uniswap-v3', state, event('Mint', tickLower=-10, tickUpper=10, amount='17'))
                self.assertEqual(state['liquidity'], expected)
                apply('uniswap-v3', state, event('Burn', tickLower=-10, tickUpper=10, amount='17'))
                self.assertEqual(state['liquidity'], 0)

    def test_aave_versions_exact_fields_and_independent_reserves(self):
        for protocol in ('aave-v2', 'aave-v3'):
            state = initial(protocol)
            for asset in ('0xABC', '0xDEF'):
                apply(protocol, state, event('ReserveInitialized', asset=asset))
            apply(protocol, state, event('ReserveDataUpdated', reserve='0xABC',
                  liquidityIndex=str(RAY + 123), variableBorrowIndex=str(RAY + 456),
                  liquidityRate='7', variableBorrowRate='8', stableBorrowRate='9'))
            self.assertEqual(state['0xabc'], dict(liquidityIndex=RAY+123,
                variableBorrowIndex=RAY+456, currentLiquidityRate=7,
                currentVariableBorrowRate=8, currentStableBorrowRate=9))
            self.assertEqual(state['0xdef']['liquidityIndex'], RAY)
            apply(protocol, state, event('ReserveDropped', asset='0xABC'))
            self.assertEqual(set(state), {'0xdef'})

    def test_reject_duplicate_wrong_hash_missing_checkpoint(self):
        e=event('Sync',reserve0=1,reserve1=2); s={'reserve0':1,'reserve1':2}
        for events,hashes,checks in [([e,e],{'10':'h10'},{10:s}),([e],{'10':'wrong'},{10:s}),([],{'10':'h10'}, {})]:
            with self.assertRaises(ValueError):
                list(replay('uniswap-v2',s,events,10,11,hashes,checks))




class SerializationTests(unittest.TestCase):
    def test_state_preserves_large_integers_as_decimal_strings(self):
        from reconstruction.direct import decimal_strings
        self.assertEqual(decimal_strings({'a':{'index':2**128-1},'tick':-200001}),
                         {'a':{'index':str(2**128-1)},'tick':'-200001'})


class RoutingTests(unittest.TestCase):
    def test_proxy_implementation_is_not_a_log_source(self):
        from reconstruction.routing import change
        key=b'LENDING_POOL_CONFIGURATOR'.ljust(32,b'\0')
        self.assertIsNone(change('AddressSet',dict(id=key,newAddress='implementation',hasProxy=True),'v2'))
        self.assertEqual(change('ProxyCreated',dict(id=key,newAddress='proxy'),'v2'),'proxy')
        self.assertEqual(change('AddressSet',dict(id=key,newAddress='replacement',hasProxy=False),'v2'),'replacement')
        self.assertIsNone(change('AddressSet',dict(id=b'PRICE_ORACLE'.ljust(32,b'\0'),newAddress='oracle'),'v3'))

if __name__=='__main__': unittest.main()
