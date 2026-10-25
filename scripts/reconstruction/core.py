"""Official-subgraph-derived core projection over already decoded events.

Integers stay integers. No normalized Aave interest views, user positions or
Uniswap fee/oracle accounting are inferred by these reducers.
"""
from copy import deepcopy

RAY = 10**27
AAVE_FIELDS = {
    'liquidityIndex': 'liquidityIndex',
    'variableBorrowIndex': 'variableBorrowIndex',
    'currentLiquidityRate': 'liquidityRate',
    'currentVariableBorrowRate': 'variableBorrowRate',
    'currentStableBorrowRate': 'stableBorrowRate',
}
FIELDS = {
    'uniswap-v2': ('reserve0', 'reserve1'),
    'uniswap-v3': ('sqrtPriceX96', 'tick', 'liquidity'),
    'aave-v2': tuple(AAVE_FIELDS),
    'aave-v3': tuple(AAVE_FIELDS),
}


def initial(protocol):
    """Creation seed only; arbitrary starts require an end-of-block node seed."""
    if protocol.startswith('aave-'):
        return {}
    return dict.fromkeys(FIELDS[protocol], 0)


def handle_sync(state, args):
    """Uniswap V2 handleSync: keep base-unit integers, not analytics decimals."""
    state.update(reserve0=int(args['reserve0']), reserve1=int(args['reserve1']))


def handle_initialize(state, args):
    """Uniswap V3 handleInitialize's price/tick projection."""
    if state['sqrtPriceX96'] != 0:
        raise ValueError('duplicate V3 initialization')
    state.update(sqrtPriceX96=int(args['sqrtPriceX96']), tick=int(args['tick']))


def handle_swap(state, args):
    """Uniswap V3 handleSwap: emitted values replace the stored core state."""
    state.update({key: int(args[key]) for key in FIELDS['uniswap-v3']})


def handle_liquidity(state, args, direction):
    """V3 handleMint/handleBurn: lower tick inclusive, upper tick exclusive."""
    if state['sqrtPriceX96'] == 0:
        raise ValueError('V3 liquidity before initialization')
    if int(args['tickLower']) <= state['tick'] < int(args['tickUpper']):
        state['liquidity'] += direction * int(args['amount'])


def handle_reserve_initialized(state, args):
    # On-chain initialization, not the subgraph's lazy zero placeholders.
    asset = args['asset'].lower()
    if asset in state:
        raise ValueError('duplicate reserve initialization')
    state[asset] = {key: RAY if key.endswith('Index') else 0 for key in AAVE_FIELDS}


def handle_reserve_dropped(state, args):
    # Our projection contains active reserves only; subgraph retains isDropped.
    asset = args['asset'].lower()
    if asset not in state:
        raise ValueError('dropped reserve missing from seed')
    del state[asset]


def handle_reserve_data_updated(state, args):
    """Aave V2/V3 handleReserveDataUpdated, restricted to stored core fields."""
    asset = args.get('reserve', args.get('asset')).lower()
    if asset not in state:
        raise ValueError('reserve update without seed or initialization')
    state[asset] = {field: int(args[source]) for field, source in AAVE_FIELDS.items()}


def handle_mint(state, args):
    handle_liquidity(state, args, 1)


def handle_burn(state, args):
    handle_liquidity(state, args, -1)


AAVE_HANDLERS = {
    'ReserveInitialized': handle_reserve_initialized,
    'ReserveDropped': handle_reserve_dropped,
    'ReserveDataUpdated': handle_reserve_data_updated,
}
HANDLERS = {
    'uniswap-v2': {'Sync': handle_sync},
    'uniswap-v3': {'Initialize': handle_initialize, 'Swap': handle_swap,
                   'Mint': handle_mint, 'Burn': handle_burn},
    'aave-v2': AAVE_HANDLERS,
    'aave-v3': AAVE_HANDLERS,
}


def apply(protocol, state, event):
    """Dispatch decoded events to the official-subgraph-derived core projection.

See subgraph-sources.json for immutable upstream revisions and README.md for
intentional differences. Other decoded events do not modify this projection.
"""
    if protocol not in HANDLERS:
        raise ValueError('unsupported reconstruction protocol')
    handler = HANDLERS[protocol].get(event['name'])
    if handler is not None:
        args = {key.removeprefix('arg__'): value for key, value in event.items()
                if key.startswith('arg__')}
        handler(state, args)
    if protocol == 'uniswap-v3' and not 0 <= state['liquidity'] < 2**128:
        raise ValueError('invalid V3 active liquidity')


def replay(protocol, seed, events, start, stop, hashes, checkpoints):
    """Yield every end-of-block state, rejecting duplicates and failed checkpoints.

Seed describes start-1. Checkpoints map block numbers to projected node state.
Consumers must exhaust this iterator and durably validate it before acceptance.
"""
    if protocol not in FIELDS or start >= stop:
        raise ValueError('invalid replay scope')
    if set(hashes) != {str(n) for n in range(start, stop)}:
        raise ValueError('incomplete replay headers')
    if stop - 1 not in checkpoints:
        raise ValueError('missing final checkpoint')
    ordered = sorted(events, key=lambda e: (e['block_number'], e['transaction_index'], e['log_index']))
    seen = set()
    for event in ordered:
        n = event['block_number']
        if not start <= n < stop or event['block_hash'] != hashes[str(n)]:
            raise ValueError('event outside canonical interval')
        key = (n, event['log_index'])
        if key in seen:
            raise ValueError('duplicate event log')
        seen.add(key)
    state = deepcopy(seed)
    index = 0
    for n in range(start, stop):
        while index < len(ordered) and ordered[index]['block_number'] == n:
            apply(protocol, state, ordered[index])
            index += 1
        if n in checkpoints and state != checkpoints[n]:
            raise ValueError(f'core checkpoint mismatch at block {n}: replay={state}, node={checkpoints[n]}')
        yield n, deepcopy(state)
