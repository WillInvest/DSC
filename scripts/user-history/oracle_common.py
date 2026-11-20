"""Shared ABI and interval helpers for end-of-block oracle collection."""
from pathlib import Path

POOL = '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2'
START = 16291127


def function(name, inputs, outputs):
    return dict(type='function', name=name, stateMutability='view',
                inputs=[dict(name='arg'+str(i), type=t) for i,t in enumerate(inputs)],
                outputs=[dict(name='', type=t) for t in outputs])


ORACLE_ABI = [function('getAssetsPrices',['address[]'],['uint256[]']),
              function('getAssetPrice',['address'],['uint256']),
              function('BASE_CURRENCY_UNIT',[],['uint256'])]
POOL_ABI = [function('getReservesList',[],['address[]']),
            function('ADDRESSES_PROVIDER',[],['address'])]
PROVIDER_ABI = [function('getPriceOracle',[],['address'])]


def intervals(snapshots, stop, extra_boundaries=()):
    """Split only at reviewed state changes and explicit reusable boundaries."""
    states = {s['block']:s for s in snapshots}
    points = sorted(set(states) | {b for b in extra_boundaries if min(states)<b<stop})
    result=[]
    for i,start in enumerate(points):
        state=states[max(b for b in states if b<=start)]
        result.append(dict(start=start,stop=points[i+1] if i+1<len(points) else stop,
                           assets=state['assets'],oracle=state['oracle'],unit=state['unit']))
    if any(j['start']>=j['stop'] for j in result):raise ValueError('Empty/reversed interval')
    return result


def raw_files(directory):
    # This is a private benchmark/backfill staging directory, never a catalog.
    return sorted(Path(directory).glob('*.parquet'))
