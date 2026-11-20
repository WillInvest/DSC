"""Rebuild one end-of-block account and balances from the frozen local inputs.

Run with the documented Linux replay runtime mounted at /home/dsc-data.
The calculation delegates to the existing version-aware BatchEvaluator.
"""
import argparse
import collections
import json
import sqlite3
import threading
from pathlib import Path
from unittest.mock import patch


def rebuild(user, block, verify_node=False):
    from native_batch import BatchEvaluator
    from accounting_inputs import read, profiles, profile_at
    root = Path('/home/dsc-data/runs/aave-user-history-v3/26407881')
    plan = read(root/'plan.json')
    if not 16291127 <= block <= plan['end_block']:
        raise ValueError('Block outside the frozen research scope')
    db = sqlite3.connect('file:'+str(root/'accounting.sqlite')+'?mode=ro&immutable=1', uri=True)
    evaluator = None
    try:
        evaluator = BatchEvaluator(root, db, plan, block, block+1)
        evaluator.advance(block)
        result = evaluator.account(user.lower(), include_balances=True)
        result['price_base_unit'] = next(j['unit'] for j in evaluator.oracle['jobs'] if j['start'] <= block < j['stop'])
        result['asset_prices'] = dict(zip(evaluator.assets, map(str, evaluator.asset_prices)))
        result['interest_indices'] = {a:{k:str(v) for k,v in values.items()}
                                      for a,values in evaluator.normalized.items()}
        result['reserve_configuration'] = {a:row['configuration'] for a,row in evaluator.templates.items()}
        result['enabled_collateral_assets'] = sorted(evaluator.collateral.get(user.lower(), set()))
        result['analysis_debt_excluded'] = int(result['account'][1]) < 1_000_000
        result['account_fields'] = ['totalCollateralBase', 'totalDebtBase', 'availableBorrowsBase',
                                    'currentLiquidationThreshold', 'ltv', 'healthFactor']
        if verify_node:
            from web3 import HTTPProvider
            import failover_hf
            profile = profile_at(profiles(), block)
            tasks = [dict(kind='call', function='getUserAccountData', args=[user],
                          block=block, profile=profile['key'])]
            tasks += [dict(kind='token_call', token=b['token'], owner=user,
                           block=block, profile=profile['key']) for b in result['balances']]
            counts = collections.Counter()
            lock = threading.Lock()
            original = HTTPProvider.make_request

            def counted(self, method, params):
                with lock:
                    if sum(counts.values()) >= 200:
                        raise ValueError('Reconstruction validation RPC limit exceeded')
                    counts[str(method)] += 1
                return original(self, method, params)

            with patch.object(HTTPProvider, 'make_request', counted):
                refs = failover_hf.direct_node(plan, dict(operation='snapshots',
                    abis={profile['key']:profile['abi']}, tasks=tasks))
            account_exact = [str(v) for v in refs[0]['result']] == result['account']
            balances_exact = all(str(ref['result']) == balance['balance']
                                 for ref,balance in zip(refs[1:], result['balances']))
            block_exact = all(ref['block_hash'].lower() == result['block_hash'].lower() for ref in refs)
            result['node_validation'] = dict(account_exact=account_exact,
                nominal_balances_exact=balances_exact, block_identity_exact=block_exact,
                checked_token_balances=len(result['balances']), provider_methods=dict(counts),
                rpc_calls=sum(counts.values()))
            if not (account_exact and balances_exact and block_exact):
                raise ValueError('Reconstructed account or token balance differs from own node')
        return result
    finally:
        if evaluator is not None:
            evaluator.close()
        db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', required=True)
    parser.add_argument('--block', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verify-node', action='store_true')
    args = parser.parse_args()
    value = rebuild(args.user, args.block, args.verify_node)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(output=str(args.output), user=value['user'], block=value['block'],
                         node_validation=value.get('node_validation'))))
