"""Compose a new immutable HF assignment from validated predecessor receipts.

The existing runner performs original file/owner/factor/node-receipt checks on
startup. This module only reconciles identity, interval union and ownership.
"""
import copy
import time
from failover_hf import assign_jobs, merge_inventories
from shared.artifacts import digest


def resume_plan(prior, identity, inventories, inventory_hashes, fixed_jobs=None,
                predecessor_manifest_sha256=None, format_change=False):
    keys=['all_users','block_step','cutoff','factor_generation','snapshot_manifest_sha256']
    if format_change:
        if identity.get('format')!='hf-vector-v1' or not 1000<=identity.get('buffer_values',0)<=1000000:
            raise ValueError('Unreviewed physical format change')
    else:keys+=['max_rows_per_job','row_group_rows']
    for key in keys:
        if identity[key] != prior['identity'][key]:
            raise ValueError('Rollout changes protected scope: ' + key)
    jobs = {j['id']: j for j in prior['jobs']}
    roles = [v['role'] for v in inventories]
    if len(roles) != len(set(roles)):
        raise ValueError('Repeated inventory role')
    for inventory in inventories:
        if inventory.get('excluded'):
            raise ValueError('Unresolved inventory exclusion')
        for item in inventory['valid']:
            r = item['receipt']; job = jobs.get(r['job_id'])
            if job is None or prior['assignments'][r['job_id']] != inventory['role']:
                raise ValueError('Receipt not owned in predecessor assignment')
            if any(r.get(k) != v for k, v in [('generation', prior['generation']),
                    ('factor_generation', identity['factor_generation']),
                    ('start', job['start']), ('stop', job['stop']),
                    ('rows', job['expected_rows']), ('all_exact', True), ('mismatches', 0)]):
                raise ValueError('Predecessor receipt identity, scope or validation differs')
            if not item.get('receipt_sha256'):
                raise ValueError('Receipt lacks immutable identity')
    recovered = copy.deepcopy(prior['recovered'])
    current = merge_inventories(inventories)
    if recovered.keys() & current.keys():
        raise ValueError('Predecessor inventories overlap already recovered intervals')
    for key, item in current.items():
        item['owner'] = prior['assignments'][key]
        recovered[key] = item
    pending = [copy.deepcopy(j) for j in prior['jobs'] if j['id'] not in current]
    weights = dict(node=5, revert=9, web=7)
    assignments = assign_jobs(pending, weights)
    for key, role in (fixed_jobs or {}).items():
        if key not in assignments or role not in weights:
            raise ValueError('Canary is not a remaining production interval')
        assignments[key] = role
    covered = copy.deepcopy(prior['coverage'])
    if {j['id'] for j in covered} != set(recovered) | {j['id'] for j in pending}:
        raise ValueError('Rollout loses or invents an interval')
    bounds = {j['id']: (j['start'], j['stop']) for j in prior['jobs']}
    bounds.update({key: (v['receipt']['start'], v['receipt']['stop']) for key, v in prior['recovered'].items()})
    if any((j['start'], j['stop']) != bounds[j['id']] for j in covered):
        raise ValueError('Coverage differs from recorded receipt/job bounds')
    ordered = sorted(covered, key=lambda j: j['start'])
    if ordered[-1]['stop'] != identity['cutoff'] + 1 or any(a['stop'] != b['start'] for a, b in zip(ordered, ordered[1:])):
        raise ValueError('Rollout interval union has gaps or overlaps')
    before = sum(j['expected_rows'] for j in prior['jobs']) + sum(v['receipt']['rows'] for v in prior['recovered'].values())
    after = sum(j['expected_rows'] for j in pending) + sum(v['receipt']['rows'] for v in recovered.values())
    if before != after:
        raise ValueError('Rollout changes logical row population')
    for j in covered:
        j['source'] = 'recovered' if j['id'] in recovered else 'new'
    return dict(identity=copy.deepcopy(identity), generation=digest(identity),
                jobs=pending, assignments=assignments, recovered=recovered,
                coverage=covered, inventory_identities=inventory_hashes,
                created_at=time.time(), predecessor_generation=prior['generation'],
                predecessor_manifest_sha256=predecessor_manifest_sha256,
                planned_rows=after, assignment_weights=weights,
                fixed_canary_jobs=fixed_jobs or {},
                method='Retain original receipts/generations; reassign only unfinished intervals')
