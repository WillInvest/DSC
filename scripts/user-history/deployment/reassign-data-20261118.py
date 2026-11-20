"""User-authorized recovery of Data-only HF work; numerical plan stays pinned."""
import argparse
import copy
import fcntl
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path('/home/fao/hf-flow-control-20261117')
OUT = ROOT / 'reassignment-20261118'
sys.path.insert(0, str(ROOT / 'code'))
from hf_flow_queue import read_snapshot
from shared.artifacts import checksum, digest, write_json


def mutate(db, lost):
    db.execute('begin immediate')
    try:
        for key in lost:
            row = db.execute('select role,node_value,cloud_batch from jobs where job_id=?', (key,)).fetchone()
            assert row == ('data', None, None), key
            assert not db.execute('select 1 from node_acceptance where job_id=?', (key,)).fetchone()
        db.executemany('delete from metrics where job_id=?', [(k,) for k in lost])
        db.executemany('delete from jobs where job_id=?', [(k,) for k in lost])
        db.execute("update operations set state='superseded',report=? where role='data' and state='prepared'",
                   (json.dumps(dict(reason='User authorized independent recomputation; original payload and partial copies preserved', recovery='reassignment-20261118')),))
        db.commit()
    except BaseException:
        db.rollback()
        raise


def prepare():
    OUT.mkdir(exist_ok=True)
    if (OUT / 'prepared.json').exists():
        print('Prepared immutable recovery already exists', flush=True)
        return
    cfg = json.loads((ROOT / 'flow-config.json').read_text())
    plan = json.loads(Path(cfg['numeric_plan']).read_text())
    assert checksum(cfg['numeric_plan']) == cfg['numeric_plan_sha256']
    jobs = {j['id']: j for j in plan['jobs']}
    with read_snapshot(ROOT / 'flow.sqlite') as db:
        known = {x[0] for x in db.execute('select job_id from jobs')}
        node = {x[0] for x in db.execute('select job_id from jobs where node_value is not null')}
        lost = sorted(x[0] for x in db.execute("select job_id from jobs where role='data' and node_value is null"))
        target = (set(jobs) - known) | set(lost)
        assert set(jobs) - target == node
        assert all(k not in plan['sources'] for k in target)
        # Retain exact original observations, metrics and pending-operation payloads.
        with (OUT / 'superseded-data-observations.jsonl').open('x') as stream:
            for key in lost:
                value = dict(job=db.execute('select * from jobs where job_id=?', (key,)).fetchone(),
                             metric=db.execute('select * from metrics where job_id=?', (key,)).fetchone())
                stream.write(json.dumps(value) + '\n')
        write_json(OUT / 'before-data-operations.json', db.execute("select * from operations where role='data' and state='prepared'").fetchall())
        before_metrics = db.execute('select coalesce(sum(rows),0) from metrics').fetchone()[0]
    weights = dict(node=2, revert=4, web=4)
    extra = {r: [] for r in weights}
    load = {r: 0 for r in weights}
    for key in sorted(target, key=lambda k: jobs[k]['start']):
        role = min(weights, key=lambda r: (load[r] / weights[r], r))
        extra[role].append(key)
        load[role] += jobs[key]['expected_rows']
    after = copy.deepcopy(cfg)
    for role, rc in after['roles'].items():
        old = list(rc['jobs'])
        rc['jobs'] = [k for k in old if k not in target] + extra.get(role, [])
        rc['assignment_sha256'] = digest(rc['jobs'])
        rc['expected_role_jobs'] += len(rc['jobs']) - len(old)
        rc['recovery_epoch'] = 'data-recompute-20261118'
        if role != 'data':
            rc['slots'] = 20
            rc['production_enabled'] = True
            rc['canary_limit'] = None
        write_json(OUT / ('role-' + role + '.json'), rc)
    assigned = [k for rc in after['roles'].values() for k in rc['jobs']]
    assert len(assigned) == len(set(assigned))
    assert set(target) <= set(assigned)
    assert all(k in jobs for k in assigned)
    after['data_recompute_authorization'] = dict(epoch='data-recompute-20261118', at=time.time(),
        reason='User explicitly requested replacing unavailable Data work on the three online hosts',
        superseded_observations=str(OUT / 'superseded-data-observations.jsonl'),
        rejoin='Do not attach Data workers or accept its old reassigned outputs; preserve and reconcile by job ID')
    write_json(OUT / 'before-flow-config.json', cfg)
    write_json(OUT / 'after-flow-config.json', after)
    # Trial the exact catalog transition on a consistent SQLite backup.
    trial = OUT / 'trial.sqlite'
    with sqlite3.connect('file:' + str(ROOT / 'flow.sqlite') + '?mode=ro', uri=True) as src, sqlite3.connect(trial) as dst:
        src.backup(dst)
        mutate(dst, lost)
        assert dst.execute('select count(*) from jobs').fetchone()[0] == len(node)
        assert dst.execute('select count(*) from jobs where node_value is not null').fetchone()[0] == len(node)
        remaining = dst.execute('select coalesce(sum(rows),0) from metrics').fetchone()[0]
        assert remaining + sum(jobs[k]['expected_rows'] for k in target) == 910310934885
        assert dst.execute('pragma quick_check').fetchone() == ('ok',)
    report = dict(lost=lost, targets=sorted(target), extra=extra, extra_values=load,
                  jobs=len(target), values=sum(jobs[k]['expected_rows'] for k in target),
                  retained_node_jobs=len(node), retained_values=remaining, before_values=before_metrics,
                  before_config_sha256=checksum(ROOT / 'flow-config.json'),
                  numeric_plan_sha256=cfg['numeric_plan_sha256'], generation=cfg['generation'],
                  trial_sqlite_exact=True, prepared_at=time.time())
    write_json(OUT / 'prepared.json', report)
    print(json.dumps({k: report[k] for k in ['jobs', 'values', 'retained_node_jobs', 'retained_values', 'extra_values', 'trial_sqlite_exact']}), flush=True)


def apply():
    if (OUT / 'applied.json').exists():
        print('Already applied; do not repeat catalog transition', flush=True)
        return
    p = json.loads((OUT / 'prepared.json').read_text())
    assert checksum(ROOT / 'flow-config.json') == p['before_config_sha256']
    # Controller must be stopped; never compete with its acceptance transaction.
    with (ROOT / 'controller.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with sqlite3.connect(ROOT / 'flow.sqlite') as db:
            current = {k for (k,) in db.execute("select job_id from jobs where role='data' and node_value is null")}
            assert current == set(p['lost'])
            assert db.execute('select count(*) from jobs where node_value is not null').fetchone()[0] == p['retained_node_jobs']
            mutate(db, p['lost'])
            assert db.execute('select count(*) from jobs').fetchone()[0] == p['retained_node_jobs']
        cfg = json.loads((OUT / 'after-flow-config.json').read_text())
        write_json(ROOT / 'flow-config.json', cfg)
        write_json(OUT / 'applied.json', dict(at=time.time(), generation=p['generation'],
                    before_config_sha256=p['before_config_sha256'], after_config_sha256=checksum(ROOT / 'flow-config.json'),
                    replaced_unavailable_observations=len(p['lost']), jobs=p['jobs'], values=p['values']))
    print('Catalog recovery applied; existing node/cloud acceptance preserved', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'apply'])
    args = parser.parse_args()
    (prepare if args.mode == 'prepare' else apply)()
