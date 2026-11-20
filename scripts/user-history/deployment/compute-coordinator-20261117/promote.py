"""One-time operational promotion; existing flow APIs retain all numerical rules."""
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import sys
import time

R = pathlib.Path('/home/fao/hf-flow-control-20261117')
B = R / 'bootstrap'
SCHEDULER = 'tls://10.246.103.143:18896'


def sha(p):
    h = hashlib.sha256()
    with pathlib.Path(p).open('rb') as f:
        for chunk in iter(lambda: f.read(2**20), b''):
            h.update(chunk)
    return h.hexdigest()


def write(p, value):
    p = pathlib.Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    t = p.with_suffix(p.suffix + '.new')
    with t.open('w') as f:
        json.dump(value, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    t.replace(p)


def prepare():
    manifest = json.loads((B / 'manifest.json').read_text())
    for n, v in manifest.items():
        assert (B/n).stat().st_size == v['bytes'] and sha(B/n) == v['sha256'], n
    if (R/'flow.sqlite').exists():
        raise RuntimeError('Promotion already prepared; do not overwrite its live ledger')
    cfg = json.loads((B/'flow-config.json').read_text())
    source = sqlite3.connect(f'file:{B}/flow.sqlite?mode=ro', uri=True)
    assert source.execute('pragma quick_check').fetchone()[0] == 'ok'
    target = sqlite3.connect(R/'flow.sqlite')
    source.backup(target)
    target.close()
    source.close()
    shutil.copytree(B/'code', R/'code', dirs_exist_ok=True)
    shutil.copy2(B/'numeric-plan.json', R/'numeric-plan.json')
    shutil.copytree(B/'audits', R/'audits', dirs_exist_ok=True)
    for n in ['pilot-completed.json', 'packed-pilot-completed.json']:
        shutil.copy2(B/n, R/n)
    controller = R/'code/hf_flow_control.py'
    code = controller.read_text()
    assert code.count("coordinator_role='data'") == 1
    controller.write_text(code.replace("coordinator_role='data'", "coordinator_role=cfg.get('coordinator_role','data')"))
    cfg.update(control_root=str(R), code_root=str(R/'code'),
               numeric_plan=str(R/'numeric-plan.json'),
               private='/home/dsc-data/worker-state/private',
               old_base=str(R/'metadata'), scheduler=SCHEDULER,
               coordinator_role='compute', authority_epoch='compute-20261117',
               backup_node_ssh='node@10.246.103.160')
    for role in ['node', 'revert', 'web']:
        path = R/'roles'/(role+'.json')
        live = json.loads(path.read_text())
        assert live['generation'] == cfg['generation']
        assert live['jobs'] == cfg['roles'][role]['jobs'], role
        cfg['roles'][role] = live
        cfg['roles'][role]['scheduler'] = SCHEDULER
    cfg['node_bulk'] = cfg['roles']['node']['node_bulk']
    for role in cfg['roles']:
        cfg['roles'][role]['scheduler'] = SCHEDULER
    write(R/'flow-config.json', cfg)
    write(R/'authority-epoch.json', dict(
        epoch=cfg['authority_epoch'], coordinator='dsc-compute', generation=cfg['generation'],
        numeric_plan_sha256=cfg['numeric_plan_sha256'], created_at=time.time(),
        recovered_backup='node/control-backups/1794924560924572584',
        fencing='Reachable flow IO/cloud workers move to compute scheduler; Data gets no workers. Data assignment unchanged.',
        rejoin='Do not auto-switch back or overwrite either ledger; reconcile Data after return.'))
    print('prepared compute authority without changing numerical generation or assignments', flush=True)


def reconcile():
    sys.path[:0] = [str(R/'code'), '/home/dsc-data/scripts', '/home/dsc-data/scripts/user-history']
    from hf_flow_queue import FlowLedger
    from hf_flow_control import validate_value
    from shared.artifacts import digest
    cfg = json.loads((R/'flow-config.json').read_text())
    plan = json.loads((R/'numeric-plan.json').read_text())
    jobs = {j['id']: j for j in plan['jobs']}
    imported = []
    expected = {
        '87631df9d7c40ebdbb18efdf': '1b6074d54be1294972e5d2d5304b32190f0b9efe09e6c8233fcdbe3b577bbc0e',
        '4abe26a99153a4069281beca': '91685cab5430d9a4df3af3986095d5a830554fb9e5e814d1fdad8429d19b5f91',
        '1123a3670a90e9df96c88bc4': '31e5ce82790a0f83a77ae33a7b63189ef443ffed35155dded436da8af902682c',
    }
    with FlowLedger(R/'flow.sqlite', cfg['generation']) as db:
        for batch, expected_sha in expected.items():
            p = R/'transfers'/batch/'verified.json'
            if expected_sha is not None:
                assert sha(p) == expected_sha, batch
            report = json.loads(p.read_text())
            assert report['all_exact'] is True and report['id'] == batch
            role = report['role']
            keys = [v['job_id'] for v in report['jobs']]
            assert len(keys) == len(set(keys))
            pending = next((o for o in db.pending_operations() if o['id'] == batch), None)
            if pending:
                assert set(pending['payload']['keys']) == set(keys)
            else:
                db.prepare_operation(batch, 'transfer', role, {'keys': keys})
            for value in report['jobs']:
                key = value['job_id']
                assert key in cfg['roles'][role]['jobs']
                validate_value(value, jobs[key], cfg['generation'], plan['sources'].get(key))
                db.node_commit(key, value, role=role)
            retired = json.loads((R/'retirements'/(batch+'.json')).read_text())
            assert retired['source_copies_retired'] is True and retired['node_copies_verified'] is True
            db.finish_operation(batch, dict(recovered_node_proof_sha256=sha(p), source_retirement=retired))
            imported.append(dict(batch=batch, jobs=len(keys), node_proof_sha256=sha(p)))
        write(R/'reconciled-transfers.json', dict(imported=imported, generation=cfg['generation'], at=time.time()))
        print(json.dumps(dict(imported=imported, totals=db.stage_totals())), flush=True)


def catch_up():
    sys.path[:0] = [str(R/'code'), '/home/dsc-data/scripts', '/home/dsc-data/scripts/user-history']
    from distributed import Client
    from vector_coordinator import security
    from hf_flow_queue import FlowLedger
    from hf_flow_io import read_catalog
    from hf_flow_control import validate_value
    cfg = json.loads((R/'flow-config.json').read_text())
    plan = json.loads((R/'numeric-plan.json').read_text())
    jobs = {j['id']: j for j in plan['jobs']}
    with Client(cfg['scheduler'], security=security(cfg['private']), set_as_default=False) as client, FlowLedger(R/'flow.sqlite', cfg['generation']) as db:
        info = client.scheduler_info()['workers']
        stats = {}
        for role in ['node', 'revert', 'web']:
            worker = [a for a,v in info.items() if v['name']=='flow-io-'+role]
            assert len(worker) == 1, role
            row = db.db.execute('select sequence from cursors where role=?',(role,)).fetchone()
            cursor = row[0] if row else 0
            count = 0
            while True:
                page = next(iter(client.run(read_catalog,cfg['roles'][role],cursor,workers=worker).values()))
                if not page['entries']:
                    break
                for item in page['entries']:
                    key = item['job_id']; value = item['value']
                    assert key in cfg['roles'][role]['jobs']
                    validate_value(value,jobs[key],cfg['generation'],plan['sources'].get(key))
                    prior = db.db.execute('select role,local_value,node_value from jobs where job_id=?',(key,)).fetchone()
                    if prior and prior[1] is None:
                        assert prior[0] == role
                        node = json.loads(prior[2])
                        assert node['receipt'].get('origin_receipt_sha256',node['sha256']) == value['sha256']
                        with db.transaction():
                            db.db.execute('update jobs set local_value=? where job_id=? and local_value is null',(json.dumps(value,sort_keys=True),key))
                    db.observe(key,role,value)
                    if role == 'node':
                        db.node_commit(key,value)
                    count += 1
                cursor = page['sequence']
                with db.transaction():
                    db.db.execute('insert into cursors values(?,?) on conflict(role) do update set sequence=excluded.sequence',(role,cursor))
            stats[role] = dict(new_catalog_entries=count,sequence=cursor,producer=page['progress']['state'])
            print(json.dumps(dict(catalog_catch_up=role,**stats[role])),flush=True)
        cfg['archive_fill_transfers'] = True
        write(R/'flow-config.json',cfg)
        write(R/'catalog-catch-up.json',dict(at=time.time(),roles=stats,totals=db.stage_totals(),generation=cfg['generation']))


if __name__ == '__main__':
    {'prepare': prepare, 'reconcile': reconcile, 'catch_up': catch_up}[sys.argv[1]]()
