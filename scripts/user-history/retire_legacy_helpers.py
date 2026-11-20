"""Bounded legacy-source retirement using the existing independent proof APIs."""
import argparse
import fcntl
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from distributed import Client
from vector_coordinator import security
from shared.artifacts import write_json

SOURCE_ARCHIVE = '/home/node/hf-centralized/20261118-archive'
SOURCE_INVENTORY = '/home/node/hf-centralized/20261116/source-inventory-20261118.json'
HOMES = dict(web='/home/fao', revert='/home/dsc-revert')
HELPER = '/usr/local/libexec/dsc-hf-consolidate-20261116/consolidate_hf.py'
HELPER_SHA = '19d96f155607942d964c661a6393edf637dd3c817ef5fcea46c16c7865d40c5d'


def read_dataset_gate(root='/home/node/aave-research-dataset'):
    import csv
    import json
    from pathlib import Path
    from shared.artifacts import checksum

    root = Path(root)
    provenance = '04_Validation_and_Provenance/'
    required = {
        'README.md', 'AGENTS.md', 'CLAUDE.md', 'VARIABLE_DICTIONARY.md',
        'DATA_CATALOG.csv', '01_HF_Vectors/CANONICAL_JOBS.csv',
        '01_HF_Vectors/dictionaries/users.parquet',
        '01_HF_Vectors/dictionaries/reserves.csv',
        '01_HF_Vectors/dictionaries/token_contracts.csv',
        '03_Code_and_Definitions/read_archive.py',
        '03_Code_and_Definitions/rebuild_account.py',
        provenance+'collection-events.jsonl',
        provenance+'canonical-archive-audit.json',
        provenance+'delivered-bundle-replay-proof.json',
        provenance+'dataset-status.json',
        provenance+'job-catalog-snapshot.json',
    }
    publication = root / '.metadata-publication/complete.json'
    if not publication.is_file():
        return None
    report = json.loads(publication.read_text())
    if (report.get('all_remote_sha256_exact') is not True or
            not required <= report.get('files', {}).keys()):
        return None
    for name in required:
        path = root / name
        if not path.is_file() or checksum(path) != report['files'][name]['sha256']:
            return None
        probe = report['sha_readbacks'][name]
        if probe['exact'] is not True or probe['sha256'] != report['files'][name]['sha256']:
            raise ValueError('Published reader file proof differs: '+name)
    with (root / 'DATA_CATALOG.csv').open(newline='') as stream:
        components = list(csv.DictReader(stream))
    if len(components) != 12 or any(row['status'] != 'accepted' for row in components):
        return None
    status = json.loads((root / (provenance+'dataset-status.json')).read_text())
    audit = json.loads((root / (provenance+'canonical-archive-audit.json')).read_text())
    replay = json.loads((root / (provenance+'delivered-bundle-replay-proof.json')).read_text())
    if not (status['all_catalog_components_accepted'] is True and
            audit['all_canonical_jobs_have_node_and_drive_archive_proofs'] is True and
            audit['jobs'] == 92294 and audit['observations'] == 910310934885 and
            audit['catalog_sha256'] == report['files']['01_HF_Vectors/CANONICAL_JOBS.csv']['sha256'] and
            replay['all_account_fields_and_balances_match_own_node_validated_sample'] is True):
        raise ValueError('Two-copy dataset acceptance is inconsistent')
    return dict(publication_sha256=checksum(publication),
                catalog_sha256=report['files']['DATA_CATALOG.csv']['sha256'])


def read_json(path):
    import json
    from pathlib import Path
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else None


def prepare_source(role, inventory, directory):
    import json
    from pathlib import Path
    from shared.artifacts import write_json
    from consolidate_hf import prepare
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    plan = root/'plan.json'
    if not plan.exists():
        write_json(root/'inventory.json', inventory)
        prepare(role, root/'inventory.json', plan)
    return plan.read_bytes()


def independent_node_proof(role, raw_plan, output):
    import json
    import sys
    from pathlib import Path
    from shared.artifacts import checksum, write_json
    root = Path('/home/node/hf-centralized/20261116')
    sys.path.insert(0, str(root))
    from reuse_node_proof import build_proof
    archive = Path('/home/node/hf-centralized/20261118-archive')/role
    checked = json.loads((archive/'node-checked.json').read_text())
    manifest_path = archive/'manifest.json'
    if checksum(manifest_path) != checked['manifest_sha256'] or checked.get('all_exact') is not True:
        raise ValueError('Independent full node SHA check is missing or changed')
    manifest = json.loads(manifest_path.read_text())
    plan = json.loads(raw_plan)
    backup_root = Path('/home/node/hf-backups/lan-20261116')/role
    entries = []
    for item in plan['files']:
        backup = Path(item['backup'])
        name = str(backup.relative_to('/home/node'))
        expected = manifest['files'].get(name)
        if (not expected or expected['sha256'] != item['sha256'] or expected['bytes'] != item['bytes']
                or expected.get('source_jobs') != item['source_jobs']):
            raise ValueError('Source retirement plan differs from frozen node manifest')
        entries.append(dict(path=str(backup.relative_to(backup_root)), sha256=item['sha256'], kind='fresh_sha256'))
    destination = Path(output)
    if destination.exists():
        saved = json.loads(destination.read_text())
        import hashlib
        if saved['plan_sha256'] != hashlib.sha256(raw_plan).hexdigest() or not saved['all_exact']:
            raise ValueError('Saved node proof differs')
        for item in saved['files']:
            s = Path(item['backup']).stat()
            ident = dict(dev=s.st_dev, ino=s.st_ino, bytes=s.st_size, ctime_ns=s.st_ctime_ns, mtime_ns=s.st_mtime_ns)
            if ident != item['current_identity']:
                raise ValueError('Verified node backup identity drifted')
        return destination.read_bytes()
    prior = json.loads((root/('missing-'+role+'.json')).read_text())['prior_backup_receipt']
    selection = dict(role=role, inventory_sha256=plan['inventory_sha256'], created_at=__import__('time').time(),
                     prior_backup_receipt=prior, verified_existing=entries,
                     full_node_sha_receipt_sha256=checksum(archive/'node-checked.json'))
    proof = build_proof(raw_plan, json.dumps(selection).encode(), backup_root, checked['started_at'])
    proof['full_node_sha_receipt_sha256'] = selection['full_node_sha_receipt_sha256']
    if not proof['all_exact']:
        raise ValueError('Independent node proof failed')
    write_json(destination, proof)
    return destination.read_bytes()


def retire_source(role, directory, raw_proof):
    import hashlib
    import json
    import subprocess
    from pathlib import Path
    helper = Path('/usr/local/libexec/dsc-hf-consolidate-20261116/consolidate_hf.py')
    if hashlib.sha256(helper.read_bytes()).hexdigest() != '19d96f155607942d964c661a6393edf637dd3c817ef5fcea46c16c7865d40c5d':
        raise ValueError('Root-owned retirement helper differs')
    home = Path('/home/fao' if role == 'web' else '/home/dsc-revert')
    root = Path(directory)
    if not root.resolve().is_relative_to(home/'hf-legacy-retirement-20261118'):
        raise ValueError('Unapproved legacy retirement request root')
    plan = root/'plan.json'
    completed = root/'plan.completed.json'
    if completed.exists():
        value = json.loads(completed.read_text())
        if value['plan_sha256'] != hashlib.sha256(plan.read_bytes()).hexdigest():
            raise ValueError('Completed retirement plan changed')
        if any(Path(a['path']).exists() for item in json.loads(plan.read_text())['files'] for a in item['aliases']):
            raise ValueError('A retired legacy path was recreated')
        return value
    proof = root/'node-proof.json'
    if proof.exists():
        if proof.read_bytes() != raw_proof:
            raise ValueError('Preserve the original independent node proof')
    else:
        with proof.open('xb') as stream:
            stream.write(raw_proof)
            stream.flush()
            __import__('os').fsync(stream.fileno())
    sha = hashlib.sha256(raw_proof).hexdigest()
    child = subprocess.run(['sudo','-n','/usr/bin/python3',str(helper),'retire','--role',role,
                            '--plan',str(plan),'--proof',str(proof),'--proof-sha256',sha],
                           capture_output=True, text=True, timeout=600)
    if child.returncode:
        if 'Resource temporarily unavailable' in child.stderr:
            raise BlockingIOError('HF writer is busy; retry this closed batch')
        raise RuntimeError(child.stderr[-3000:])
    return json.loads(child.stdout)


def batches(files, limit=8_000_000_000):
    group = []
    size = 0
    for item in files:
        if group and size+item['bytes'] > limit:
            yield group
            group, size = [], 0
        group.append(item)
        size += item['bytes']
    if group:
        yield group


def run(config_path):
    cfg = json.loads(Path(config_path).read_text())
    root = Path(cfg['control_root'])/'legacy-cleanup-20261118'
    root.mkdir(exist_ok=True)
    with (root/'coordinator.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with Client(cfg['scheduler'], security=security(cfg['private']), set_as_default=False) as client:
            def work(role):
                done = root/(role+'-complete.json')
                if done.exists():
                    return
                while True:
                    workers = client.scheduler_info(n_workers=-1)['workers']
                    node = [a for a,w in workers.items() if str(w['name']).startswith('flow-io-node')]
                    sources = [a for a,w in workers.items() if str(w['name']).startswith('flow-io-'+role)]
                    if node and sources:
                        checked = next(iter(client.run(read_dataset_gate, workers=node[:1]).values()))
                        if checked:
                            break
                    write_json(root/(role+'-progress.json'), dict(state='waiting_for_verified_node_and_drive_copies', at=time.time()))
                    time.sleep(30)
                # Compute shares its legacy writer guard with active HF
                # acceptance. Do not make numerical jobs wait for retirement.
                if role == 'web':
                    while True:
                        progress = next(iter(client.run(read_json, cfg['roles'][role]['local_root']+'/progress.json', workers=sources[:1]).values()))
                        if progress and progress.get('state') == 'complete' and not progress.get('active_jobs'):
                            break
                        write_json(root/(role+'-progress.json'), dict(state='waiting_for_compute_numerical_completion', at=time.time()))
                        time.sleep(30)
                original = next(iter(client.run(read_json, SOURCE_INVENTORY, workers=node[:1]).values()))
                files = [f for f in original['files'] if f['role'] == role and f['fully_consumed']]
                reports = []
                for number, group in enumerate(batches(files)):
                    directory = str(Path(HOMES[role])/'hf-legacy-retirement-20261118'/('batch-%04d'%number))
                    subset = dict(original, files=group)
                    while True:
                        try:
                            raw = client.submit(prepare_source, role, subset, directory, workers=sources[:1],
                                                allow_other_workers=False, resources={'flow_io':1}, pure=False).result(timeout=600)
                            plan = json.loads(raw)
                            if not plan['files']:
                                result = dict(files=0, logical_bytes=0, already_absent=len(plan['missing_sources']))
                            else:
                                proof_path = '/home/node/hf-centralized/20261118-cleanup/'+role+'/batch-%04d-proof.json'%number
                                proof = client.submit(independent_node_proof, role, raw, proof_path, workers=node[:1],
                                                      allow_other_workers=False, resources={'flow_io':1}, pure=False).result(timeout=600)
                                result = client.submit(retire_source, role, directory, proof, workers=sources[:1],
                                                       allow_other_workers=False, resources={'flow_io':1}, pure=False).result(timeout=660)
                            reports.append(result)
                            write_json(root/(role+'-progress.json'), dict(state='running', batch=number, at=time.time(),
                                retired_files=sum(x['files'] for x in reports), retired_logical_bytes=sum(x['logical_bytes'] for x in reports),
                                reports=reports))
                            break
                        except BlockingIOError:
                            time.sleep(5)
                write_json(done, dict(role=role, completed_at=time.time(), reports=reports,
                                      retired_files=sum(x['files'] for x in reports),
                                      retired_logical_bytes=sum(x['logical_bytes'] for x in reports),
                                      node_originals_retained=True, archive_root=SOURCE_ARCHIVE,
                                      dataset_acceptance=checked))
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(work, role) for role in ['revert','web']]
                for future in futures:
                    future.result()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--config', required=True)
    run(p.parse_args().config)
