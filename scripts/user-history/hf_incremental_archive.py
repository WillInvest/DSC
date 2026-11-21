"""Back up accepted HF windows, including their replay inputs and successor."""
import argparse
import fcntl
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import time

from hf_incremental_window import validate_window
from shared.artifacts import checksum, write_json

FORMAT = 'dsc-hf-window-packed-drive-v1'


def load(path):
    return json.loads(path.read_text())


def window_inventory(root):
    status = load(root/'progress.json')
    result = load(root/'result/result.json')
    closing = load(root/'successor/boundary.json')
    window = load(root/'window.json')
    identity = validate_window(window)
    if (status['stage'] != 'locally_verified' or not result['coverage']['structural_exact']
            or not result['validation']['exact_reference_node_report']['all_exact']
            or not result['validation']['within_error_budget']
            or closing['next_block'] != status['stop']
            or closing['predecessor_hash'] != identity['end_hash']):
        raise ValueError('HF window lacks Node and successor acceptance')
    files = {}

    def add(name, expected=None):
        path = root/name
        if not path.resolve().is_relative_to(root) or path.resolve() != path or not path.is_file():
            raise ValueError('Missing or redirected HF backup input')
        sha = checksum(path)
        if expected is not None and sha != expected:
            raise ValueError(f'HF backup source checksum differs: {name}')
        files[str(name)] = dict(bytes=path.stat().st_size, sha256=sha)

    result_sha = status['result']['sha256']
    add('result/result.json', result_sha)
    if (closing['input_identity']['accepted_result_sha256'] != result_sha
            or window['window_sha256'] != result['scope']['window_sha256']
            or (status['start'], status['stop']) != (result['scope']['start'], result['scope']['stop'])):
        raise ValueError('HF result and successor belong to different windows')
    add('window.json')
    add('factors.json', result['factors_sha256'])
    add('token-replay.json', result['token_replay_sha256'])
    factors = load(root/'factors.json')
    add('pool/pool-inputs.json', factors['pool_sha256'])
    add('pool/pool-events.parquet', load(root/'pool/pool-inputs.json')['sha256'])
    token_replay = load(root/'token-replay.json')
    add('raw-tokens/registered-token-inputs.json', token_replay['raw_manifest_sha256'])
    for part in load(root/'raw-tokens/registered-token-inputs.json')['files']:
        add(Path('raw-tokens')/part['path'], part['sha256'])
    add('oracle/oracle-inputs.json', factors['oracle_sha256'])
    oracle_index = load(root/'oracle/oracle-inputs.json')
    add('oracle/plan.json', oracle_index['plan_sha256'])
    for job in oracle_index['receipts']:
        receipt = Path(job['path'])
        add(receipt.relative_to(root), job['sha256'])
        oracle = load(receipt)
        for item in oracle['files']:
            for layer in ('raw', 'decoded'):
                path = Path(oracle[layer+'_root'])/item['name']
                add(path.relative_to(root), item[layer+'_sha256'])
    add('result/wallets.json', result['wallet_dictionary_sha256'])
    for item in result['wallet_parts']+result['aggregates']['parts']:
        add(Path('result')/item['path'], item['sha256'])
    add('successor/boundary.json', status['successor_sha256'])
    add('successor/state.json.gz', closing['checkpoint']['sha256'])
    if checksum(root/'successor/wallets.json') != result['wallet_dictionary_sha256']:
        raise ValueError('Successor wallet dictionary differs from the accepted wallet dictionary')
    return dict(format=FORMAT, start=status['start'], stop=status['stop'], files=files,
                result_sha256=result_sha, successor_sha256=status['successor_sha256'],
                derived_files={'successor/wallets.json': 'result/wallets.json'})


def verify_download(remote, index, index_path, manifest_path, work, readback):
    from hf_flow_packing import _read_members
    from releases.read_archive import restore

    with TemporaryDirectory(prefix='restore-', dir=work) as temporary:
        isolated = Path(temporary)
        for source in (index_path, manifest_path):
            expected = dict(bytes=source.stat().st_size, sha256=checksum(source))
            readback(remote+'/'+source.name, expected, output=isolated/source.name)
        restored_index = load(isolated/'pack-index.json')
        if restored_index != index or load(isolated/'manifest.json') != load(manifest_path):
            raise ValueError('Drive metadata differs from the accepted HF archive')
        for package, expected in index['packs'].items():
            path = isolated/package
            path.parent.mkdir(parents=True, exist_ok=True)
            readback(remote+'/'+package, expected, output=path)
            members = {name: item for name, item in index['members'].items() if item['pack'] == package}
            if _read_members(path, members, package, True) != members:
                raise ValueError('Restored TAR member offsets or checksums differ')
        anchor = ('successor/state.json.gz', 'successor/boundary.json', 'result/wallets.json')
        for member in anchor:
            restore(isolated, member, isolated/'restart'/member, checksum(index_path))
        manifest = load(isolated/'manifest.json')
        boundary = load(isolated/'restart/successor/boundary.json')
        if (boundary['next_block'] != manifest['stop']
                or checksum(isolated/'restart/successor/state.json.gz') != boundary['checkpoint']['sha256']
                or checksum(isolated/'restart/result/wallets.json') != boundary['input_identity']['wallet_file_sha256']):
            raise ValueError('Restored HF restart inputs disagree')


def backup_window(root, directory, drive):
    from hf_flow_cloud import _copy_and_check, _readback
    from hf_flow_packing import pack

    root = root.resolve()
    manifest = window_inventory(root)
    directory.mkdir(parents=True, exist_ok=True)
    work = directory.resolve()
    remote = f"{drive.rstrip('/')}/{manifest['start']}-{manifest['stop']-1}/{manifest['result_sha256']}/packed-v1"
    receipt = work/'verified.json'
    with (work/'archive.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest_path = work/'manifest.json'
        if manifest_path.exists() and load(manifest_path) != manifest:
            raise ValueError('Existing HF backup input differs')
        if not manifest_path.exists():
            write_json(manifest_path, manifest)
        if receipt.exists():
            saved = load(receipt)
            if (saved['remote'] != remote or saved['manifest_sha256'] != checksum(manifest_path)
                    or not saved['full_drive_restore_exact']):
                raise ValueError('Existing HF Drive proof differs')
            return saved
        index = pack(root, manifest['files'], work)
        index_path = work/'pack-index.json'
        _copy_and_check(work, remote, [*index['packs'], index_path.name, manifest_path.name], work)
        verify_download(remote, index, index_path, manifest_path, work, _readback)
        proof = dict(format=FORMAT, remote=remote, start=manifest['start'], stop=manifest['stop'],
                     manifest_sha256=checksum(manifest_path), index_sha256=checksum(index_path),
                     files=len(index['members']), packages=len(index['packs']),
                     full_drive_restore_exact=True)
        staging = work/'catalog'
        staging.mkdir(exist_ok=True)
        candidate = staging/receipt.name
        write_json(candidate, proof)
        _copy_and_check(staging, remote, [candidate.name], staging)
        _readback(remote+'/'+candidate.name, dict(bytes=candidate.stat().st_size, sha256=checksum(candidate)))
        write_json(receipt, proof)
        return proof


def follow(windows, work, remote, start, through, once=False):
    work.mkdir(parents=True, exist_ok=True)
    path = work/'progress.json'
    identity = dict(windows=str(windows.resolve()), drive=remote, start=start)
    with (work/'follow.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        progress = load(path) if path.exists() else dict(identity=identity, next_block=start, verified_windows=0)
        if progress['identity'] != identity:
            raise ValueError('HF backup follower is bound to different sources')
        while progress['next_block'] <= through:
            prefix = str(progress['next_block'])
            candidates = [p for p in windows.glob(prefix+'-*') if re.fullmatch(prefix+r'-\d+', p.name)
                          and (p/'progress.json').is_file()
                          and load(p/'progress.json')['stage'] == 'locally_verified']
            if len(candidates) > 1:
                raise ValueError('More than one accepted HF window starts at the backup position')
            progress.update(state='waiting_for_window')
            write_json(path, progress)
            if not candidates:
                if once:
                    return progress
                time.sleep(30)
                continue
            root = candidates[0]
            progress.update(state='backing_up', current_window=root.name)
            write_json(path, progress)
            report = backup_window(root, work/root.name, remote)
            if report['start'] != progress['next_block'] or report['stop'] <= report['start']:
                raise ValueError('HF Drive proof is not contiguous')
            progress.update(state='verified', next_block=report['stop'], current_window=None,
                            verified_windows=progress['verified_windows']+1, last_verified=report)
            write_json(path, progress)
            if once:
                return progress
        progress.update(state='complete')
        write_json(path, progress)
        return progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--windows', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--remote', required=True)
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--through', type=int, required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    print(json.dumps(follow(args.windows, args.work, args.remote, args.start, args.through, args.once)))


if __name__ == '__main__':
    main()
