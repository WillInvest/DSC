"""Dispatch one pinned offline HF job from Node to a configured SSH helper."""
import argparse
import json
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory, TemporaryFile

from hf_incremental_transport import (BUNDLE, CANDIDATE, MAX_RESULT_BYTES, PROTOCOL, artifact_name,
                                      file_identity, hex_digest, input_manifest, read_archive,
                                      result_paths, write_archive)
from shared.artifacts import digest, write_json


def request(helper, operation, scope, *, stdin=None, stdout=subprocess.PIPE):
    host = helper['host']
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9.-]*', host):
        raise ValueError('Invalid HF helper SSH alias')
    timeout = helper.get('compute_timeout_seconds', 21600) if operation == 'compute' else 900
    command = ['ssh', '-F', helper['ssh_config'], '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
               '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
               host, PROTOCOL, operation, scope]
    result = subprocess.run(command, stdin=subprocess.DEVNULL if stdin is None else stdin,
                            stdout=stdout, stderr=subprocess.PIPE,
                            check=True, timeout=timeout)
    return json.loads(result.stdout) if stdout == subprocess.PIPE else None


def dispatch(helper, paths, output):
    manifest = input_manifest(paths)
    scope = digest(manifest)
    output.mkdir(parents=True, exist_ok=True)
    target = output/scope
    state = request(helper, 'status', scope)
    if state['state'] == 'missing':
        with TemporaryDirectory(dir=output) as temporary, TemporaryFile() as wire:
            marker = Path(temporary)/BUNDLE
            write_json(marker, manifest)
            write_archive(wire, {**paths, BUNDLE: marker})
            wire.seek(0)
            state = request(helper, 'submit', scope, stdin=wire)
    if state['state'] == 'queued':
        state = request(helper, 'compute', scope)
    sha = state.get('candidate_sha256', '')
    if state['state'] != 'computed' or not hex_digest(sha):
        raise ValueError('HF helper did not declare an exact candidate')
    if target.exists():
        if target.is_symlink() or file_identity(target/CANDIDATE)['sha256'] != sha:
            raise ValueError('Previously received HF result conflicts with helper')
        result_paths(target)
    else:
        with TemporaryDirectory(dir=output) as temporary, TemporaryFile() as wire:
            stage = Path(temporary)/'receive'
            stage.mkdir()
            request(helper, 'download', scope, stdout=wire)
            wire.seek(0)
            members = read_archive(wire, stage, lambda name: artifact_name(name) and (
                name in {CANDIDATE, 'wallets.json'} or name.endswith('.parquet')), MAX_RESULT_BYTES)
            if file_identity(stage/CANDIDATE)['sha256'] != sha or set(result_paths(stage)) != members:
                raise ValueError('Downloaded HF result fails manifest verification')
            stage.rename(target)
    return dict(path=str(target), sha256=sha, scope=scope, helper=helper['host'])


def input_paths(window, state, factors, tokens):
    paths = {'window.json': window, 'state.json.gz': state,
             'factors.json': factors, 'token-replay.json': tokens}
    wallets = state.with_name('wallets.json')
    if wallets.is_file():
        paths[wallets.name] = wallets
    return paths


def schedule(helpers, window, state, factors, tokens, output):
    paths = input_paths(window, state, factors, tokens)
    failed = []
    for helper in helpers:
        try:
            return dispatch(helper, paths, output), failed
        except (subprocess.SubprocessError, OSError):
            failed.append(helper['host'])
    return None, failed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('helpers', 'window', 'state', 'factors', 'tokens', 'output'):
        parser.add_argument('--'+field, required=True, type=Path)
    args = parser.parse_args()
    helpers = json.loads(args.helpers.read_text())
    candidate, failed = schedule(helpers, args.window, args.state, args.factors, args.tokens, args.output)
    print(json.dumps(dict(candidate=candidate, failed=failed)))
    return 0 if candidate else 1


if __name__ == '__main__':
    raise SystemExit(main())
