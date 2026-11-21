"""OpenSSH forced command for the isolated Aave HF compute helper."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory

from hf_incremental_transport import (BUNDLE, CANDIDATE, INPUTS, MAX_INPUT_BYTES, PROTOCOL,
                                      check_input, file_identity, hex_digest, read_archive,
                                      result_paths, write_archive)


def parse(command):
    if len(command) > 160:
        raise ValueError('Unsupported HF worker command')
    arguments = command.split()
    if arguments == [PROTOCOL, 'ping']:
        return 'ping', None
    if (len(arguments) == 3 and arguments[0] == PROTOCOL
            and arguments[1] in {'status', 'submit', 'compute', 'download'} and hex_digest(arguments[2])):
        return arguments[1], arguments[2]
    raise ValueError('Unsupported HF worker command')


def status(job):
    result = job/'result'/CANDIDATE
    if result.exists():
        return dict(state='computed', candidate_sha256=file_identity(result)['sha256'])
    return dict(state='queued' if (job/'inputs'/BUNDLE).is_file() else 'missing')


def dispatch(root, launcher, operation, scope):
    if operation == 'ping':
        print(json.dumps({'protocol': PROTOCOL}))
        return
    job = root/scope
    if job.is_symlink():
        raise ValueError('Redirected offline job')
    if operation == 'status':
        print(json.dumps(status(job)))
        return
    if operation == 'download':
        directory = job/'result'
        write_archive(sys.stdout.buffer, result_paths(directory))
        return
    job.mkdir(parents=True, exist_ok=True)
    inputs, output = job/'inputs', job/'result'
    with (job/'worker.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if operation == 'submit':
            with TemporaryDirectory(dir=job) as temporary:
                stage = Path(temporary)/'bundle'
                stage.mkdir()
                names = INPUTS | {BUNDLE, 'wallets.json'}
                read_archive(sys.stdin.buffer, stage, names.__contains__, MAX_INPUT_BYTES+2**20)
                check_input(stage, scope)
                if inputs.exists():
                    check_input(inputs, scope)
                else:
                    stage.rename(inputs)
        else:
            check_input(inputs, scope)
            if not output.exists():
                with (job/'compute.log').open('a') as log:
                    subprocess.run([str(launcher), str(inputs), str(output)],
                                   stdout=log, stderr=subprocess.STDOUT, check=True)
            result_paths(output)
        print(json.dumps(status(job)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--launcher', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        dispatch(args.root, args.launcher, *parse(os.environ.get('SSH_ORIGINAL_COMMAND', '')))
    except (ValueError, OSError, KeyError, tarfile.TarError, subprocess.SubprocessError):
        print('HF worker request failed', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
