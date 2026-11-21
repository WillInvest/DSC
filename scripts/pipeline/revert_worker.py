"""Node client for the restricted Revert worker and its read-only output."""
import gzip
import json
from pathlib import Path
import re
import shlex
import subprocess

from shared.artifacts import checksum

PROTOCOL = 'dsc-revert-worker-v1'
HOST = 'dsc-revert-pipeline'


def parse_scope(value):
    match = re.fullmatch(r'[0-9a-f]{24}/([0-9]{1,9})-([0-9]{1,9})', value)
    if not match or int(match[1]) > int(match[2]):
        raise ValueError('Invalid Revert worker scope')
    return value


class RevertWorker:
    def __init__(self, config, scope):
        self.ssh = ['ssh', '-F', str(config), '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10']
        self.scope = parse_scope(scope)

    def command(self, operation):
        return [*self.ssh, HOST, PROTOCOL, operation, self.scope]

    def progress(self, operation):
        completed = subprocess.run(self.command(operation), capture_output=True,
                                   text=True, check=True, timeout=30)
        return json.loads(completed.stdout)

    def download(self, directory, destination):
        if directory not in ('archive', 'selected'):
            raise ValueError('Unexpected Revert artifact family')
        destination.mkdir(parents=True, exist_ok=True)
        return ['rsync','-a','--no-links','--partial','--checksum','-e',shlex.join(self.ssh),
                f'{HOST}:{self.scope}/traces/{directory}/', str(destination) + '/']


def verify_full_archive(root, accepted):
    """Check full calls against the same sealed batch metadata as the selection."""
    total = 0
    expected = set()
    for batch in accepted.values():
        start, stop = batch['start'], batch['stop']
        path = Path(root) / f'{start}-{stop-1}.jsonl.gz'
        expected.add(path)
        if checksum(path) != batch['archive_sha256']:
            raise ValueError('Full trace archive checksum differs')
        count = failed = 0
        with gzip.open(path, 'rt') as stream:
            for line in stream:
                row = json.loads(line)
                if row['identity'] != batch['identity'] or row['receipt_summary']['block_number'] != start+count:
                    raise ValueError('Full trace archive has inconsistent coverage or identity')
                count += 1
                failed += len(row['failed'])
        if count != stop-start or failed != batch['failed_transactions']:
            raise ValueError('Full trace archive count differs')
        total += failed
    if set(Path(root).glob('*.jsonl.gz')) != expected:
        raise ValueError('Unexpected full trace archive file')
    return total
