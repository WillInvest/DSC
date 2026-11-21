import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from hf_incremental_dispatch import dispatch, request, schedule
from hf_incremental_transport import CANDIDATE, INPUTS, PROTOCOL, read_archive
from hf_incremental_worker import parse
from shared.artifacts import checksum


class HelperDispatchTests(unittest.TestCase):
    def test_forced_command_rejects_shell_and_unscoped_paths(self):
        scope = 'a'*64
        self.assertEqual(parse(f'{PROTOCOL} compute {scope}'), ('compute', scope))
        for command in ('/bin/sh', f'{PROTOCOL} download ../private',
                        f'{PROTOCOL} status {scope};cat /etc/passwd', 'rsync --server --sender'):
            with self.assertRaisesRegex(ValueError, 'Unsupported'):
                parse(command)

    def test_tar_import_rejects_traversal_before_writing(self):
        with TemporaryDirectory() as temporary:
            target = Path(temporary)
            wire = io.BytesIO()
            with tarfile.open(fileobj=wire, mode='w') as archive:
                item = tarfile.TarInfo('../state.json.gz')
                item.size = 1
                archive.addfile(item, io.BytesIO(b'x'))
            wire.seek(0)
            with self.assertRaisesRegex(ValueError, 'Invalid or duplicate'):
                read_archive(wire, target, lambda _: True, 1024)
            self.assertEqual(list(target.iterdir()), [])

    def test_control_request_does_not_consume_the_coordinator_input(self):
        with patch('hf_incremental_dispatch.subprocess.run') as execute:
            execute.return_value.stdout = b'{"state":"missing"}'
            self.assertEqual(request({'host': 'node-helper', 'ssh_config': 'helper.conf'}, 'status', 'a'*64),
                             {'state': 'missing'})
        self.assertEqual(execute.call_args.kwargs['stdin'], subprocess.DEVNULL)

    def test_worker_protocol_round_trip_and_idempotent_download(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = {name: root/name for name in INPUTS}
            for name, path in paths.items():
                path.write_text(name)
            marker = root/'launches'
            launcher = root/'launch'
            launcher.write_text(
                f'#!{sys.executable}\n'
                'import hashlib,json,sys\n'
                'from pathlib import Path\n'
                f'with Path({str(marker)!r}).open("a") as log: log.write("one\\n")\n'
                'out=Path(sys.argv[2]); out.mkdir()\n'
                'for name in ("wallets.json","wallet.parquet","aggregate.parquet"): (out/name).write_text(name)\n'
                'def sha(name): return hashlib.sha256((out/name).read_bytes()).hexdigest()\n'
                'def part(name): return dict(path=name,sha256=sha(name))\n'
                '(out/"candidate.json").write_text(json.dumps(dict(wallet_dictionary_sha256=sha("wallets.json"),'
                'wallet_parts=[part("wallet.parquet")],aggregates=dict(parts=[part("aggregate.parquet")]))))\n')
            launcher.chmod(0o700)
            worker = Path(__file__).with_name('hf_incremental_worker.py')

            def request(_helper, operation, scope, *, stdin=None, stdout=subprocess.PIPE):
                result = subprocess.run([sys.executable, str(worker), '--root', str(root/'worker'),
                                         '--launcher', str(launcher)], stdin=stdin, stdout=stdout,
                                        env=dict(os.environ, SSH_ORIGINAL_COMMAND=f'{PROTOCOL} {operation} {scope}'),
                                        check=True, stderr=subprocess.PIPE)
                return json.loads(result.stdout) if stdout == subprocess.PIPE else None

            with patch('hf_incremental_dispatch.request', side_effect=request):
                first = dispatch({'host': 'helper'}, paths, root/'received')
                again = dispatch({'host': 'helper'}, paths, root/'received')
            self.assertEqual(first, again)
            self.assertEqual(checksum(Path(first['path'])/CANDIDATE), first['sha256'])
            self.assertEqual(marker.read_text().splitlines(), ['one'])

    def test_scheduler_skips_unavailable_helper_but_not_invalid_evidence(self):
        path = Path('/token')
        workers = [{'host': 'first'}, {'host': 'second'}]
        good = {'helper': 'second'}
        with patch('hf_incremental_dispatch.dispatch', side_effect=[OSError('offline'), good]):
            self.assertEqual(schedule(workers, path, path, path, path, path), (good, ['first']))
        with patch('hf_incremental_dispatch.dispatch', side_effect=ValueError('wrong provenance')) as call:
            with self.assertRaisesRegex(ValueError, 'wrong provenance'):
                schedule(workers, path, path, path, path, path)
            self.assertEqual(call.call_count, 1)
