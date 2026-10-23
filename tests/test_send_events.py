import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).parents[1] / 'scripts/send_events.py'


class TransferTest(unittest.TestCase):
    def test_manifest_published_last_and_not_after_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = (root / 'source').resolve()
            source.mkdir()
            payload = gzip.compress(b'{}\n')
            (source / 'batch.jsonl.gz').write_bytes(payload)
            (source / 'manifest.json').write_text(json.dumps({'chunks': [
                {'file': 'batch.jsonl.gz', 'sha256': hashlib.sha256(payload).hexdigest()}
            ]}))
            bin_dir = root / 'bin'
            bin_dir.mkdir()
            calls = root / 'calls'
            for command in ['ssh', 'rsync']:
                executable = bin_dir / command
                executable.write_text('#!' + sys.executable + '\n' + '''import json,os,sys
with open(os.environ['CALLS'], 'a') as stream:
    stream.write(json.dumps(sys.argv) + '\\n')
if os.environ.get('FAIL_DATA') and '--files-from=-' in sys.argv:
    sys.exit(1)
''')
                executable.chmod(0o755)
            env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH'], CALLS=str(calls))
            cmd = [sys.executable, str(SCRIPT), '--source', str(source), '--destination-dir', '/tmp/pilot']
            result = subprocess.run(cmd, env=env, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(len(recorded), 3)
            self.assertIn('--files-from=-', recorded[1])
            self.assertIn(str(source / 'manifest.json'), recorded[2])
            calls.unlink()
            result = subprocess.run(cmd, env=dict(env, FAIL_DATA='1'), capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(str(source / 'manifest.json'), calls.read_text())


if __name__ == '__main__':
    unittest.main()
