from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from pipeline import runtime


class RuntimeTests(unittest.TestCase):
    def test_node_transport_uses_local_commands_paths_and_rpc(self):
        with TemporaryDirectory() as temporary, patch.object(runtime, 'NODE_LOCAL', True):
            path = Path(temporary) / 'progress.json'
            path.write_text('{"state": "complete"}')
            self.assertEqual(runtime.node_command(['cat', path], terminal=True), ['cat', str(path)])
            self.assertEqual(runtime.node_path(str(path.parent) + '/'), str(path.parent) + '/')
            self.assertEqual(runtime.node_json(path), {'state': 'complete'})
            with patch.object(runtime.subprocess, 'run', return_value=subprocess.CompletedProcess(
                [], 0, stdout='{"number":"0x1"}',
            )) as run:
                self.assertEqual(runtime.rpc('eth_getBlockByNumber', ['finalized', False]), {'number': '0x1'})
            self.assertEqual(run.call_args.args[0][:2], [runtime.NODE_PYTHON, '-c'])

    def test_data_transport_preserves_node_ssh_and_trailing_slash(self):
        with patch.object(runtime, 'NODE_LOCAL', False):
            command = runtime.node_command(['cat', '/path with spaces'], terminal=True)
            self.assertEqual(command[-3:], ['-tt', runtime.NODE, "cat '/path with spaces'"])
            source = runtime.node_path('/source/')
            rsync = runtime.rsync_command(source, '/destination/', '--info=progress2')
            self.assertEqual(rsync[-2:], [f'{runtime.NODE}:/source/', '/destination/'])

    def test_exception_stops_a_running_local_worker(self):
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            with runtime.running_worker([sys.executable, '-c', 'import time; time.sleep(30)']) as worker:
                raise RuntimeError('interrupted')
        self.assertIsNotNone(worker.returncode)
