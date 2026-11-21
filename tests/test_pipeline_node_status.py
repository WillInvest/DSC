import json
import io
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.node_status import FORMAT, LEGACY_COMMAND, fetch_legacy, hf_progress, legacy_report
from pipeline.main import main
from shared.artifacts import write_json


class NodeStatusTests(unittest.TestCase):
    def test_legacy_report_exposes_counts_without_worker_errors_or_paths(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root/'progress.json', dict(state='running', stage='events',
                                                 completed_through=100, error='private error',
                                                 cycle=dict(id='101-105', start=101, end=105)))
            write_json(root/'compaction.json', dict(state='complete', through=100,
                                                   canonical_root='/private/internal'))
            write_json(root/'cycles/101-105/events/progress.json', dict(complete_addresses=11))
            result = legacy_report(root)
            self.assertEqual((result['format'], result['market']['events']['complete_addresses']), (FORMAT, 11))
            self.assertNotIn('private', json.dumps(result))

    def test_pending_hf_work_does_not_advance_the_accepted_waterline(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            path = root/'progress.json'
            write_json(path, dict(state='running', next_block=101, pending={'start': 101, 'stop': 111}))
            write_json(root/'101-110/progress.json', dict(stage='dispatch_hf', helper='dsc-hf-data'))
            result = hf_progress(path, root)
            self.assertEqual(result['through'], 100)
            self.assertEqual(result['pending']['stop'], 111)
            self.assertEqual(result['active_window']['helper'], 'dsc-hf-data')

    def test_legacy_endpoint_is_fixed_and_offline_status_does_not_expose_stderr(self):
        profile = dict(host='dsc-hf-data', ssh_config='read-only.conf')
        with patch('pipeline.node_status.subprocess.run', side_effect=subprocess.CalledProcessError(
                255, ['ssh'], stderr=b'private error')) as execute:
            self.assertEqual(fetch_legacy(profile), {'availability': 'unreachable'})
        args, kwargs = execute.call_args
        self.assertEqual(args[0][-1], LEGACY_COMMAND)
        self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)

    def test_public_status_accepts_the_combined_report_without_a_legacy_root(self):
        with TemporaryDirectory() as directory:
            config = Path(directory)/'roles.json'
            write_json(config, {'version': 1})
            output = io.StringIO()
            with (
                patch('pipeline.main.NODE_LOCAL', True),
                patch.object(sys, 'argv', ['main.py', 'status', '--status-config', str(config)]),
                patch('pipeline.node_status.report', return_value={'format': FORMAT}) as assemble,
                patch.object(sys, 'stdout', output),
            ):
                main()
            assemble.assert_called_once_with({'version': 1})
            self.assertEqual(json.loads(output.getvalue()), {'format': FORMAT})
