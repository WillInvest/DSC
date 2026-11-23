import json
import io
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from pipeline.node_status import (FORMAT, LEGACY_COMMAND, MAX_MONITOR_BYTES, MONITOR_COMMAND,
                                  fetch_legacy, hf_progress, legacy_report, publish_monitor,
                                  receive_monitor)
from pipeline.main import main
from pipeline.monitor_health import coverage, drive_status
from shared.artifacts import write_json


class NodeStatusTests(unittest.TestCase):
    def test_live_stage_watermarks_use_accepted_publication_and_hf_cursor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            meta = root/'metadata'
            meta.mkdir()
            write_json(meta/'publication.json', dict(stage='complete', market_through=100, revert_through=100))
            write_json(meta/'encoded-publication.json', dict(stage='complete', market_through=101))
            write_json(meta/'global-revert-publication.json', dict(stage='complete', through=100))
            write_json(meta/'protocol-revert-publication.json', dict(stage='complete', through=98))
            market = dict(stage='decode', state='running', cycle=dict(start=101, end=110))
            rows = coverage(root, market, {'through': 95}, 112)
            self.assertEqual([(v['through'], v['lag']) for v in rows.values()],
                             [(100, 12), (100, 12), (98, 14), (95, 17)])
            self.assertEqual((rows['events']['start'], rows['events']['end']), (101, 110))

    def test_drive_uploaded_packs_and_restore_coverage_are_independent(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root/'wallet', dict(uploaded_pack_count=8, expected_pack_count=10, pending_pack_count=2))
            write_json(root/'revert', dict(format='dsc-revert-restore-plan-drive-receipt-v1',
                                          through=100, restore={'exact': True}))
            rows = drive_status({'wallet': [root/'wallet'], 'revert': [root/'revert']}, {})
            self.assertEqual(rows['wallet']['uploaded_pack_count'], 8)
            self.assertNotIn('verified_through', rows['wallet'])
            self.assertEqual(rows['revert']['verified_through'], 100)

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

    def test_private_monitor_accepts_node_watermarks_when_data_is_offline(self):
        payload = dict(format=FORMAT, observed_at=20.0,
                       node_market=dict(coordinator=dict(state='running', stage='receipts', error='private'),
                                        compaction=dict(through=100, path='/private/catalog')),
                       legacy=dict(availability='unreachable'), hf=dict(through=90), hf_drive=dict(through=80))
        payload.update({key: {} for key in ('handoff', 'shadow', 'market_drive', 'historical_market',
                                            'historical_market_drive', 'historical_revert_drive')})
        payload['health'] = dict(chain=dict(availability='available', latest=140, finalized=130),
                                 stages=dict(events=dict(through=100, lag=30, path='/private/input')),
                                 workers=dict(CU1=dict(availability='reachable', task='hf'),
                                              private=dict(availability='reachable')))
        with TemporaryDirectory() as directory:
            path = Path(directory)/'report.json'
            receive_monitor(path, io.BytesIO(json.dumps(payload).encode()))
            saved = json.loads(path.read_text())
            self.assertEqual(saved['catalog'], {'through': 100})
            self.assertEqual(saved['legacy'], {'availability': 'unreachable'})
            self.assertEqual((saved['hf']['through'], saved['hf_drive']['through']), (90, 80))
            self.assertEqual(saved['health']['stages'], {'events': {'through': 100, 'lag': 30}})
            self.assertEqual(set(saved['health']['workers']), {'CU1'})
            self.assertNotIn('private', json.dumps(saved))
            with self.assertRaisesRegex(ValueError, 'size limit'):
                receive_monitor(path, io.BytesIO(b'x' * (MAX_MONITOR_BYTES + 1)))
            self.assertEqual(json.loads(path.read_text()), saved)

    def test_monitor_publish_uses_a_fixed_ssh_command(self):
        config = dict(host='dsc-hf-compute', ssh_config='read-only.conf')
        with patch('pipeline.node_status.subprocess.run') as execute:
            publish_monitor(config, {'format': FORMAT})
        args, kwargs = execute.call_args
        self.assertEqual(args[0][-2:], [config['host'], MONITOR_COMMAND])
        self.assertEqual(json.loads(kwargs['input']), {'format': FORMAT})
