"""Archive acceptance uses only complete, receipt-declared node coverage."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shared.artifacts import checksum
from hf_market_replay.ledger import Ledger
from hf_market_replay.archive import accepted_manifest, _catalog_snapshot, run


class ArchiveManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.jobs = [dict(id='100-109', start=100, stop=110),
                     dict(id='110-119', start=110, stop=120)]
        self.plan = dict(generation='generation', jobs=self.jobs,
                         identity=dict(scope=dict(start=100, stop=120)))
        self.ledger = Ledger(self.root / 'flow.sqlite', 'generation', self.jobs)
        self.results = {}

    def tearDown(self):
        self.ledger.close()
        self.temp.cleanup()

    def publish(self, job):
        directory = self.root / 'received/node' / job['id']
        directory.mkdir(parents=True)
        part = directory / 'aggregates.parquet'
        part.write_bytes(b'closed aggregate bytes ' + job['id'].encode())
        receipt = directory / 'receipt.json'
        receipt.write_text(json.dumps(dict(job=job)))
        payload = 'payload-' + job['id']
        self.results[job['id']] = dict(
            job=job, payload_sha256=payload, reserves=['asset-a', 'asset-b'],
            parts=[dict(name=part.name, path=str(part), bytes=part.stat().st_size,
                        sha256=checksum(part))])
        self.ledger.accept(job['id'], payload, receipt, 'node')

    def read_receipt(self, directory, plan, job):
        return self.results[job['id']]

    def test_incomplete_coverage_cannot_be_archived(self):
        self.publish(self.jobs[0])
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            accepted_manifest(self.root, self.plan)

    def test_only_accepted_receipt_members_enter_the_manifest(self):
        for job in self.jobs:
            self.publish(job)
        (self.root / 'stray.parquet').write_bytes(b'unaccepted')
        with patch('hf_market_replay.runner.read_receipt', side_effect=self.read_receipt):
            manifest = accepted_manifest(self.root, self.plan)
        self.assertEqual(manifest['job_count'], 2)
        self.assertEqual(manifest['blocks'], 20)
        self.assertEqual(len(manifest['files']), 4)
        self.assertNotIn('stray.parquet', manifest['files'])

    def test_receipt_payload_must_match_node_acceptance(self):
        for job in self.jobs:
            self.publish(job)
        self.results[self.jobs[0]['id']]['payload_sha256'] = 'different'
        with patch('hf_market_replay.runner.read_receipt', side_effect=self.read_receipt):
            with self.assertRaisesRegex(ValueError, 'payload'):
                accepted_manifest(self.root, self.plan)

    def test_receipt_cannot_point_outside_the_authority_root(self):
        for job in self.jobs:
            self.publish(job)
        with self.ledger.db:
            self.ledger.db.execute('update jobs set receipt=? where id=?',
                                   ('/outside/receipt.json', self.jobs[0]['id']))
        with self.assertRaisesRegex(ValueError, 'outside'):
            accepted_manifest(self.root, self.plan)

    def test_catalog_backup_is_consistent_and_retained(self):
        for job in self.jobs:
            self.publish(job)
        metadata = self.root / 'metadata'
        metadata.mkdir()
        snapshot = _catalog_snapshot(self.root, metadata, self.plan)
        sha = checksum(snapshot)
        self.assertEqual(_catalog_snapshot(self.root, metadata, self.plan), snapshot)
        self.assertEqual(checksum(snapshot), sha)

    def test_publication_waits_for_checks_then_resumes_packing(self):
        for job in self.jobs:
            self.publish(job)
        config_path = self.root / 'cluster.json'
        config_path.write_text(json.dumps(dict(root=str(self.root), plan_path='mock-plan')))
        archive = self.root / 'archive'

        def metadata(root, plan, config, accepted):
            directory = root / 'metadata'
            directory.mkdir(exist_ok=True)
            paths = [directory / 'plan.json', directory / 'reserves.json']
            for path in paths:
                path.write_text('{}')
            return paths

        def readback(remote, expected, *, offset=None, count=None):
            name = remote.split('/generation/', 1)[1]
            import hashlib
            raw = (archive / name).read_bytes()
            if offset is not None:
                raw = raw[offset:offset + count]
            self.assertEqual(len(raw), expected['bytes'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), expected['sha256'])
            return dict(bytes=len(raw), sha256=expected['sha256'], exact=True)

        with patch('hf_market_replay.archive.load_plan', return_value=self.plan), \
                patch('hf_market_replay.runner.read_receipt', side_effect=self.read_receipt), \
                patch('hf_market_replay.archive._metadata', side_effect=metadata), \
                patch('hf_flow_cloud._copy_and_check') as copy, \
                patch('hf_flow_cloud._readback', side_effect=readback) as restore:
            copy.side_effect = RuntimeError('upload quota')
            with self.assertRaisesRegex(RuntimeError, 'quota'):
                run(config_path, archive)
            self.assertFalse((archive / 'accepted.json').exists())
            index_sha = checksum(archive / 'pack-index.json')
            copy.side_effect = None
            result = run(config_path, archive)
            self.assertEqual(checksum(archive / 'pack-index.json'), index_sha)
            self.assertTrue(result['publication']['remote_acceptance_exact']['exact'])
            self.assertEqual(restore.call_count, 5)
            self.assertEqual(run(config_path, archive), result)
            self.assertEqual(restore.call_count, 5)
            self.assertTrue((self.root / 'received/node/100-109/aggregates.parquet').exists())


if __name__ == '__main__':
    unittest.main()
