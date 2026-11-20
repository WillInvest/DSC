import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import publish_reconstruction_inputs as release


class ReconstructionInputTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base/'tokens/0x01'
        (self.root/'encoded').mkdir(parents=True)
        self.common = Path(__file__).resolve().parents[1]/'event-cryo-extraction/common.py'
        self.chunks = release.event_chunks(self.common)
        self.plan = dict(end_block=102, end_hash='0xabc', chunk_size=2,
            jobs=[dict(address='0x01', asset='0x02', kind='aToken',
                       start_block=100, node_dir=str(self.root))])
        self.plan_path = self.base/'plan.json'
        self.plan_path.write_text(json.dumps(self.plan))
        names = list(self.chunks(self.plan['jobs'][0], self.plan))
        self.name = names[0]
        data = b'original encoded event bytes'
        (self.root/'encoded'/self.name).write_bytes(data)
        identity = dict(end_block=102, end_hash='0xabc', chunk_size=2,
                        address='0x01', start_block=100)
        (self.root/'identity.json').write_text(json.dumps(identity))
        self.manifest = dict(identity=identity, total_chunks=2,
            empty_chunks=[names[1]], files={self.name:dict(bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(), rows=1)})
        self.save()

    def save(self):
        (self.root/'manifest.json').write_text(json.dumps(self.manifest))

    def inventory(self):
        return release.token_inventory(self.base, self.plan_path, self.chunks)

    def test_empty_evidence_required_and_cutoff_bound(self):
        self.assertEqual(self.inventory()['contracts'][0]['stop'], 103)
        self.manifest['empty_chunks'] = []
        self.save()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            self.inventory()
        self.manifest['identity']['end_hash'] = '0xwrong'
        self.save()
        with self.assertRaisesRegex(ValueError, 'identity'):
            self.inventory()

    def test_full_remote_byte_readback_and_idempotent_acceptance(self):
        out = self.base/'published'
        out.mkdir()
        (out/'source-manifest.json').write_text(json.dumps(self.inventory()))
        copied = {}

        def copy(base, remote, files, ops):
            copied.update({name:(Path(base)/name).read_bytes() for name in files})

        def readback(remote, expected):
            name = remote.removeprefix('remote:/inputs/')
            data = copied[name]
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected['sha256'])
            self.assertEqual(len(data), expected['bytes'])
            return dict(bytes=len(data), sha256=expected['sha256'], exact=True)

        with patch.object(release, '_copy_and_check', copy), patch.object(release, '_readback', readback):
            result = release.publish(self.base, out, 'remote:/inputs')
        self.assertTrue(result['all_remote_sha256_exact'])
        self.assertTrue(result['original_sources_retained'])
        self.assertEqual(result['numerical_coverage_increment'], 0)
        self.assertTrue((self.root/'encoded'/self.name).exists())
        self.assertEqual(release.publish(self.base, out, 'remote:/inputs'), result)

    def test_corrupted_original_never_becomes_accepted_archive(self):
        out = self.base/'published'
        out.mkdir()
        (out/'source-manifest.json').write_text(json.dumps(self.inventory()))
        p = self.root/'encoded'/self.name
        original = p.read_bytes()
        p.write_bytes(b'x'+original[1:])
        with self.assertRaisesRegex(ValueError, 'SHA'):
            release.publish(self.base, out, 'remote:/inputs')
        self.assertFalse((out/'complete.json').exists())
        self.assertTrue(p.exists())

    def test_pool_baseline_and_tail_must_be_contiguous_and_cutoff_updates(self):
        first = dict(files={}, contracts=[dict(address='pool', kind='Pool', start=1, stop=5)],
                     end_block=4, end_hash='first', plan_sha256='a')
        tail = dict(files={}, contracts=[dict(address='pool', kind='Pool', start=5, stop=10)],
                    end_block=9, end_hash='last', plan_sha256='b')
        merged = release.merge_pool_inventories([first, tail])
        self.assertEqual(merged['end_block'], 9)
        self.assertEqual(merged['end_hash'], 'last')
        for wrong in [4, 6]:
            tail['contracts'][0]['start'] = wrong
            with self.assertRaisesRegex(ValueError, 'gap or overlap'):
                release.merge_pool_inventories([first, tail])


if __name__ == '__main__':
    unittest.main()
