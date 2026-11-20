import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import archive_legacy_hf as archive


class LegacyArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name).resolve()/'node'
        self.source = self.home/'frozen'
        self.root = self.home/'archive'
        self.source.mkdir(parents=True)
        for name, value in [('HOME',self.home),('SOURCE',self.source),('ROOT',self.root)]:
            patcher=patch.object(archive,name,value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.part = self.home/'hf-vector-20261116/part-0000.parquet'
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(b'exact retained HF bits')
        self.item=dict(role='node',path=str(self.part),bytes=900,sha256=hashlib.sha256(self.part.read_bytes()).hexdigest(),
                       source_jobs=['100-102'],fully_consumed=True)
        self.inventory([self.item])

    def inventory(self, files):
        (self.source/'source-inventory-20261118.json').write_text(json.dumps(dict(generation='a'*64,files=files)))

    def test_node_vector_part_size_is_bound_to_real_bytes_and_original_sha(self):
        value=archive.prepare('node')
        entry=next(iter(value['files'].values()))
        self.assertEqual(entry['bytes'],self.part.stat().st_size)
        self.assertEqual(entry['inventory_bytes'],900)
        self.assertEqual(entry['sha256'],self.item['sha256'])
        archive.check_node('node')
        self.assertTrue(json.loads((self.root/'node/node-checked.json').read_text())['all_exact'])
        archive.check_node('node')

    def test_different_bytes_never_get_an_independent_node_receipt(self):
        archive.prepare('node')
        self.part.write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            archive.check_node('node')
        self.assertFalse((self.root/'node/node-checked.json').exists())

    def test_previously_checked_identity_drift_is_not_silently_accepted(self):
        archive.check_node('node')
        self.part.write_bytes(b'new bytes')
        with self.assertRaisesRegex(ValueError,'identity drifted'):
            archive.check_node('node')

    def test_helper_expected_size_is_still_strict(self):
        relative=Path('old-generation/jobs/100-102/accounts.parquet')
        part=self.home/'hf-backups/lan-20261116/revert'/archive.WORK/relative
        part.parent.mkdir(parents=True)
        part.write_bytes(b'bytes')
        self.inventory([dict(self.item,role='revert',path=str(archive.VIRTUAL/relative),bytes=999)])
        with self.assertRaisesRegex(ValueError,'size differs'):
            archive.prepare('revert')

    def test_frozen_input_archive_refuses_operational_credentials(self):
        bundle=self.source/'hf-frozen-inputs.tar.gz'
        with tarfile.open(bundle,'w:gz') as writer:
            info=tarfile.TarInfo('worker-state/private/host-key.pem')
            info.size=4
            writer.addfile(info,io.BytesIO(b'test'))
        (self.source/'hf-frozen-inputs.tar.gz.verified.json').write_text(json.dumps(dict(
            all_member_hashes_exact=True,archive_bytes=bundle.stat().st_size,archive_sha256=hashlib.sha256(bundle.read_bytes()).hexdigest())))
        with self.assertRaisesRegex(ValueError,'credential path'):
            archive.prepare('inputs')
        self.assertFalse((self.root/'inputs/manifest.json').exists())


if __name__ == '__main__':
    unittest.main()
