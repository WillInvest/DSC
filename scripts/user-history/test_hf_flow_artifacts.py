"""Certificate and precise retirement tests using real temporary files."""
import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from hf_flow_artifacts import seal, combine, fast_verify, verify_destination, retire
from shared.artifacts import checksum, digest


class ArtifactCertificates(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'source'
        self.root.mkdir()
        (self.root / 'a').write_bytes(b'first exact file')
        (self.root / 'b').write_bytes(b'second exact file')
        self.files = {name: dict(bytes=(self.root / name).stat().st_size,
                                 sha256=checksum(self.root / name)) for name in ['a', 'b']}

    def copied(self, cert):
        dest = Path(self.temp.name) / 'destination'
        shutil.copytree(self.root, dest)
        return verify_destination(cert, dest)

    def test_combine_preserves_exact_manifest_and_identity_without_rehash(self):
        left = seal(self.root, {'a': self.files['a']})
        right = seal(self.root, {'b': self.files['b']})
        grouped = combine(Path(self.temp.name), [left, right])
        self.assertEqual(grouped['files'], {'source/' + k: v for k, v in self.files.items()})
        self.assertEqual(grouped['identities']['source/a'], left['identities']['a'])
        self.assertEqual(grouped['identities']['source/b'], right['identities']['b'])
        self.assertEqual(grouped['combined_from'], [digest(left), digest(right)])
        self.assertTrue(fast_verify(grouped))
        dest = Path(self.temp.name) / 'destination'
        shutil.copytree(self.root, dest / 'source')
        self.assertTrue(verify_destination(grouped, dest)['all_exact'])

    def test_combine_rejects_changed_duplicate_or_outside_members(self):
        cert = seal(self.root, self.files)
        with self.assertRaises(ValueError): combine(self.root, [cert, cert])
        other = Path(self.temp.name) / 'other'
        other.mkdir()
        with self.assertRaises(ValueError): combine(other, [cert])
        (self.root / 'a').write_bytes(b'changed after original certificate')
        with self.assertRaises(ValueError): combine(self.root, [cert])
        with self.assertRaises(ValueError): combine(self.root, [])

    def test_fast_restart_checks_identity_and_detects_touch(self):
        cert = seal(self.root, self.files)
        self.assertTrue(fast_verify(json.loads(json.dumps(cert))))
        p = self.root / 'a'
        os.utime(p, ns=(p.stat().st_atime_ns, p.stat().st_mtime_ns + 1000000))
        with self.assertRaises(ValueError): fast_verify(cert)

    def test_seal_rejects_wrong_bytes_symlink_and_parent_escape(self):
        wrong = dict(self.files, a=dict(bytes=999, sha256=self.files['a']['sha256']))
        with self.assertRaises(ValueError): seal(self.root, wrong)
        (self.root / 'link').symlink_to('a')
        with self.assertRaises(ValueError): seal(self.root, {'link': self.files['a']})
        with self.assertRaises(ValueError): seal(self.root, {'../source/a': self.files['a']})
        (self.root / 'sub').symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError): seal(self.root, {'sub/a': self.files['a']})

    def test_destination_proof_reads_copy_and_detects_bad_copy(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        self.assertTrue(proof['all_exact'])
        self.assertEqual(proof['source_certificate_sha256'], digest(cert))
        dest = Path(proof['destination'])
        (dest / 'a').write_bytes(b'corrupt copied file')
        with self.assertRaises(ValueError): verify_destination(cert, dest)

    def test_fresh_full_sha_seals_combine_without_rehash_and_keep_exact_scope(self):
        cert = seal(self.root, self.files)
        dest = Path(self.temp.name) / 'destination'
        shutil.copytree(self.root, dest)
        fresh = [seal(dest, {name: self.files[name]}) for name in self.files]
        with patch('hf_flow_artifacts.checksum', side_effect=AssertionError('duplicate full read')):
            proof = verify_destination(cert, dest, verified_certificates=fresh)
        self.assertTrue(proof['all_exact'])
        self.assertEqual(proof['destination_certificate']['files'], cert['files'])
        with self.assertRaisesRegex(ValueError, 'different bytes'):
            verify_destination(cert, dest, verified_certificates=fresh[:1])
        # A fresh seal of corrupted copy bytes still cannot satisfy source SHA pins.
        (dest/'b').write_bytes(b'changed')
        corrupt = seal(dest, {'b':dict(bytes=(dest/'b').stat().st_size,sha256=checksum(dest/'b'))})
        with self.assertRaisesRegex(ValueError, 'different bytes'):
            verify_destination(cert, dest, verified_certificates=[fresh[0],corrupt])

    def test_same_root_is_not_independent_preservation(self):
        cert = seal(self.root, self.files)
        with self.assertRaises(ValueError): verify_destination(cert, self.root)

    def test_destination_can_verify_source_host_paths_not_locally_visible(self):
        cert = seal(self.root, self.files)
        remote = dict(cert, root='/not-mounted-on-destination/source')
        dest = Path(self.temp.name) / 'destination'
        shutil.copytree(self.root, dest)
        proof = verify_destination(remote, dest, destination_host='node')
        self.assertEqual(proof['verification'], 'remote_sha256_copy')
        self.assertEqual(proof['destination_host'], 'node')
        self.assertEqual(proof['source_certificate_sha256'], digest(remote))

    def test_changed_destination_and_wrong_proof_pin_refuse_retirement(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        log = Path(self.temp.name) / 'delete.jsonl'
        with self.assertRaises(ValueError): retire(self.root, cert, log, proof, '0' * 64)
        (Path(proof['destination']) / 'b').write_bytes(b'copy was changed after verification')
        with self.assertRaises(ValueError): retire(self.root, cert, log, proof, digest(proof))
        self.assertTrue((self.root / 'a').exists())
        self.assertTrue((self.root / 'b').exists())

    def test_certificate_publication_preserves_existing_evidence(self):
        output = Path(self.temp.name) / 'sealed.json'
        cert = seal(self.root, self.files, output)
        before = output.read_bytes()
        self.assertEqual(json.loads(before), cert)
        with self.assertRaises(FileExistsError): seal(self.root, self.files, output)
        self.assertEqual(before, output.read_bytes())

    def test_retirement_refuses_unverified_proof(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        proof['all_exact'] = False
        with self.assertRaises(ValueError):
            retire(self.root, cert, Path(self.temp.name) / 'delete.jsonl', proof, digest(proof))
        self.assertTrue((self.root / 'a').exists())


@unittest.skipUnless(os.geteuid() == 0 and Path('/proc/self/fd').exists(),
                     'Retirement requires root Linux all-process FD visibility')
class PreciseRetirement(unittest.TestCase):
    setUp = ArtifactCertificates.setUp
    copied = ArtifactCertificates.copied

    def test_resume_partial_deletion_preserves_unlisted_files(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        (self.root / 'unrelated').write_bytes(b'keep')
        # Models interruption after unlink but before completion logging.
        (self.root / 'a').unlink()
        log = Path(self.temp.name) / 'delete.jsonl'
        result = retire(self.root, cert, log, proof, digest(proof))
        self.assertEqual(result['removed_files'], 1)
        self.assertEqual(result['already_absent_files'], 1)
        self.assertEqual((self.root / 'unrelated').read_bytes(), b'keep')
        self.assertEqual((Path(proof['destination']) / 'a').read_bytes(), b'first exact file')
        again = retire(self.root, cert, log, proof, digest(proof))
        self.assertEqual(again['removed_files'], 0)

    def test_group_intents_are_durable_before_unlink_and_partial_group_resumes(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        log = Path(self.temp.name) / 'delete.jsonl'
        unlink = Path.unlink
        calls = []
        def interrupted(path, *args, **kwargs):
            entries = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual({e['action'] for e in entries}, {'unlink_intent'})
            self.assertEqual({e['path'] for e in entries}, {str(self.root/n) for n in self.files})
            calls.append(path)
            if len(calls) == 2:
                raise OSError('crash after first group member')
            return unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', interrupted):
            with self.assertRaisesRegex(OSError, 'crash after first'):
                retire(self.root, cert, log, proof, digest(proof))
        self.assertEqual(sum((self.root/n).exists() for n in self.files), 1)
        result = retire(self.root, cert, log, proof, digest(proof))
        self.assertEqual(result['already_absent_files'], 1)
        self.assertEqual(result['removed_files'], 1)
        self.assertTrue(all((Path(proof['destination'])/n).exists() for n in self.files))

    def test_failed_intent_sync_never_unlinks_a_member(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        log = Path(self.temp.name) / 'delete.jsonl'
        with patch('hf_flow_artifacts.os.fsync', side_effect=OSError('sync failed')):
            with self.assertRaisesRegex(OSError, 'sync failed'):
                retire(self.root, cert, log, proof, digest(proof))
        self.assertTrue(all((self.root/n).exists() for n in self.files))

    def test_changed_source_and_changed_copy_refuse_retirement(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        (self.root / 'b').write_bytes(b'mutated source')
        with self.assertRaises(ValueError):
            retire(self.root, cert, Path(self.temp.name) / 'delete.jsonl', proof, digest(proof))
        self.assertTrue((self.root / 'a').exists())
        cert = seal(self.root, {n:dict(bytes=(self.root/n).stat().st_size,sha256=checksum(self.root/n)) for n in self.files})
        # Destination proof still belongs to the previous certificate.
        with self.assertRaises(ValueError):
            retire(self.root, cert, Path(self.temp.name) / 'delete.jsonl', proof, digest(proof))

    def test_open_file_and_unknown_hardlink_refuse_retirement(self):
        cert = seal(self.root, self.files)
        proof = self.copied(cert)
        with (self.root / 'b').open('rb'):
            with self.assertRaises(ValueError):
                retire(self.root, cert, Path(self.temp.name) / 'delete.jsonl', proof, digest(proof))
        self.assertTrue((self.root / 'a').exists())
        os.link(self.root / 'b', self.root / 'alias')
        with self.assertRaises(ValueError):
            retire(self.root, cert, Path(self.temp.name) / 'delete.jsonl', proof, digest(proof))
        self.assertTrue((self.root / 'a').exists())


if __name__ == '__main__': unittest.main()
