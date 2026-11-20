"""Staging must be bounded to accepted source manifests and immutable receipt pins."""
import json
import os
import shutil
import tempfile
import unittest
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
from hf_flow_io import recover_published_transfer, _copy_batch
from hf_flow_queue import LocalLedger
from hf_flow_rsync import manifest_entries,stage_source
from hf_flow_artifacts import seal
from shared.artifacts import checksum,write_json

class OperationalJsonImport(unittest.TestCase):
    def test_frozen_shared_module_does_not_shadow_operational_writer(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);shared=root/'shared';shared.mkdir()
            (shared/'__init__.py').write_text('')
            (shared/'artifacts.py').write_text('def checksum(p): return "unused"\ndef digest(v): return "unused"\ndef write_json(path,data): raise AssertionError("frozen writer selected")\n')
            code='import json,pathlib,sys;sys.path.insert(0,sys.argv[1]);sys.path.insert(1,sys.argv[2]);import hf_flow_io;p=pathlib.Path(sys.argv[1])/"probe.json";hf_flow_io.write_json(p,{"exact":123},durable=False);assert json.loads(p.read_text())=={"exact":123}'
            result=subprocess.run([sys.executable,'-c',code,str(root),str(Path(__file__).parent)],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)

class RsyncScope(unittest.TestCase):
    def fixture(self, root):
        out=root/'outputs';out.mkdir();part=out/'parts.parquet';part.write_bytes(b'exact')
        validation=out/'validation.json';validation.write_text('{}')
        receipt=out/'receipt.json';receipt.write_text('{}')
        value=dict(path=str(receipt),sha256=checksum(receipt),certificate=None,
                   receipt=dict(parts=[dict(path=str(part),sha256=checksum(part))],aggregate_parts=[],validation=str(validation),validation_sha256=checksum(validation),bytes=5))
        cfg=dict(local_root=str(root),output_root=str(out),rsync_transport=dict(destination='unused',node_stage_base='/unused'))
        with LocalLedger(root/'local.sqlite','g') as db:db.commit('1-2',value)
        return value,cfg

    def test_manifest_rejects_parent_escape_and_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);v,c=self.fixture(root)
            self.assertEqual(len(manifest_entries(v,c['output_root'])),3)
            v['receipt']['parts'][0]['path']=str(root/'outside')
            with self.assertRaises(ValueError):manifest_entries(v,c['output_root'])
            link=Path(c['output_root'])/'link';link.symlink_to(root/'outside')
            v['receipt']['parts'][0]['path']=str(link)
            with self.assertRaises(ValueError):manifest_entries(v,c['output_root'])

    def test_unaccepted_or_changed_receipt_never_invokes_rsync(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);v,c=self.fixture(root)
            with patch('hf_flow_rsync.subprocess.run') as run:
                for pins in [[('missing',v['sha256'])],[('1-2','wrong')]]:
                    with self.assertRaises(ValueError):stage_source(c,pins,'a'*24,{'parts.parquet':'x'})
                run.assert_not_called()

    def test_outside_manifest_is_rejected_before_transport(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);v,c=self.fixture(root)
            with patch('hf_flow_rsync.subprocess.run') as run:
                with self.assertRaises(ValueError):stage_source(c,[('1-2',v['sha256'])],'a'*24,{'../private':'sha'})
                run.assert_not_called()

class PublishedTransferRecovery(unittest.TestCase):
    def test_crash_after_publication_removes_only_known_verified_aliases(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);canonical=root/'part.parquet';canonical.write_bytes(b'exact closed bytes')
            incoming=root/'incoming';incoming.mkdir();stage=incoming/'part.parquet'
            temporary=root/'part.parquet.transferring';os.link(canonical,stage);os.link(canonical,temporary)
            unrelated=root/'unknown-link';os.link(canonical,unrelated)
            recover_published_transfer(canonical,checksum(canonical),[temporary,stage])
            self.assertTrue(canonical.exists());self.assertTrue(unrelated.exists())
            self.assertFalse(temporary.exists());self.assertFalse(stage.exists())
            self.assertEqual(canonical.stat().st_nlink,2)  # Unknown alias remains for strict certificate rejection.
            unrelated.unlink();self.assertEqual(canonical.stat().st_nlink,1)

    def test_batch_recovery_defers_directory_sync_to_the_outer_barrier(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);canonical=root/'part';canonical.write_bytes(b'exact');alias=root/'part.transferring';os.link(canonical,alias)
            with patch('hf_flow_io.os.fsync') as sync:
                recover_published_transfer(canonical,checksum(canonical),[alias],durable=False)
                sync.assert_not_called()
            self.assertEqual(canonical.read_bytes(),b'exact');self.assertFalse(alias.exists())

    def test_conflicting_scratch_is_preserved_before_any_alias_is_removed(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);canonical=root/'part';canonical.write_bytes(b'exact')
            known=root/'part.transferring';os.link(canonical,known)
            conflict=root/'incoming';conflict.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'scratch differs'):
                recover_published_transfer(canonical,checksum(canonical),[known,conflict])
            self.assertTrue(known.exists());self.assertEqual(conflict.read_bytes(),b'changed')
            link=root/'symlink';link.symlink_to(canonical)
            with self.assertRaisesRegex(ValueError,'scratch alias'):
                recover_published_transfer(canonical,checksum(canonical),[link])
            self.assertTrue(link.is_symlink())

class PreStagedTransport(unittest.TestCase):
    def prepared(self, root):
        source=root/'source';directory=source/'outputs/g/jobs/1-2';directory.mkdir(parents=True)
        part=directory/'parts-0000.parquet';part.write_bytes(b'closed accepted transport bytes')
        validation=directory/'validation.json';write_json(validation,{'within_error_budget':True})
        receipt=dict(job_id='1-2',generation='g',parts=[dict(path=str(part),sha256=checksum(part))],aggregate_parts=[],validation=str(validation),validation_sha256=checksum(validation))
        original=directory/'receipt.json';write_json(original,receipt)
        value=dict(receipt=receipt,path=str(original),sha256=checksum(original))
        certificate=seal(source,{str(part.relative_to(source)):dict(bytes=part.stat().st_size,sha256=checksum(part))})
        staged=root/'incoming'/('a'*24);shutil.copytree(source,staged)
        payload=dict(id='a'*24,role='web',values=[value],certificate=certificate,pre_staged=True,
                     rsync_source=dict(output_root=str(source),rsync_transport=dict(node_stage_base=str(root/'incoming'))))
        cfg=dict(role='node',generation='g',node_flow_root=str(root/'flow'),node_bulk=str(root/'canonical'),reserve_bytes=0,slots=1,inflight_bytes_per_slot=0,copy_file_reserve_bytes=2**20)
        return payload,cfg,part,staged/part.relative_to(source)

    def test_pre_staged_copy_verifies_every_byte_without_a_scheduler(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);payload,cfg,source,staged=self.prepared(root)
            with patch.dict('sys.modules',{'dask':None,'distributed':None,'fsspec':None}):
                report=_copy_batch(payload,cfg)
            self.assertTrue(report['all_exact']);self.assertTrue(report['destination_proof']['all_exact'])
            canonical=Path(report['jobs'][0]['receipt']['parts'][0]['path'])
            self.assertEqual(canonical.read_bytes(),source.read_bytes())
            self.assertFalse(staged.exists());self.assertEqual(canonical.stat().st_nlink,1)
            self.assertTrue(source.exists())

    @unittest.skipUnless(Path('/dev/shm').is_dir(),'Linux second filesystem required')
    def test_cross_device_copy_retains_staging_until_separate_verified_retirement(self):
        with tempfile.TemporaryDirectory() as d,tempfile.TemporaryDirectory(dir='/dev/shm') as other:
            payload,cfg,source,staged=self.prepared(Path(d));cfg['node_bulk']=str(Path(other)/'canonical')
            self.assertNotEqual(staged.stat().st_dev,Path(other).stat().st_dev)
            report=_copy_batch(payload,cfg)
            self.assertTrue(report['all_exact']);self.assertTrue(staged.exists())
            canonical=Path(report['jobs'][0]['receipt']['parts'][0]['path'])
            self.assertEqual(canonical.read_bytes(),source.read_bytes())
            self.assertNotEqual(canonical.stat().st_ino,staged.stat().st_ino)

    @unittest.skipUnless(Path('/dev/shm').is_dir(),'Linux second filesystem required')
    def test_corrupt_cross_device_copy_keeps_staging_and_publishes_no_proof(self):
        with tempfile.TemporaryDirectory() as d,tempfile.TemporaryDirectory(dir='/dev/shm') as other:
            payload,cfg,source,staged=self.prepared(Path(d));cfg['node_bulk']=str(Path(other)/'canonical')
            original=shutil.copyfile
            def corrupt(src,dst,*args,**kwargs):
                result=original(src,dst,*args,**kwargs)
                if str(src).endswith('.parquet'):Path(dst).write_bytes(b'corrupt copy')
                return result
            with patch('hf_flow_io.shutil.copyfile',side_effect=corrupt):
                with self.assertRaises(ValueError):_copy_batch(payload,cfg)
            self.assertTrue(staged.exists());self.assertTrue(source.exists())
            self.assertFalse((Path(cfg['node_flow_root'])/'transfers'/payload['id']/'verified.json').exists())

    def test_missing_required_storage_mount_stops_before_publication(self):
        with tempfile.TemporaryDirectory() as d:
            payload,cfg,source,staged=self.prepared(Path(d));cfg['node_storage_mount']=str(Path(d))
            with self.assertRaisesRegex(ValueError,'mount'):_copy_batch(payload,cfg)
            self.assertTrue(staged.exists());self.assertFalse(Path(cfg['node_bulk']).exists())

    def test_missing_required_staging_mount_stops_before_publication(self):
        with tempfile.TemporaryDirectory() as d:
            payload,cfg,source,staged=self.prepared(Path(d))
            cfg['node_staging_mounts']={'web':str(Path(d)/'incoming')}
            with self.assertRaisesRegex(ValueError,'staging mount'):_copy_batch(payload,cfg)
            self.assertTrue(staged.exists());self.assertFalse(Path(cfg['node_bulk']).exists())

    def test_same_device_cross_mount_link_failure_uses_copy_and_keeps_stage(self):
        import errno
        with tempfile.TemporaryDirectory() as d:
            payload,cfg,source,staged=self.prepared(Path(d));original=os.link
            def link(src,dst,*args,**kwargs):
                if str(src).startswith(str(Path(d)/'incoming')):raise OSError(errno.EXDEV,'different mount')
                return original(src,dst,*args,**kwargs)
            with patch('hf_flow_io.os.link',side_effect=link):report=_copy_batch(payload,cfg)
            self.assertTrue(report['all_exact']);self.assertTrue(staged.exists())

    @unittest.skipUnless(os.environ.get('TEST_HF_STORAGE_ROOT'),'explicit existing bind-mount fixture required')
    def test_verified_bind_alias_uses_one_mount_for_publication(self):
        base=Path(os.environ['TEST_HF_STORAGE_ROOT']);logical=base/'rsync-incoming/web';physical=base/'storage/rsync-incoming/web'
        with tempfile.TemporaryDirectory(dir=logical) as d,tempfile.TemporaryDirectory(dir=base/'storage/tests/tmp') as target:
            payload,cfg,source,staged=self.prepared(Path(d));cfg['node_bulk']=str(Path(target)/'canonical');cfg['node_storage_mount']=str(base/'storage')
            cfg['node_staging_sources']={'web':str(physical/Path(d).name/'incoming')}
            self.assertTrue(Path(cfg['node_staging_sources']['web']).samefile(Path(d)/'incoming'))
            correct=cfg['node_staging_sources']['web'];cfg['node_staging_sources']['web']=target
            with self.assertRaisesRegex(ValueError,'Physical staging alias'):_copy_batch(payload,cfg)
            self.assertTrue(staged.exists());cfg['node_staging_sources']['web']=correct
            report=_copy_batch(payload,cfg)
            self.assertTrue(report['all_exact']);self.assertFalse(staged.exists())

    def test_failed_batch_sync_never_publishes_destination_proof(self):
        with tempfile.TemporaryDirectory() as d:
            payload,cfg,source,staged=self.prepared(Path(d))
            with patch('hf_flow_io.subprocess.run',side_effect=RuntimeError('sync failed')):
                with self.assertRaisesRegex(RuntimeError,'sync failed'):_copy_batch(payload,cfg)
            self.assertTrue(source.exists())
            self.assertFalse((Path(cfg['node_flow_root'])/'transfers'/payload['id']/'verified.json').exists())

    def test_bulk_sync_finishes_before_destination_proof(self):
        from hf_flow_artifacts import verify_destination
        with tempfile.TemporaryDirectory() as d:
            payload,cfg,source,staged=self.prepared(Path(d));synchronized=[]
            def sync(args,**kwargs):
                self.assertEqual(args[0],'/usr/bin/sync')
                self.assertNotIn('--file-system',args)
                self.assertTrue(all(str(path).startswith(str(Path(d))) for path in args[1:]))
                self.assertTrue((Path(cfg['node_bulk'])/'outputs/g/jobs/1-2/receipt.json').is_file())
                synchronized.append(True)
            def proof(*args,**kwargs):
                self.assertTrue(synchronized,'Destination proof preceded durable batch sync')
                return verify_destination(*args,**kwargs)
            with patch('hf_flow_io.subprocess.run',side_effect=sync),patch('hf_flow_io.verify_destination',side_effect=proof):
                self.assertTrue(_copy_batch(payload,cfg)['all_exact'])

    def test_pre_staged_corruption_and_missing_files_never_publish_a_proof(self):
        for missing in [False,True]:
            with self.subTest(missing=missing),tempfile.TemporaryDirectory() as d:
                root=Path(d);payload,cfg,source,staged=self.prepared(root)
                if missing:staged.unlink()
                else:staged.write_bytes(b'corrupt transport')
                with self.assertRaises(ValueError):_copy_batch(payload,cfg)
                self.assertTrue(source.exists())
                self.assertFalse((Path(cfg['node_flow_root'])/'transfers'/payload['id']/'verified.json').exists())

if __name__=='__main__':unittest.main()
