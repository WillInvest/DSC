import hashlib,json,subprocess,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from hf_flow_cloud import upload_and_retire,restore_jobs
from hf_flow_packing import pack,FORMAT
from hf_flow_queue import LocalLedger
from shared.artifacts import checksum,write_json


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.base=self.root/'node';self.base.mkdir();self.remote=self.root/'drive';self.remote.mkdir()
        self.cfg=dict(node_home=str(self.base),node_flow_root=str(self.base/'flow'),cloud_remote='fake:root',generation='a'*64,cloud_transport='packed',cloud_pack_bytes=500,roles={'node':{'local_root':str(self.base/'local'),'reserve_bytes':4096,'slots':1,'inflight_bytes_per_slot':4096}})
        self.directory=self.base/'outputs'/self.cfg['generation']/'jobs'/'100-102';self.directory.mkdir(parents=True)
        parts=[]
        for name,data in [('parts-0000.parquet',b'precise-user\x00'*100),('aggregate_parts-0000.parquet',b'precise-reserve\xff'*70)]:
            p=self.directory/name;p.write_bytes(data);parts.append(dict(path=str(p),sha256=checksum(p)))
        self.original={p['path']:Path(p['path']).read_bytes() for p in parts}
        v=self.directory/'validation.json';write_json(v,{'exact':True})
        receipt=dict(job_id='100-102',parts=parts[:1],aggregate_parts=parts[1:],validation=str(v),validation_sha256=checksum(v),bytes=sum(len(b) for b in self.original.values()))
        r=self.directory/'receipt.json';write_json(r,receipt)
        self.value=dict(receipt=receipt,path=str(r),sha256=checksum(r));self.user=self.base/'users.parquet';self.user.write_bytes(b'address dictionary')
        self.payload=dict(id='packed-proof',values=[self.value],job_receipts=[['100-102',self.value['sha256']]],metadata={'users.parquet':dict(bytes=self.user.stat().st_size,sha256=checksum(self.user))})
        self.calls=[];self.cleanup_requests=[]
        with LocalLedger(self.base/'local/local.sqlite',self.cfg['generation']) as ledger:ledger.commit('100-102',self.value)

    def remote_path(self,text):
        return self.remote/text.removeprefix('fake:root/').removeprefix('fake:root')

    def rclone_run(self,args,**kw):
        self.calls.append(args);verb=next(x for x in ['copy','check'] if x in args);i=args.index(verb);source=self.remote_path(args[i+1]) if args[i+1].startswith('fake:') else Path(args[i+1]);dest=self.remote_path(args[i+2]) if args[i+2].startswith('fake:') else Path(args[i+2]);listing=Path(args[args.index('--files-from')+1]).read_text().splitlines()
        for name in listing:
            src=source/name;dst=dest/name
            if verb=='copy':
                dst.parent.mkdir(parents=True,exist_ok=True)
                if dst.exists() and dst.read_bytes()!=src.read_bytes():raise ValueError('immutable cloud conflict')
                dst.write_bytes(src.read_bytes())
            elif src.read_bytes()!=dst.read_bytes():raise ValueError('cloud checksum mismatch')
        return subprocess.CompletedProcess(args,0)

    def popen(self,args,**kw):
        self.calls.append(args);i=args.index('cat');path=self.remote_path(args[i+1]);f=path.open('rb')
        offset=int(args[args.index('--offset')+1]) if '--offset' in args else 0
        count=int(args[args.index('--count')+1]) if '--count' in args else None
        import io
        f.seek(offset);data=f.read() if count is None else f.read(count);f.close()
        class Process:
            def __init__(self):self.stdout=io.BytesIO(data)
            def __enter__(self):return self
            def __exit__(self,*a):self.stdout.close()
            def wait(self):return 0
        return Process()

    def retire(self,cert,proof,request):
        self.cleanup_requests.append(request)
        # Tests use a disposable root; production root policy/FD checks are
        # exercised separately by the artifact retirement tests.
        self.assertTrue(proof['all_remote_size_md5_exact'])
        for n,item in cert['files'].items():
            p=Path(cert['root'])/n
            if p.exists():self.assertEqual(checksum(p),item['sha256']);p.unlink()
        return {'removed_files':len(cert['files'])}

    def test_packed_upload_restore_and_completed_restart_without_sources(self):
        with LocalLedger(self.base/'local/local.sqlite',self.cfg['generation']) as db:db.commit('100-102',self.value)
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',self.popen),patch('hf_flow_cloud._retire',self.retire):
            report=upload_and_retire(self.payload,self.cfg)
            self.assertEqual(report['archive_format'],FORMAT)
            self.assertTrue(report['node_result_parquets_retired']);self.assertTrue(report['node_tar_packages_retired'])
            self.assertTrue(all(not Path(p).exists() for p in self.original))
            self.assertFalse(list((self.base/'flow/cloud/packed-proof/packs').glob('*.tar')))
            cloud_files=[p for p in self.remote.rglob('*') if p.is_file()]
            self.assertTrue(any(p.suffix=='.tar' for p in cloud_files));self.assertFalse(any(p.suffix=='.parquet' for p in cloud_files))
            self.assertEqual(len(report['restore_probes']),3)
            before=len(self.calls);self.assertEqual(upload_and_retire(self.payload,self.cfg),report);self.assertEqual(len(self.calls),before)
            restore_jobs(dict(id='restore',values=[self.value],remote=report['remote'],archive_format=report['archive_format'],index_sha256=report['index_sha256']),self.cfg)
        for path,data in self.original.items():self.assertEqual(Path(path).read_bytes(),data)
        with LocalLedger(self.base/'local/local.sqlite',self.cfg['generation']) as db:self.assertEqual(db.buffered_bytes(),0)

    def test_interrupted_package_cleanup_resumes_without_reopening_retired_sources(self):
        failed=[False]
        def interrupted(cert,proof,request):
            if request.name=='packages.cleanup.json' and not failed[0]:
                failed[0]=True;raise OSError('interrupted package cleanup')
            return self.retire(cert,proof,request)
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',self.popen),patch('hf_flow_cloud._retire',interrupted):
            with self.assertRaises(OSError):upload_and_retire(self.payload,self.cfg)
            self.assertTrue(all(not Path(p).exists() for p in self.original))
            before=len(self.calls);report=upload_and_retire(self.payload,self.cfg)
            self.assertTrue(report['node_tar_packages_retired']);self.assertEqual(len(self.calls),before)

    def test_primary_archive_retention_keeps_exact_local_packs_and_restart_skips_upload(self):
        primary=self.base/'primary-archives'
        self.cfg.update(retain_node_archives=True,node_archive_root=str(primary))
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',self.popen),patch('hf_flow_cloud._retire',self.retire):
            report=upload_and_retire(self.payload,self.cfg)
            self.assertTrue(report['node_result_parquets_retired'])
            self.assertTrue(report['node_tar_packages_retained'])
            self.assertFalse(report['node_tar_packages_retired'])
            self.assertEqual(len(self.cleanup_requests),1)
            self.assertTrue(self.cleanup_requests[0].is_relative_to(self.base/'flow/cloud'))
            index_path=Path(report['index_path'])
            self.assertTrue(index_path.is_relative_to(primary))
            index=json.loads(index_path.read_text())
            for name,item in index['packs'].items():
                path=index_path.parent/name
                self.assertEqual(path.stat().st_size,item['bytes'])
                self.assertEqual(checksum(path),item['sha256'])
            before=len(self.calls)
            self.assertEqual(upload_and_retire(self.payload,self.cfg),report)
            self.assertEqual(len(self.calls),before)
        self.assertTrue(all(not Path(p).exists() for p in self.original))

    def test_retention_rejects_raw_before_mutating_any_files(self):
        self.cfg.update(retain_node_archives=True,cloud_transport='raw')
        with self.assertRaisesRegex(ValueError,'packed transport'):
            upload_and_retire(self.payload,self.cfg)
        self.assertTrue(all(Path(p).exists() for p in self.original))

    def test_primary_archive_root_cannot_escape_node_home(self):
        self.cfg.update(retain_node_archives=True,node_archive_root=str(self.root/'outside'))
        with self.assertRaisesRegex(ValueError,'inside the node home'):
            upload_and_retire(self.payload,self.cfg)
        self.assertFalse((self.root/'outside').exists())
        self.assertTrue(all(Path(p).exists() for p in self.original))

    def test_cloud_only_batch_restores_a_pinned_node_primary_without_reupload(self):
        import restore_primary_hf
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',self.popen),patch('hf_flow_cloud._retire',self.retire):
            report=upload_and_retire(self.payload,self.cfg)
            primary=self.base/'primary'
            with patch.object(restore_primary_hf,'ROOT',primary),patch.object(restore_primary_hf,'OLD',self.base/'flow/cloud'):
                restore_primary_hf.run(self.payload['id'])
                done=primary/self.payload['id']/'.primary-restore/complete.json'
                restored=json.loads(done.read_text())
                self.assertTrue(restored['all_local_sha256_exact'])
                self.assertEqual(restored['production_coverage_increment'],0)
                before=len(self.calls)
                restore_primary_hf.run(self.payload['id'])
                self.assertEqual(len(self.calls),before)

    def test_bulk_backup_sha_proof_does_not_overwrite_another_backup_receipt(self):
        from backup_hf import verify
        existing=self.base/'backup-verified.json';existing.write_bytes(b'previous independently verified backup')
        manifest=self.base/'scoped-manifest.json'
        write_json(manifest,dict(role='node',files={'users.parquet':dict(bytes=self.user.stat().st_size,sha256=checksum(self.user))},new_rows=0,recovered_rows=0))
        proof=self.base/'scoped/proof.json'
        verify(self.base,manifest.name,checksum(manifest),report_path=proof,max_workers=2)
        self.assertTrue(json.loads(proof.read_text())['all_exact'])
        self.assertEqual(existing.read_bytes(),b'previous independently verified backup')
        self.user.write_bytes(b'corrupt bytes')
        with self.assertRaises(ValueError):
            verify(self.base,manifest.name,checksum(manifest),report_path=self.base/'failed-proof.json')
        self.assertFalse((self.base/'failed-proof.json').exists())

    def test_legacy_raw_batch_remains_restorable_and_complete_retries_repair_local_journal(self):
        self.cfg['cloud_transport']='raw'
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',self.popen),patch('hf_flow_cloud._retire',self.retire):
            report=upload_and_retire(self.payload,self.cfg)
            restore_jobs(dict(id='raw-restore',values=[self.value],remote=report['remote']),self.cfg)
            with LocalLedger(self.base/'local/local.sqlite',self.cfg['generation']) as ledger:
                ledger.db.execute('delete from retired');ledger.db.commit()
                self.assertGreater(ledger.buffered_bytes(),0)
            upload_and_retire(self.payload,self.cfg)
        with LocalLedger(self.base/'local/local.sqlite',self.cfg['generation']) as ledger:self.assertEqual(ledger.buffered_bytes(),0)
        for path,data in self.original.items():self.assertEqual(Path(path).read_bytes(),data)

    def test_failed_cloud_read_is_retryable_without_retiring_sources(self):
        def interrupted(args,**kwargs):
            child=self.popen(args,**kwargs)
            child.wait=lambda:1
            return child
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',interrupted),patch('hf_flow_cloud._retire',self.retire):
            with self.assertRaises(subprocess.CalledProcessError):upload_and_retire(self.payload,self.cfg)
        self.assertTrue(all(Path(p).exists() for p in self.original))

    def test_insufficient_pack_space_preserves_sources_without_creating_archives(self):
        import types
        with patch('hf_flow_io.shutil.disk_usage',return_value=types.SimpleNamespace(free=1)):
            with self.assertRaises(OSError):upload_and_retire(self.payload,self.cfg)
        self.assertTrue(all(Path(p).read_bytes()==data for p,data in self.original.items()))
        self.assertFalse(list((self.base/'flow/cloud/packed-proof').glob('packs/*.tar')))
        self.assertEqual(self.calls,[])

    def test_changed_remote_index_rejected_before_writing_a_result(self):
        with patch('hf_flow_cloud.subprocess.run',self.rclone_run),patch('hf_flow_cloud.subprocess.Popen',self.popen),patch('hf_flow_cloud._retire',self.retire):
            report=upload_and_retire(self.payload,self.cfg)
            index=self.remote_path(report['remote']+'/pack-index.json');index.write_bytes(index.read_bytes()+b' ')
            with self.assertRaises(ValueError):restore_jobs(dict(id='restore',values=[self.value],remote=report['remote'],archive_format=FORMAT,index_sha256=report['index_sha256']),self.cfg)
        self.assertTrue(all(not Path(p).exists() for p in self.original))

if __name__=='__main__':unittest.main()
