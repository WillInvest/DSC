import hashlib,json,os,tempfile,unittest
from pathlib import Path
from shared.artifacts import checksum
from hf_flow_packing import pack

class PackingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name)/'source';self.base.mkdir()
        self.out=Path(self.tmp.name)/'archive'
        self.contents={'a.parquet':b'a'*1000,'nested/b.parquet':bytes(range(256))*5,'nested/c.json':b'{"proof":true}'}
        for name,data in self.contents.items():
            p=self.base/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
        self.files={n:dict(bytes=len(b),sha256=hashlib.sha256(b).hexdigest()) for n,b in self.contents.items()}

    def test_exact_ranges_across_multiple_packs_and_resume_after_source_retirement(self):
        index=pack(self.base,self.files,self.out,target_bytes=1500)
        self.assertGreater(len(index['packs']),1)
        for name,item in index['members'].items():
            with (self.out/item['pack']).open('rb') as f:
                f.seek(item['offset']);data=f.read(item['bytes'])
            self.assertEqual(data,self.contents[name])
        for p in self.base.rglob('*'):
            if p.is_file():p.unlink()
        again=pack(self.base,self.files,self.out,target_bytes=1500)
        self.assertEqual(again,index)
        self.assertEqual(len(list((self.out/'packs').glob('pack-*.tar'))),len(index['packs']))

    def test_wrong_source_sha_rejected_without_deleting_sources(self):
        wrong=dict(self.files);wrong['a.parquet']=dict(wrong['a.parquet'],sha256='0'*64)
        with self.assertRaises(ValueError):pack(self.base,wrong,self.out,target_bytes=1500)
        self.assertEqual((self.base/'a.parquet').read_bytes(),self.contents['a.parquet'])
        self.assertFalse((self.out/'pack-index.json').exists())

    def test_symlink_and_escape_rejected(self):
        (self.base/'link').symlink_to('a.parquet')
        for name in ['link','../source/a.parquet','/absolute']:
            with self.assertRaises(ValueError):pack(self.base,{name:self.files['a.parquet']},self.out)

    def test_interrupted_incoming_preserved_then_only_incomplete_pack_rebuilt(self):
        (self.out/'packs').mkdir(parents=True)
        incoming=self.out/'packs/pack-0000.tar.incoming';incoming.write_bytes(b'incomplete original bytes')
        index=pack(self.base,self.files,self.out,target_bytes=1500)
        quarantined=list((self.out/'packs').glob('pack-0000.tar.incoming.interrupted-*'))
        self.assertEqual(len(quarantined),1)
        self.assertEqual(quarantined[0].read_bytes(),b'incomplete original bytes')
        self.assertTrue(index['packs'])

    def test_changed_final_pack_and_conflicting_manifest_refused(self):
        index=pack(self.base,self.files,self.out,target_bytes=1500)
        item=next(iter(index['packs']));p=self.out/item
        raw=p.read_bytes();p.write_bytes(b'x'+raw[1:])
        with self.assertRaises(ValueError):pack(self.base,self.files,self.out,target_bytes=1500)
        self.assertEqual(p.read_bytes(),b'x'+raw[1:])
        wrong=dict(self.files);wrong['a.parquet']=dict(wrong['a.parquet'],bytes=12)
        with self.assertRaises(ValueError):pack(self.base,wrong,self.out,target_bytes=1500)

    def test_completed_chunks_reused_after_index_interruption(self):
        index=pack(self.base,self.files,self.out,target_bytes=1500)
        (self.out/'pack-index.json').unlink()
        first=self.out/next(iter(index['packs']));before=first.stat().st_ino
        for p in self.base.rglob('*'):
            if p.is_file():p.unlink()
        resumed=pack(self.base,self.files,self.out,target_bytes=1500)
        self.assertEqual(first.stat().st_ino,before)
        self.assertEqual(resumed['members'],index['members'])

    def test_pax_long_unicode_names_and_empty_members_have_exact_ranges(self):
        long='nested/'+'asset-'*35+'币.parquet'
        for name,data in [(long,b'unicode payload'),('empty.parquet',b'')]:
            path=self.base/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            self.files[name]=dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
            self.contents[name]=data
        index=pack(self.base,self.files,self.out,target_bytes=1500)
        for name,item in index['members'].items():
            with (self.out/item['pack']).open('rb') as stream:
                stream.seek(item['offset']);self.assertEqual(stream.read(item['bytes']),self.contents[name])

if __name__=='__main__':unittest.main()
