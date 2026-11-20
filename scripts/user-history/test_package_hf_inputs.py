import json,os,tempfile,unittest
from pathlib import Path
from shared.artifacts import checksum,write_json
from package_hf_inputs import package
class PackageTests(unittest.TestCase):
 def fixture(self,root):
  image=root/'image';image.mkdir();(image/'data').write_bytes(b'original input'*100);os.link(image/'data',image/'alias');os.symlink('data',image/'link')
  write_json(image/'snapshot-manifest.json',dict(files={n:checksum(image/n) for n in ['data','alias']},symlinks={'link':'data'}))
  return image,checksum(image/'snapshot-manifest.json')
 def test_exact_bytes_and_links_survive_package(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);image,sha=self.fixture(root);out=root/'input.tar.gz';r=package(image,sha,out);self.assertTrue(r['all_member_hashes_exact']);self.assertEqual(r['symlinks'],1);self.assertTrue((image/'data').exists())
   with self.assertRaises(FileExistsError):package(image,sha,out)
 def test_modified_source_never_gets_verified_package(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);image,sha=self.fixture(root);(image/'data').write_bytes(b'wrong source');out=root/'input.tar.gz'
   with self.assertRaises(ValueError):package(image,sha,out)
   self.assertFalse(out.exists())
if __name__=='__main__':unittest.main()
