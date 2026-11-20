import datetime,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
path=Path(__file__).parent/'validation/20261116-consolidation/reuse_node_proof.py';spec=importlib.util.spec_from_file_location('reuse_proof',path);r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
class ProofTests(unittest.TestCase):
 def test_modification_during_selection_cannot_reuse_finish_timestamp(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);copy=root/'copy';copy.write_bytes(b'accepted');sha=hashlib.sha256(copy.read_bytes()).hexdigest();anchor=copy.stat().st_ctime-1
   plan=dict(kind='consumed_source',role='revert',inventory_sha256='i',files=[dict(source='original',backup=str(copy),sha256=sha,bytes=8)])
   selection=dict(role='revert',inventory_sha256='i',created_at=anchor+100,prior_backup_receipt=dict(all_exact=True,verified_at=datetime.datetime.fromtimestamp(anchor-100,datetime.timezone.utc).isoformat()),verified_existing=[dict(path='copy',sha256=sha,kind='fresh_sha256')])
   encode=lambda x:json.dumps(x).encode()
   self.assertTrue(r.build_proof(encode(plan),encode(selection),root,anchor)['all_exact'])
   copy.write_bytes(b'changed!')
   self.assertFalse(r.build_proof(encode(plan),encode(selection),root,anchor)['all_exact'])
if __name__=='__main__':unittest.main()
