"""Package only frozen manifest members; read back every byte and link declaration."""
import argparse,hashlib,json,os,tarfile,time
from pathlib import Path
from shared.artifacts import checksum,write_json


def package(image,expected_manifest_sha,output):
 image=Path(image);output=Path(output);manifest=image/'snapshot-manifest.json'
 if checksum(manifest)!=expected_manifest_sha:raise ValueError('Wrong input image')
 spec=json.loads(manifest.read_text());expected=dict(spec['files']);expected['snapshot-manifest.json']=expected_manifest_sha;links=spec['symlinks'];names=sorted(set(expected)|set(links))
 for name in names:
  p=Path(name)
  if p.is_absolute() or '..' in p.parts or '.ssh' in p.parts or name.startswith('worker-state/private/') or name.endswith('/dsc-hf.conf'):raise ValueError('Unreviewed/private image member')
 if set(expected)&set(links):raise ValueError('Ambiguous image member')
 temp=output.with_name(output.name+'.incoming')
 if output.exists() or temp.exists():raise FileExistsError('Preserve existing package')
 output.parent.mkdir(parents=True,exist_ok=True)
 with tarfile.open(temp,'x:gz',compresslevel=3,dereference=False) as writer:
  for name in names:
   p=image/name
   if name in expected and (p.is_symlink() or not p.is_file()):raise ValueError('Expected regular image member')
   if name in links and (not p.is_symlink() or os.readlink(p)!=links[name]):raise ValueError('Image symlink changed')
   writer.add(p,arcname=name,recursive=False)
 seen={};seen_links={}
 with tarfile.open(temp,'r|gz') as reader:
  for member in reader:
   name=member.name
   if name in seen or name in seen_links:raise ValueError('Duplicate package member')
   if member.issym():
    if name not in links or member.linkname!=links[name]:raise ValueError('Unexpected symlink')
    seen_links[name]=member.linkname
   elif member.islnk():
    if name not in expected or seen.get(member.linkname)!=expected[name]:raise ValueError('Unverified hardlink')
    seen[name]=expected[name]
   elif member.isfile():
    if name not in expected:raise ValueError('Unexpected package member')
    h=hashlib.sha256()
    with reader.extractfile(member) as stream:
     for block in iter(lambda:stream.read(2**20),b''):h.update(block)
    if h.hexdigest()!=expected[name]:raise ValueError('Packaged input hash differs')
    seen[name]=h.hexdigest()
   else:raise ValueError('Unsupported package member')
 if set(seen)!=set(expected) or seen_links!=links:raise ValueError('Missing package members')
 with temp.open('rb') as stream:os.fsync(stream.fileno())
 os.link(temp,output);temp.unlink()
 result=dict(created_at=time.time(),archive=str(output),archive_sha256=checksum(output),archive_bytes=output.stat().st_size,image_manifest_sha256=expected_manifest_sha,regular_files=len(seen),symlinks=len(links),all_member_hashes_exact=True,symlink_targets_not_followed=True,scope='Frozen reconstruction inputs and runtime only; source originals retained; no filesystem extraction performed')
 write_json(output.with_suffix(output.suffix+'.verified.json'),result);return result


if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--image',required=True);p.add_argument('--manifest-sha',required=True);p.add_argument('--output',required=True);a=p.parse_args();print(json.dumps(package(a.image,a.manifest_sha,a.output)),flush=True)
