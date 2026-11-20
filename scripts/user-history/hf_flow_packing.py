"""Lossless uncompressed TAR chunks with exact byte-range restore indexes."""
import fcntl
import hashlib
import json
import os
import re
import tarfile
import time
import uuid
from pathlib import Path

from hf_flow_artifacts import _publish
from shared.artifacts import digest

FORMAT = 'hf-flow-tar-index-v1'
CHUNK_FORMAT = 'hf-flow-tar-chunk-v1'


def _manifest(files):
    normalized={}
    for name,item in files.items():
        p=Path(name)
        if (not isinstance(name,str) or p.is_absolute() or '..' in p.parts
                or str(p)!=name or not p.parts or '\n' in name or '\r' in name):
            raise ValueError('Invalid archive-relative input path')
        size,sha=item['bytes'],item['sha256']
        if (not isinstance(size,int) or isinstance(size,bool) or size<0
                or not isinstance(sha,str) or re.fullmatch('[a-f0-9]{64}',sha) is None):
            raise ValueError('Invalid input byte identity')
        normalized[name]=dict(bytes=size,sha256=sha)
    if not normalized:raise ValueError('Empty archive manifest')
    return dict(sorted(normalized.items()))


def _source(base,name):
    path=base/name
    for parent in [path,*path.parents]:
        if parent==base:break
        if parent.is_symlink():raise ValueError('Symlink archive input')
    if not path.resolve().is_relative_to(base) or not path.is_file():
        raise ValueError('Missing/nonregular archive input')
    return path


def _stat(path):
    s=path.stat()
    return s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns


def _hashes(path):
    sha=hashlib.sha256();md5=hashlib.md5()
    with path.open('rb') as stream:
        for data in iter(lambda:stream.read(2**20),b''):
            sha.update(data);md5.update(data)
    return dict(bytes=path.stat().st_size,sha256=sha.hexdigest(),md5=md5.hexdigest())


def _read_members(path,entries,relative,verify_bytes):
    members={}
    with tarfile.open(path,'r:') as reader:
        for item in reader:
            if item.name in members or item.name not in entries or not item.isfile():
                raise ValueError('Unexpected/duplicate TAR member')
            expected=entries[item.name]
            if item.size!=expected['bytes']:raise ValueError('TAR member size differs')
            if verify_bytes:
                sha=hashlib.sha256()
                with reader.extractfile(item) as stream:
                    for data in iter(lambda:stream.read(2**20),b''):sha.update(data)
                if sha.hexdigest()!=expected['sha256']:raise ValueError('Archived input SHA differs')
            members[item.name]=dict(pack=relative,offset=item.offset_data,
                                    bytes=item.size,sha256=expected['sha256'])
    if set(members)!=set(entries):raise ValueError('Missing TAR members')
    return members


def _groups(files,target):
    groups=[];current={};size=0
    for name,item in files.items():
        if current and size+item['bytes']>target:
            groups.append(current);current={};size=0
        current[name]=item;size+=item['bytes']
    if current:groups.append(current)
    return groups


def _completed(path,receipt,entries,relative):
    if (receipt.get('format')!=CHUNK_FORMAT or receipt.get('files')!=entries
            or receipt.get('pack')!=relative or receipt.get('files_sha256')!=digest(entries)):
        raise ValueError('Completed chunk manifest conflicts')
    before=_stat(path)
    hashes=_hashes(path)
    if hashes!=receipt['hashes'] or _stat(path)!=before:
        raise ValueError('Completed TAR bytes changed')
    # Whole-TAR digest binds the previously verified bytes. Checking actual
    # headers additionally binds the saved offsets used for ranged restoration.
    members=_read_members(path,entries,relative,False)
    if members!=receipt['members']:raise ValueError('Saved TAR offsets differ')
    return receipt


def _build(base,path,entries,relative,prior=None):
    incoming=path.with_name(path.name+'.incoming')
    if incoming.exists() or incoming.is_symlink():
        if incoming.is_symlink():raise ValueError('Symlink interrupted archive')
        incoming.rename(incoming.with_name(incoming.name+'.interrupted-'+uuid.uuid4().hex))
    with tarfile.open(incoming,'x:',format=tarfile.PAX_FORMAT) as writer:
        for name,expected in entries.items():
            source=_source(base,name);before=_stat(source)
            if before[2]!=expected['bytes']:raise ValueError('Input size changed')
            info=writer.gettarinfo(str(source),arcname=name)
            if not info.isfile():raise ValueError('Input is not a regular file')
            # Payload bytes carry all analytical precision. Stable headers
            # make a resumed chunk independent of host owner/time metadata.
            info.uid=info.gid=0;info.uname=info.gname='';info.mtime=0;info.mode=0o600
            with source.open('rb') as stream:writer.addfile(info,stream)
            if _stat(source)!=before:raise ValueError('Input changed while packing')
    with incoming.open('rb') as stream:os.fsync(stream.fileno())
    members=_read_members(incoming,entries,relative,True)
    hashes=_hashes(incoming)
    if prior is not None and (prior['hashes']!=hashes or prior['members']!=members):
        raise ValueError('Rebuilt immutable chunk differs')
    # Exclusive publication preserves any concurrently created or conflicting
    # final object. An interrupted final without receipt is adopted below.
    os.link(incoming,path);incoming.unlink()
    fd=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)
    return dict(format=CHUNK_FORMAT,pack=relative,files=entries,
                files_sha256=digest(entries),hashes=hashes,members=members,
                all_member_hashes_exact=True,verified_at=time.time())


def _pack(base,files,directory,target_bytes):
    """Package a closed explicit manifest; originals are never removed here.

    ``target_bytes`` groups payload bytes; TAR headers add small overhead and a
    single oversized input occupies its own chunk. No compression is applied.
    """
    files=_manifest(files)
    if not isinstance(target_bytes,int) or isinstance(target_bytes,bool) or target_bytes<=0:
        raise ValueError('Positive TAR payload target required')
    base=Path(base)
    if base.is_symlink() or not base.is_dir():raise ValueError('Expected a real input base')
    base=base.resolve();directory=Path(directory)
    if directory.is_symlink():raise ValueError('Symlink package directory')
    directory.mkdir(parents=True,exist_ok=True);directory=directory.resolve()
    folder=directory/'packs'
    if folder.is_symlink():raise ValueError('Symlink package folder')
    folder.mkdir(exist_ok=True)
    index_path=directory/'pack-index.json'
    if index_path.is_symlink():raise ValueError('Symlink archive index')
    groups=_groups(files,target_bytes);receipts=[]
    saved=json.loads(index_path.read_text()) if index_path.exists() else None
    if saved is not None and (saved.get('format')!=FORMAT
            or saved.get('original_manifest_sha256')!=digest(files)
            or saved.get('target_bytes')!=target_bytes or saved.get('files')!=files
            or saved.get('all_member_hashes_exact') is not True):
        raise ValueError('Existing archive index conflicts')
    for number,entries in enumerate(groups):
        relative='packs/pack-'+str(number).zfill(4)+'.tar';path=directory/relative
        receipt_path=path.with_suffix('.verified.json')
        if path.is_symlink() or receipt_path.is_symlink():raise ValueError('Symlink chunk/receipt')
        prior=json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        if prior is not None and (prior.get('format')!=CHUNK_FORMAT or prior.get('all_member_hashes_exact') is not True
                or prior.get('files')!=entries or prior.get('files_sha256')!=digest(entries) or prior.get('pack')!=relative):
            raise ValueError('Preserved chunk receipt conflicts')
        if path.exists():
            if prior is None:
                members=_read_members(path,entries,relative,True);hashes=_hashes(path)
                current=dict(format=CHUNK_FORMAT,pack=relative,files=entries,files_sha256=digest(entries),hashes=hashes,members=members,all_member_hashes_exact=True,verified_at=time.time())
                _publish(receipt_path,current)
            else:current=_completed(path,prior,entries,relative)
        else:
            if saved is not None:raise ValueError('Final indexed package missing')
            current=_build(base,path,entries,relative,prior)
            if prior is None:_publish(receipt_path,current)
            else:current=prior
        receipts.append(current)
    members={name:item for r in receipts for name,item in r['members'].items()}
    packs={r['pack']:r['hashes'] for r in receipts}
    if saved is not None:
        if saved.get('members')!=members or saved.get('packs')!=packs:
            raise ValueError('Final archive index bytes/offsets changed')
        return saved
    result=dict(format=FORMAT,original_manifest_sha256=digest(files),files=files,
                members=members,packs=packs,target_bytes=target_bytes,
                index_path=str(index_path),all_member_hashes_exact=True,
                created_at=time.time())
    _publish(index_path,result)
    return result


def pack(base,files,directory,target_bytes=2_000_000_000):
    """Serialize packaging of one immutable closed batch; never retire inputs."""
    directory=Path(directory)
    if directory.is_symlink():raise ValueError('Symlink package directory')
    directory.mkdir(parents=True,exist_ok=True)
    lock=directory/'pack.lock'
    if lock.is_symlink():raise ValueError('Symlink package lock')
    with lock.open('a') as stream:
        fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        return _pack(base,files,directory,target_bytes)
