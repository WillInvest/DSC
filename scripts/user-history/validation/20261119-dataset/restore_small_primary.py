"""One-time manifest-scoped recovery; uses existing immutable copy/check and SHA verifier."""
import json, os, shutil, time
from pathlib import Path
from shared.artifacts import checksum, write_json
from backup_hf import verify
from hf_flow_cloud import _copy_and_check
root=Path('/home/node/hf-flow-20261116/storage/primary-archives')
proofs=json.loads(Path('/home/node/aave-research-dataset/04_Validation_and_Provenance/small-cloud-accepted.json').read_text())
source=Path('/home/node/hf-f64-v2-20261116/bulk')
for batch in ['1c7bff15e4d744a29591','1e9b38866259bf82060b','247f9896cc1d99917abbb330']:
    report=proofs[batch]
    if report.get('chunks'):
        accepted=report['chunks'][0]
        relative=('manifest.json' if batch.startswith('1c7') else 'metadata/direct-archive/'+batch+'-0/manifest.json')
        manifest=(source/'archive-publish'/ (batch+'-0')/'files/manifest.json' if batch.startswith('1c7') else source/relative)
        expected=accepted['source_sha256_manifest']
    else:
        accepted=report
        relative='hf-flow-20261116/cloud/'+batch+'/manifest.json'
        manifest=Path('/home/node/hf-flow-20261116/cloud')/batch/'manifest.json'
        expected=accepted['manifest_sha256']
    if checksum(manifest)!=expected or accepted['all_remote_size_md5_exact'] is not True:
        raise ValueError('Accepted raw manifest identity differs')
    value=json.loads(manifest.read_text())
    files=dict(value['files']);files[relative]=dict(bytes=manifest.stat().st_size,sha256=expected)
    target=root/batch;target.mkdir(parents=True,exist_ok=True)
    ops=target/'.primary-restore';ops.mkdir(exist_ok=True)
    if (ops/'complete.json').exists():
        old=json.loads((ops/'complete.json').read_text())
        if old['manifest_sha256']!=expected:raise ValueError('Completed restore identity differs')
        continue
    if accepted.get('chunks'): raise ValueError('Unexpected nested chunks')
    if batch.startswith(('1c7','1e9')):
        verify(source,str(manifest.relative_to(source)),expected,report_path=ops/'original-sha256-verified.json')
        for name in files:
            src=manifest if name==relative else source/name
            dst=target/name;dst.parent.mkdir(parents=True,exist_ok=True)
            if dst.exists():
                if checksum(dst)!=files[name]['sha256']:raise ValueError('Destination conflict')
            else: shutil.copy2(src,dst)
    else:
        _copy_and_check(accepted['remote'],str(target),files,ops)
    check=ops/'verification-manifest.json'
    write_json(check,dict(role='node',files=files,new_rows=0,recovered_rows=0))
    verify(target,str(check.relative_to(target)),checksum(check),report_path=ops/'sha256-verified.json')
    write_json(ops/'complete.json',dict(batch=batch,archive_format='raw-files',manifest_sha256=expected,manifest_relative=relative,remote=accepted['remote'],files=len(files),bytes=sum(x['bytes'] for x in files.values()),all_local_sha256_exact=True,cloud_previously_accepted=True,production_coverage_increment=0,completed_at=time.time()))
    print(batch,'accepted',len(files),flush=True)
