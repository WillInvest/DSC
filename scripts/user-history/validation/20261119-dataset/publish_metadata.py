"""Publish explicitly named, mutable navigation metadata with rclone copy/check.
Scientific payloads and their immutable archive identities are never selected.
"""
import json,subprocess,time
from pathlib import Path
from shared.artifacts import checksum,write_json
from hf_flow_cloud import _common,_readback
root=Path('/home/node/aave-research-dataset')
names=['README.md','AGENTS.md','CLAUDE.md','VARIABLE_DICTIONARY.md','DATA_CATALOG.csv','01_HF_Vectors/CANONICAL_JOBS.csv']
names += ['01_HF_Vectors/dictionaries/'+x for x in ['users.parquet','dataset.json','reserves.csv','token_contracts.csv']]
names += ['03_Code_and_Definitions/'+x for x in ['read_archive.py','rebuild_account.py','NUMERICAL_DEFINITION.json']]
proofs=['accounting-source-crosscheck.json','archive-reader-real-data.json','job-catalog-snapshot.json','raw-event-node-audit-20261119.json','rebuilt-account-22434005.json','reserve-symbol-dictionary.json','small-cloud-accepted.json','token-initialization-coverage.json','goal-progress-20261119.json','collection-events.jsonl','dataset-status.json']
proofs += [x+'-accepted.json' for x in ['token_events','pool_events','configuration','oracle_prices','pinned_scientific_code','replay-bundle']]
proofs += ['delivered-bundle-replay-proof.json','canonical-archive-audit.json',
           'vector-reader-sample-22434005.json']
proofs += ['hf-'+x+'-node-primary.json' for x in ['1c7bff15e4d744a29591','1e9b38866259bf82060b','247f9896cc1d99917abbb330','95b56cee0bd19fa0d445463a']]
proofs += ['hf-'+x+'-cloud-accepted.json' for x in ['247f9896cc1d99917abbb330','95b56cee0bd19fa0d445463a','primary-retention-56903b5d63ef028b','3c4e71fb0df22139098dc66d']]
names += ['04_Validation_and_Provenance/'+x for x in proofs if (root/'04_Validation_and_Provenance'/x).exists()]
files={n:dict(bytes=(root/n).stat().st_size,sha256=checksum(root/n)) for n in names}
ops=root/'.metadata-publication';ops.mkdir(exist_ok=True)
listing=ops/'files.txt';listing.write_text('\n'.join(sorted(files))+'\n')
remote='dsc_hf:AaveV3_Ethereum'
common=_common()+['--fast-list','--tpslimit','2','--tpslimit-burst','2','--drive-chunk-size','128M']
log=['--log-file',str(ops/'rclone.log'),'--log-level','INFO']
# These are selected reader documents/catalogs, which must reflect current status.
# No sync/delete and no scientific data overwrite: only this exact list is used.
subprocess.run(common+['copy',str(root),remote,'--files-from',str(listing),'--checksum','--transfers','2','--checkers','2']+log,check=True,timeout=86400)
subprocess.run(common+['check',str(root),remote,'--files-from',str(listing),'--one-way','--checkers','2']+log,check=True,timeout=86400)
checkpoint=ops/'sha-readback-checkpoint.json'
prior={}
for receipt in [ops/'complete.json',checkpoint]:
    if receipt.is_file():
        previous=json.loads(receipt.read_text())
        if previous.get('remote')==remote:
            prior.update(previous.get('sha_readbacks',{}))
probes={}
for name,value in files.items():
    if checksum(root/name)!=value['sha256']:raise ValueError('Metadata source changed during publication')
    saved=prior.get(name)
    if saved and saved.get('exact') is True and all(saved.get(k)==v for k,v in value.items()):
        # The fresh Drive size/MD5 comparison above binds unchanged bytes
        # to their previous exact SHA readback.
        probes[name]=saved
    else:
        probes[name]=_readback(remote+'/'+name,value)
    write_json(checkpoint,dict(remote=remote,sha_readbacks={**prior,**probes}))
    write_json(ops/'progress.json',dict(at=time.time(),files=len(files),sha_readbacks_completed=len(probes)))
write_json(ops/'complete.json',dict(completed_at=time.time(),files=files,remote=remote,all_remote_size_md5_exact=True,all_remote_sha256_exact=True,sha_readbacks=probes,sharing_changed=False,scientific_payloads_unchanged=True))
print('Readable metadata published and full SHA verified',len(files),flush=True)
