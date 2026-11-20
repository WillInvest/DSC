"""Build the readable catalog from explicit accepted receipts, not file presence."""
import csv,json,shutil,time
from pathlib import Path
from shared.artifacts import checksum,write_json
r=Path('/home/node/aave-research-dataset');flow=Path('/home/node/hf-flow-20261116')
rows=[]
drive_folders={
 'HF archive 1c7bff15e4d744a29591':'1Zbok470b4z8mm0VP7rsFTyvttK_paTi1',
 'HF archive 1e9b38866259bf82060b':'19UJyaZTiBPaSaPXw7ilR7Wc9vq9JGVyy',
 'HF archive 247f9896cc1d99917abbb330':'1rqua8-xwIgoikRyoZV9AIA10yDVNrXdT',
 'HF archive 95b56cee0bd19fa0d445463a':'1R4W_wKnwZ1BBJyoua041sFu2qfbIQYxN',
 'HF archive primary-retention-56903b5d63ef028b':'1soo7IiWkEx_jCiKz25lDp3uuRIQMVvpn',
 'HF archive 3c4e71fb0df22139098dc66d':'1YAmyilvsNmno8NN-Ly4y-IJaqrSU2op9',
 'token_events':'11CL9rk5UYMWrBo43pMns5dcqaWbld9Y_',
 'pool_events':'1QTQBN3FdXkMm-r915a8fnzAO4r247GtD',
 'configuration':'1qcKh6v9cOEMo6qy2KEGnHo_TDYg5rkdo',
 'oracle_prices':'1MqQnfWPzppbrw6YN52ZKys6ogViYpZT6',
 'pinned_scientific_code':'1r2qd2N02Du2GHduXYI_8d5iEf2PZ6XzN',
}
def add(component,status,fmt,node,remote,index='',manifest='',nbytes=0,notes=''):
 url=('https://drive.google.com/file/d/1xaWNQPHHtwZY_4UDGpwN8q7g4qFylAEs/view'
      if component=='replay_bundle' else 'https://drive.google.com/drive/folders/'+drive_folders[component])
 rows.append(dict(component=component,status=status,format=fmt,node_path=str(node),drive_path=remote,drive_url=url,index_sha256=index,manifest_sha256=manifest,archive_bytes=nbytes,notes=notes))
for batch in ['1c7bff15e4d744a29591','1e9b38866259bf82060b','247f9896cc1d99917abbb330','95b56cee0bd19fa0d445463a','primary-retention-56903b5d63ef028b','3c4e71fb0df22139098dc66d']:
 p=flow/'storage/primary-archives'/batch
 receipt=p/'complete.json'
 if not receipt.exists():receipt=flow/'cloud'/batch/'complete.json'
 restoration=p/'.primary-restore/complete.json'
 if receipt.exists():
  v=json.loads(receipt.read_text());status='accepted' if restoration.exists() or batch in ['primary-retention-56903b5d63ef028b','3c4e71fb0df22139098dc66d'] else 'node_restore_pending'
  notes=json.loads(restoration.read_text()).get('manifest_relative','') if restoration.exists() else ''
  add('HF archive '+batch,status,v.get('archive_format','raw-files'),p,v['remote'],v.get('index_sha256',''),v['manifest_sha256'],v['verified_bytes'],notes)
  shutil.copy2(receipt,r/'04_Validation_and_Provenance'/('hf-'+batch+'-cloud-accepted.json'))
 elif restoration.exists():
  v=json.loads(restoration.read_text());add('HF archive '+batch,'accepted',v['archive_format'],p,v['remote'],'',v['manifest_sha256'],v['bytes'],v['manifest_relative'])
 else:
  add('HF archive '+batch,'cloud_pending','hf-flow-tar-index-v1',p,'dsc_hf:26407881/hf-flow-20261116/batches/'+batch,checksum(p/'pack-index.json') if (p/'pack-index.json').exists() else '',checksum(p/'manifest.json') if (p/'manifest.json').exists() else '',notes='Not accepted until cloud complete report exists')
 if restoration.exists():shutil.copy2(restoration,r/'04_Validation_and_Provenance'/('hf-'+batch+'-node-primary.json'))
for component,relative in [('token_events','02_Reconstruction_Inputs/token_events'),('pool_events','02_Reconstruction_Inputs/pool_events'),('configuration','02_Reconstruction_Inputs/configuration'),('oracle_prices','02_Reconstruction_Inputs/oracle_prices'),('pinned_scientific_code','03_Code_and_Definitions')]:
 p=r/relative;done=p/'complete.json'
 if done.exists():
  v=json.loads(done.read_text());
  if not all(v.get(k) is True for k in ['all_local_member_sha256_exact','all_remote_size_md5_exact','all_remote_sha256_exact']):raise ValueError('Input archive is not fully accepted')
  add(component,'accepted','hf-flow-tar-index-v1',p,v['remote'],v['index_sha256'],v['source_manifest_sha256'],v['archive_bytes'])
  shutil.copy2(done,r/'04_Validation_and_Provenance'/(component+'-accepted.json'))
 else:add(component,'cloud_pending','hf-flow-tar-index-v1',p,'dsc_hf:AaveV3_Ethereum/'+relative,notes='Original source retained; cloud acceptance pending')
bundle=Path('/home/node/hf-centralized/20261116/hf-frozen-inputs.tar.gz')
proof=Path('/home/node/hf-centralized/20261118-archive/inputs/cloud-verified.json')
v=json.loads(proof.read_text());shutil.copy2(proof,r/'04_Validation_and_Provenance/replay-bundle-accepted.json')
add('replay_bundle','accepted','tar-gzip-frozen-runtime',bundle,'dsc_hf:26407881/legacy-hf-source-20261118/inputs/hf-centralized/20261116/hf-frozen-inputs.tar.gz',manifest='13e3d756e53473ecb0f5153b6d5a6aab49531bde863814d083098ecd5e71b244',nbytes=2028819638,notes='SHA256 column is full bundle SHA; Linux x86-64 Python3.10 needed')
with (r/'DATA_CATALOG.csv').open('w',newline='') as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
write_json(r/'04_Validation_and_Provenance/dataset-status.json',dict(at=time.time(),all_catalog_components_accepted=all(x['status']=='accepted' for x in rows),components=len(rows),pending=[x['component'] for x in rows if x['status']!='accepted'],cleanup_complete=False,goal_complete=False,canonical_jobs='01_HF_Vectors/CANONICAL_JOBS.csv'))
print(json.dumps({'components':len(rows),'pending':[x['component'] for x in rows if x['status']!='accepted']}),flush=True)
