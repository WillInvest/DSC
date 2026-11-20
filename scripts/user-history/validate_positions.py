"""Independent node comparisons of a consistent scaled-position checkpoint."""
import json,sqlite3,subprocess,sys,time,hashlib
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
from shared.artifacts import digest,write_json
from pipeline.runtime import ssh,NODE,NODE_PYTHON

NODE_CODE='''import json,sys
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3,HTTPProvider
p=json.load(sys.stdin)
def provider():return Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}))
w=provider();assert w.eth.chain_id==1;block=p['block'];before=w.eth.get_block(block).hash.to_0x_hex()
if p.get('expected_hash'):assert before==p['expected_hash'],'Pinned ending block differs'
def read(item):
 owner,expected=item;contract=provider().eth.contract(address=Web3.to_checksum_address(p['token']),abi=p['abi'])
 actual=contract.functions.scaledBalanceOf(Web3.to_checksum_address(owner)).call(block_identifier=block)
 return dict(owner=owner,expected=expected,actual=str(actual),exact=int(expected)==actual)
if p['kind']=='stableDebtToken':
 c=w.eth.contract(address=Web3.to_checksum_address(p['token']),abi=p['abi'])
 actual=c.functions.totalSupply().call(block_identifier=block)
 samples=[dict(field='totalSupply',expected='0',actual=str(actual),exact=actual==0)]
else:
 with ThreadPoolExecutor(max_workers=8) as pool:samples=list(pool.map(read,p['positions']))
assert before==w.eth.get_block(block).hash.to_0x_hex(),'Block identity changed during comparison'
print(json.dumps(dict(block_hash=before,samples=samples)))
'''

def validate(root,job,result,limit=64):
    root=Path(root);db=sqlite3.connect(root/'scaled-positions.sqlite',timeout=60)
    try:
        db.execute('begin')
        # One transaction prevents mixing position rows from different checkpoints.
        checkpoints=db.execute('select file,decoded_receipt_sha256 from replayed_files where token=? order by file',(job['address'],)).fetchall()
        positions=db.execute('select owner,scaled from positions where token=? order by owner',(job['address'],)).fetchall()
    finally:db.close()
    raw=json.loads((root/'receipts'/(job['address']+'.json')).read_text())
    ordered=sorted(raw['files'],key=lambda n:raw['files'][n]['start'])
    if {x[0] for x in checkpoints}!=set(ordered[:len(checkpoints)]):raise ValueError('Position checkpoint is not a contiguous input prefix')
    if not checkpoints or raw['files'][ordered[len(checkpoints)-1]]['stop']-1!=result['through_block']:raise ValueError('Comparison block differs from position checkpoint')
    positions.sort(key=lambda x:hashlib.sha256((job['address']+str(result['through_block'])+x[0]).encode()).hexdigest())
    profile=json.loads((root/'token-profiles'/(job['address']+'.profiles.json')).read_text())
    version=max((p for p in profile['profiles'] if p['block']<=result['through_block']),key=lambda p:(p['block'],p['log_index']))
    abi=json.loads((root/'token-profiles'/version['abi']).read_text())
    payload=dict(token=job['address'],kind=job['kind'],block=result['through_block'],abi=abi,positions=positions[:limit])
    plan=json.loads((root/'plan.json').read_text())
    if result['through_block']==plan['end_block']:payload['expected_hash']=plan['end_hash']
    if not positions and job['kind']!='stableDebtToken':
        payload['positions']=[('0x0000000000000000000000000000000000000001','0')]
    response=subprocess.run(ssh(NODE,[NODE_PYTHON,'-c',NODE_CODE]),input=json.dumps(payload),capture_output=True,text=True,timeout=180)
    if response.returncode:raise RuntimeError(response.stderr[-4000:])
    reference=json.loads(response.stdout)
    report=dict(token=job['address'],asset=job['asset'],kind=job['kind'],block=result['through_block'],checkpoint_digest=digest(checkpoints),positions_in_snapshot=len(positions),scope='sampled scaled balances; not nominal positions or HF acceptance',created_at=time.time(),**reference)
    report['all_exact']=all(s['exact'] for s in report['samples'])
    path=root/'position-validations'/(job['address']+'-'+str(result['through_block'])+'.json');write_json(path,report)
    if not report['all_exact']:raise ValueError('Node scaled-position mismatch: '+str(path))
    return dict(report=str(path),samples=len(report['samples']),all_exact=True,block=report['block'])

if __name__=='__main__':
    root=Path(sys.argv[1]);plan=json.loads((root/'plan.json').read_text());job=next(j for j in plan['jobs'] if j['address']==sys.argv[2])
    print(json.dumps(validate(root,job,dict(through_block=int(sys.argv[3])),int(sys.argv[4]) if len(sys.argv)>4 else 64)))
