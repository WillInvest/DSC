"""Node-only verification of token implementation intervals from retained logs."""
import json,sys,hashlib
import requests
from pathlib import Path
from collections import defaultdict
sys.path.insert(0,'/home/node/dsc-extraction/scripts')
from web3 import Web3,HTTPProvider
from registry.build_aave_profiles import implementation,verified_abi,extraction_abi
from shared.artifacts import digest,write_json

UPGRADED={'type':'event','name':'Upgraded','anonymous':False,'inputs':[dict(name='implementation',type='address',indexed=True)]}
def source_abi(w,address,block,directory):
    fallback=directory/(address.lower()+'.blockscout.json')
    if not fallback.exists():
        try:return verified_abi(w,address,block,directory)
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code!=404:raise
        url='https://eth.blockscout.com/api/v2/smart-contracts/'+address
        response=requests.get(url,timeout=90);response.raise_for_status();record=response.json()
        if not record.get('is_verified') or not record.get('abi'):raise ValueError('No verified ABI for '+address)
        write_json(fallback,record)
    record=json.loads(fallback.read_text());runtime=bytes(w.eth.get_code(address,block_identifier=block))
    if not runtime or runtime!=bytes.fromhex(record['deployed_bytecode'].removeprefix('0x')):raise ValueError('Blockscout source record / historical node runtime mismatch')
    proof=dict(address=address,url='https://eth.blockscout.com/api/v2/smart-contracts/'+address,record_sha256=hashlib.sha256(fallback.read_bytes()).hexdigest(),runtime_sha256=hashlib.sha256(runtime).hexdigest(),source_verification='partial' if record.get('is_partially_verified') else 'verified',note='ABI from verified source record; deployed bytecode matched node exactly; compiler metadata may be partially matched')
    return record['abi'],proof

def build(request,root):
    root=Path(root);root.mkdir(parents=True,exist_ok=True);key=digest(request);result_path=root/(request['address']+'.profiles.json')
    if result_path.exists():
        saved=json.loads(result_path.read_text())
        if saved['request_digest']!=key:raise ValueError('Token profile input identity changed')
        return saved
    w=Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':60}));address=Web3.to_checksum_address(request['address']);end=request['end_block'];start=request['start_block']
    if w.eth.chain_id!=1 or w.eth.get_block(end).hash.to_0x_hex()!=request['end_hash']:raise ValueError('Wrong pinned chain/end block')
    sources=root/'sources';sources.mkdir(exist_ok=True)
    proxy_abi,proxy_proof=source_abi(w,address,end,sources)
    event=w.eth.contract(address=address,abi=[UPGRADED]).events.Upgraded()
    grouped=defaultdict(list)
    for upgrade in request['upgrades']:grouped[upgrade['block']].append(upgrade)
    for block,expected in grouped.items():
        actual=event.get_logs(from_block=block,to_block=block)
        left=sorted((e['logIndex'],e['args']['implementation'].lower(),e['blockHash'].to_0x_hex(),e['transactionHash'].to_0x_hex()) for e in actual)
        right=sorted((e['log_index'],e['implementation'].lower(),e['block_hash'],e['transaction_hash']) for e in expected)
        if left!=right:raise ValueError('Token upgrade log differs from node at '+str(block))
    upgrades=sorted(request['upgrades'],key=lambda e:(e['block'],e['log_index']))
    initial=next((e['implementation'] for e in upgrades if e['block']==start),None) or implementation(w,address,start)
    boundaries=[dict(block=start,log_index=-1,implementation=initial)]
    for e in upgrades:
        if e['implementation'].lower()!=boundaries[-1]['implementation'].lower():boundaries.append(dict(block=e['block'],log_index=e['log_index'],implementation=e['implementation']))
    if implementation(w,address,end).lower()!=boundaries[-1]['implementation'].lower():raise ValueError('Upgrade history does not reach node ending implementation')
    profiles=[]
    for i,boundary in enumerate(boundaries):
        impl=Web3.to_checksum_address(boundary['implementation']);abi,proof=source_abi(w,impl,boundary['block'],sources)
        merged=extraction_abi(abi,[abi,proxy_abi]);abi_key=digest(merged);relative='abi/'+abi_key+'.json';write_json(root/relative,merged)
        profiles.append(dict(version=i+1,**boundary,abi=relative,implementation_proof=proof))
    result=dict(request_digest=key,address=request['address'],start_block=start,end_block=end,end_hash=request['end_hash'],proxy_proof=proxy_proof,profiles=profiles,validation='Every retained upgrade matched node; implementation ABIs matched historical runtime')
    write_json(result_path,result);return result
if __name__=='__main__':
    request=json.loads(sys.argv[1]);print(json.dumps(build(request,sys.argv[2])))
