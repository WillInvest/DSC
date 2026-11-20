"""Owned-node helpers for block times and historical Aave boundary snapshots."""
import json,sys,subprocess,time,fcntl,os,signal
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from web3 import Web3,HTTPProvider

def web():return Web3(HTTPProvider('http://127.0.0.1:8545',request_kwargs={'timeout':120}))
def named(fields,values):
    return {f['name']:named(f['components'],v) if f['type']=='tuple' else str(v) if f['type'].startswith(('uint','int')) else v for f,v in zip(fields,values)}
def main():
    p=json.load(sys.stdin)
    if p['operation']=='stop_headers':
        root=Path(p['output']);expected=Path('/home/node/dsc-extraction/aave-user-history-v3')/str(p['end_block'])/'accounting-headers'
        if root!=expected:raise ValueError('Cleanup outside owned header task')
        progress=root/'progress.json'
        if progress.exists():
            pid=json.loads(progress.read_text()).get('child_pid')
            if pid and Path('/proc/'+str(pid)+'/cmdline').exists():
                args=Path('/proc/'+str(pid)+'/cmdline').read_bytes().decode().split('\0')
                if 'blocks' not in args or str(root) not in args:raise ValueError('Owned header PID command differs')
                os.kill(pid,signal.SIGTERM)
        print('{}');return
    w=web();assert w.eth.chain_id==1
    assert w.eth.get_block(p['end_block']).hash.to_0x_hex()==p['end_hash']
    if p['operation']=='headers':
        root=Path(p['output']);root.mkdir(parents=True,exist_ok=True)
        sys.path[:0]=['/home/node/dsc-extraction/scripts','/home/node/dsc-extraction/scripts/event-cryo-extraction']
        from shared.artifacts import write_json
        from node import inventory
        from common import chunks
        import pyarrow.parquet as pq
        lock=(root/'extraction.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        expected={name.replace('__logs__','__blocks__'):count for name,count in chunks(dict(start_block=p['start_block']),dict(end_block=p['end_block'],chunk_size=10000)).items()}
        inventory(root,expected)
        command=['/home/node/.cargo/bin/cryo','blocks','--rpc','http://127.0.0.1:8545','--blocks',f"{p['start_block']}:{p['end_block']+1}",'--columns','block_number','block_hash','timestamp','--hex','--chunk-size','10000','--max-concurrent-requests','32','--max-concurrent-chunks','2','--max-retries','5','--output-dir',str(root),'--no-verbose']
        child=subprocess.Popen(command)
        def interrupted(signum,frame):raise KeyboardInterrupt('Header extraction interrupted')
        signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGHUP,interrupted)
        try:
            while child.poll() is None:
                complete=0
                for file in root.glob('*.parquet'):
                    try:complete+=pq.read_metadata(file).num_rows
                    except (OSError,ValueError):pass  # Cryo may still be writing a footer.
                write_json(root/'progress.json',dict(state='collecting_block_times',completed_blocks=complete,total_blocks=p['end_block']-p['start_block']+1,requests=32,updated_at=time.time(),child_pid=child.pid,worker_pid=os.getpid()))
                time.sleep(10)
            if child.returncode:raise RuntimeError('Header Cryo exited '+str(child.returncode))
        finally:
            if child.poll() is None:child.terminate();child.wait(timeout=60)
        return
    def one(item):
        local=web();block=item['block'];abi=p['abis'][item['profile']];pool=local.eth.contract(address=p['pool'],abi=abi);out=dict(item,block_hash=local.eth.get_block(block).hash.to_0x_hex())
        if item['kind']=='reserve':
            fn=pool.functions.getReserveData(Web3.to_checksum_address(item['asset']));data=fn.call(block_identifier=block);out['reserve']=named(fn.abi['outputs'][0]['components'],data)
        elif item['kind']=='mode':
            category=item['category'];mode={'id':category,'priceSource':'0x'+'0'*40,'collateralBitmap':'0','borrowableBitmap':'0','ltvzeroBitmap':'0','isolated':False}
            names={a.get('name') for a in abi if a['type']=='function'}
            name='getEModeCategoryCollateralConfig' if 'getEModeCategoryCollateralConfig' in names else 'getEModeCategoryData'
            fn=getattr(pool.functions,name)(category);value=fn.call(block_identifier=block);fields=fn.abi['outputs'][0].get('components')
            if fields:mode.update(named(fields,value))
            else:mode.update(named(fn.abi['outputs'],value))
            for fnname,field in [('getEModeCategoryCollateralBitmap','collateralBitmap'),('getEModeCategoryBorrowableBitmap','borrowableBitmap'),('getEModeCategoryLtvzeroBitmap','ltvzeroBitmap'),('getIsEModeCategoryIsolated','isolated')]:
                if fnname in names:mode[field]=getattr(pool.functions,fnname)(category).call(block_identifier=block)
            mode['threshold']=mode.get('liquidationThreshold',mode.get('threshold'));mode['bonus']=mode.get('liquidationBonus',mode.get('bonus'));out['mode']=mode
        elif item['kind']=='token_call':
            contract=local.eth.contract(address=Web3.to_checksum_address(item['token']),abi=[dict(type='function',name='balanceOf',stateMutability='view',inputs=[dict(name='owner',type='address')],outputs=[dict(name='',type='uint256')])])
            out['result']=str(contract.functions.balanceOf(Web3.to_checksum_address(item['owner'])).call(block_identifier=block))
        elif item['kind']=='call':
            fn=getattr(pool.functions,item['function'])(*[Web3.to_checksum_address(x) if isinstance(x,str) and x.startswith('0x') and len(x)==42 else x for x in item['args']]);out['result']=fn.call(block_identifier=block)
        else:raise ValueError('Unknown task')
        return out
    with ThreadPoolExecutor(max_workers=16) as ex:result=list(ex.map(one,p['tasks']))
    assert w.eth.get_block(p['end_block']).hash.to_0x_hex()==p['end_hash'];print(json.dumps(result,default=str))
if __name__=='__main__':main()
