"""Evaluate any covered end-of-block account from factorized histories and original Aave bytecode."""
import argparse,json,sys,subprocess
from functools import lru_cache
from pathlib import Path
sys.path.insert(0,'/home/dsc-data/scripts')
import pyarrow.parquet as pq
from accounting_inputs import read,ROOT,profile_at,profiles,START
from shared.artifacts import checksum,digest
from replay_positions import Math,modern_versions
from gho_replay import STANDARD_IMPLEMENTATIONS

class Worker:
    def __init__(self,args):self.child=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
    def call(self,p):
        self.child.stdin.write(json.dumps(p)+'\n');self.child.stdin.flush();line=self.child.stdout.readline()
        if not line:raise RuntimeError('Original Aave worker exited')
        result=json.loads(line)
        if result.get('error'):raise ValueError(result['error'])
        return result
    def close(self):
        self.child.stdin.close()
        try:self.child.wait(timeout=10)
        except subprocess.TimeoutExpired:self.child.terminate();self.child.wait(timeout=10)
        self.child.stdout.close()

class Evaluator:
    def __init__(self,root,db,plan):
        self.root,self.db,self.plan=Path(root),db,plan;self.profs=profiles();self.artifacts=Path('/home/dsc-data/artifacts')
        runtime=str(Path(__file__).parent/'accounting-runtime')
        self.oracle=read('/home/dsc-data/runs/aave-oracle-v3/'+plan['run_id']+'/plan.json');self.states=read('/home/dsc-data/runs/aave-oracle-v3/'+plan['run_id']+'/jobs.json');self.price_cache={};self.tokenprofiles={}
        if self.oracle['end_block']!=plan['end_block'] or self.oracle['end_hash']!=plan['end_hash']:raise ValueError('Oracle cutoff identity differs')
        cursor=START
        for job in self.oracle['jobs']:
            state=self.states.get(job['id'],{})
            if job['start']!=cursor or state.get('state')!='accepted':raise ValueError('Incomplete contiguous accepted Oracle inputs')
            if checksum(state['receipt'])!=state['receipt_sha256']:raise ValueError('Oracle receipt identity differs')
            cursor=job['stop']
        if cursor!=plan['end_block']+1:raise ValueError('Oracle history does not reach fixed cutoff')
        sources=[Path(__file__),*[Path(__file__).with_name(f) for f in ['accounting_inputs.py','accounting_node.py','run_accounting.py','replay_positions.py','account_worker.mjs','interest_worker.mjs','math_worker.mjs','math-kernel.json']],self.artifacts/'aave-account-kernels-20261115/manifest.json',self.artifacts/'aave-interest-2026-11-14/kernels.json']
        sources.extend(Path(__file__).with_name(f) for f in ['gho_replay.py','gho_worker.mjs','gho-kernel.json'])
        self.generation=digest(dict(sources={str(p):checksum(p) for p in sources},profiles=[dict(block=p['from_block'],implementation=p['implementation'],abi_sha256=digest(p['abi'])) for p in self.profs]))
        self.gho_legacy=set(read(Path(__file__).with_name('gho-kernel.json'))['implementations'])
        self.hf=Worker(['node',str(Path(__file__).with_name('account_worker.mjs')),str(self.artifacts/'aave-account-kernels-20261115'),runtime]);self.interest=Worker(['node',str(Path(__file__).with_name('interest_worker.mjs')),'--artifact',str(self.artifacts/'aave-interest-2026-11-14/kernels.json'),'--runtime-root',runtime]);self.math=Math()
        self.gho=Worker(['node',str(Path(__file__).with_name('gho_worker.mjs')),'--runtime-root',runtime])
        self.ready_tokens=set()
    def close(self):
        self.hf.close();self.interest.close();self.math.close();self.gho.close()
        self.header.cache_clear();self.reserve.cache_clear()
    @lru_cache(maxsize=20000)
    def header(self,b):
        r=self.db.execute('select timestamp,hash from headers where block=?',(b,)).fetchone()
        if r is None:raise ValueError('Block time not yet collected')
        return r
    @lru_cache(maxsize=4096)
    def reserve(self,asset,b):
        row=self.db.execute("select block,data from snapshots where kind='reserve' and key=? and block<=? order by block desc limit 1",(asset,b)).fetchone()
        if row is None:raise ValueError('Missing reserve seed '+asset)
        seedblock,encoded=row;state=json.loads(encoded)['reserve'];event=self.db.execute("select block,args from pool_events where name='ReserveDataUpdated' and block>? and block<=? and json_extract(args,'$.reserve')=? collate nocase order by block desc,log desc limit 1",(seedblock,b,asset)).fetchone()
        if event:
            eb,a=event;a=json.loads(a);state.update(liquidityIndex=a['liquidityIndex'],currentLiquidityRate=a['liquidityRate'],variableBorrowIndex=a['variableBorrowIndex'],currentVariableBorrowRate=a['variableBorrowRate'],lastUpdateTimestamp=self.header(eb)[0])
        config=state['configuration'];state['configuration']=config['data'] if isinstance(config,dict) else config[0]
        return state
    def prices(self,b):
        job=next(j for j in self.oracle['jobs'] if j['start']<=b<j['stop']);state=self.states[job['id']]
        if state['state']!='accepted':raise ValueError('Oracle interval not accepted')
        receipt=read(state['receipt']);f=next(f for f in receipt['files'] if f['start']<=b<f['stop']) if job['assets'] else None
        if f is None:return [],[]
        path=Path(job['destination'])/'decoded'/f['name'];key=str(path)
        if key not in self.price_cache:
            if checksum(path)!=f['decoded_sha256']:raise ValueError('Oracle decoded file identity differs')
            self.price_cache={key:{r['block_number']:r['prices'] for r in pq.read_table(path).to_pylist()}}
        return [a.lower() for a in job['assets']],self.price_cache[key][b]
    def shares(self,token,owner,b):
        row=self.db.execute('select scaled from changes where token=? and owner=? and block<=? order by block desc,tx desc,log desc limit 1',(token,owner,b)).fetchone();return row[0] if row else '0'
    def actual(self,job,owner,b,state=None):
        token=job['address'];profile=self.tokenprofiles.get(token)
        if profile is None:
            p=read(self.root/'token-profiles'/(token+'.profiles.json'));profile=(p,modern_versions(p,self.root));self.tokenprofiles[token]=profile
        p,modern=profile;version=max((v for v in p['profiles'] if v['block']<=b),key=lambda v:(v['block'],v['log_index']))
        shares=self.shares(token,owner,b);state=state or self.reserve(job['asset'].lower(),b);timestamp=self.header(b)[0]
        normalized=self.interest.call(dict(operation='normalize',block=b,rows=[dict(li=str(state['liquidityIndex']),lr=str(state['currentLiquidityRate']),vi=str(state['variableBorrowIndex']),vr=str(state['currentVariableBorrowRate']),updated=state['lastUpdateTimestamp'],timestamp=timestamp)]))['rows'][0]
        if job['kind']=='stableDebtToken':
            # The upstream replay accepts only node-proven zero-activity stable histories.
            if shares!='0':raise ValueError('Stable debt needs verified nominal adapter')
            actual='0'
        elif job['address']=='0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b' and version['implementation'].lower() in self.gho_legacy:
            extra=self.db.execute('select previous_index,discount_percent from gho_states where token=? and owner=? and block<=? order by block desc,tx desc,log desc limit 1',(token,owner,b)).fetchone()
            if extra is None and int(shares):raise ValueError('Missing historical GHO discount state')
            previous,discount=extra or ('0','0')
            actual=self.gho.call(dict(operation='balance',implementation=version['implementation'],shares=shares,index=normalized['debt'],previous_index=previous,discount_percent=discount,block=b))['value']
        else:
            if job['address']=='0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b' and version['implementation'].lower() not in STANDARD_IMPLEMENTATIONS:raise ValueError('Unverified historical GHO balance implementation')
            actual=self.math.call(dict(operation='balance',shares=shares,index=normalized['debt' if job['kind']=='variableDebtToken' else 'income'],modern=version['version'] in modern,debt=job['kind']=='variableDebtToken'))['value']
        return dict(token=token,asset=job['asset'],kind=job['kind'],scaled=shares,balance=actual,normalized=normalized,profile=version['version'])
    def account(self,owner,b,include_balances=True):
        if not START<=b<=self.plan['end_block']:raise ValueError('Block outside fixed historical scope')
        owner=owner.lower();assets,prices=self.prices(b);rows=[];balances=[];config=0
        modes=[]
        for asset in assets:
            state=self.reserve(asset,b);available={j['kind']:j for j in self.plan['jobs'] if j['asset'].lower()==asset and j['start_block']<=b}
            for job in available.values():
                if job['address'] in self.ready_tokens:continue
                states=read(self.root/'position-jobs.json')
                if states.get(job['address'],{}).get('state')!='scaled_replay_complete':raise ValueError('Incomplete token history '+job['address'])
                marker=self.db.execute('select receipt from nominal_tokens where token=?',(job['address'],)).fetchone()
                if marker is None or read(marker[0]).get('generation')!=self.generation:raise ValueError('Token factors not yet indexed and validated '+job['address'])
                self.ready_tokens.add(job['address'])
            supply=self.shares(available['aToken']['address'],owner,b);debt=self.shares(available['variableDebtToken']['address'],owner,b)
            if 'stableDebtToken' in available and self.shares(available['stableDebtToken']['address'],owner,b)!='0':raise ValueError('Unsupported stable debt activity')
            flag=self.db.execute("select name from pool_events where block<=? and name in ('ReserveUsedAsCollateralEnabled','ReserveUsedAsCollateralDisabled') and json_extract(args,'$.user')=? collate nocase and json_extract(args,'$.reserve')=? collate nocase order by block desc,log desc limit 1",(b,owner,asset)).fetchone()
            rid=int(state['id'])
            borrowing=int(debt)>0
            if asset=='0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f':
                ghostate=self.db.execute("select data from snapshots where kind='gho_borrowing' and key=? and block<=? order by block desc limit 1",(owner,b)).fetchone()
                if ghostate is None and borrowing:raise ValueError('Missing historical GHO borrowing flag')
                borrowing=json.loads(ghostate[0])['borrowing'] if ghostate else False
            if borrowing:config|=1<<(rid*2)
            if flag and flag[0]=='ReserveUsedAsCollateralEnabled':config|=1<<(rid*2+1)
            override=asset=='0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f' and int(debt)>0
            nominal_debt=self.actual(available['variableDebtToken'],owner,b,state)['balance'] if override else '0'
            rows.append(dict(asset=asset,id=rid,configuration=str(state['configuration']),liquidityIndex=str(state['liquidityIndex']),liquidityRate=str(state['currentLiquidityRate']),variableBorrowIndex=str(state['variableBorrowIndex']),variableBorrowRate=str(state['currentVariableBorrowRate']),updated=state['lastUpdateTimestamp'],scaledSupply=supply,scaledDebt=debt,stablePrincipal='0',stableRate='0',stableUpdated=0,hasDebtBalanceOverride=override,debtBalanceOverride=nominal_debt))
            if include_balances:
                for kind in ['aToken','variableDebtToken']:
                    job=available[kind]
                    if int(self.shares(job['address'],owner,b)):balances.append(self.actual(job,owner,b,state))
        mode=self.db.execute("select args from pool_events where name='UserEModeSet' and block<=? and json_extract(args,'$.user')=? collate nocase order by block desc,log desc limit 1",(b,owner)).fetchone();emode=int(json.loads(mode[0])['categoryId']) if mode else 0
        if emode:
            row=self.db.execute("select data from snapshots where kind='mode' and key=? and block<=? order by block desc limit 1",(str(emode),b)).fetchone()
            if row is None:raise ValueError('Missing historical eMode configuration')
            mode=json.loads(row[0])['mode']
            for key in ['ltv','threshold','bonus','collateralBitmap','borrowableBitmap','ltvzeroBitmap']:
                if key in mode:mode[key]=str(mode[key])
            modes=[mode]
        stamp,blockhash=self.header(b);p=profile_at(self.profs,b)
        request=dict(id=owner,rows=rows,userConfiguration=str(config),emode=emode,modes=modes,priceAssets=assets,assetPrices=prices)
        output=self.hf.call(dict(operation='evaluate',profile=p['key'],block=b,timestamp=stamp,accounts=[request]))['results'][0]
        fields=['totalCollateralBase','totalDebtBase','availableBorrowsBase','currentLiquidationThreshold','ltv','healthFactor']
        return dict(user=owner,block=b,block_hash=blockhash,timestamp=stamp,profile=p['key'],emode=emode,user_configuration=str(config),account=output['account'],metrics=dict(zip(fields,output['account'])),balances=balances)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--user',required=True);p.add_argument('--block',type=int,required=True);a=p.parse_args();current=read(ROOT/'current.json');root=Path(current['root']);plan=read(current['plan']);db=__import__('sqlite3').connect('file:'+str(root/'accounting.sqlite')+'?mode=ro',uri=True);db.execute('begin');ev=Evaluator(root,db,plan)
    try:print(json.dumps(ev.account(a.user,a.block)))
    finally:ev.close();db.close()
