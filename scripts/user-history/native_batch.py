"""Sequential end-of-block evaluation with shared reserve factors and sparse users.

No node or local EVM in the computation path. SQLite inputs are read-only;
original accounting_query is reused only for verified price-file loading and
reserve bootstrap lookup. Each event is applied once within the requested range.
"""
import bisect,json
from collections import defaultdict
from pathlib import Path
from accounting_inputs import read,profiles,profile_at
from accounting_query import Evaluator
from replay_positions import modern_versions
from gho_replay import STANDARD_IMPLEMENTATIONS
from native_hf import AccountKernel,normalize,balance,discounted_balance

GHO='0x786dbff3f1292ae8f92ea68cf93c30b34b1ed04b'
FLAGS=('ReserveUsedAsCollateralEnabled','ReserveUsedAsCollateralDisabled','UserEModeSet')

class BatchEvaluator:
    header=Evaluator.header
    reserve=Evaluator.reserve
    prices=Evaluator.prices
    def __init__(self,root,db,plan,start,stop):
        self.root,self.db,self.plan=Path(root),db,plan
        self.profs=profiles();self.price_cache={};self.block=None
        self.oracle=read('/home/dsc-data/runs/aave-oracle-v3/'+plan['run_id']+'/plan.json')
        self.states=read('/home/dsc-data/runs/aave-oracle-v3/'+plan['run_id']+'/jobs.json')
        self.kernel=AccountKernel('/home/dsc-data/artifacts/aave-account-kernels-20261115')
        self.positions=defaultdict(dict);self.collateral=defaultdict(set);self.emodes={};self.gho={};self.ghoborrow={};self.reserves={};self.modes={}
        self.jobs={j['address']:j for j in plan['jobs']}
        self.token_profiles={};self.modern={}
        for token in self.jobs:
            p=read(self.root/'token-profiles'/(token+'.profiles.json'))
            self.token_profiles[token]=p['profiles'];self.modern[token]=modern_versions(p,self.root)
        self.gho_legacy=set(read('/home/dsc-data/scripts/user-history/gho-kernel.json')['implementations'])
        # The primary key already orders each token/owner history; keep only its
        # latest value. Closed positions disappear from memory, never from owners.
        for token,owner,scaled in db.execute('select token,owner,scaled from changes where block<? order by token,owner,block,tx,log',(start,)):
            self.update_position(token,owner,scaled)
        for name,args in db.execute("select name,args from pool_events where block<? and name in (?,?,?) order by block,log",(start,*FLAGS)):
            self.update_user(name,json.loads(args))
        for owner,previous,discount in db.execute('select owner,previous_index,discount_percent from gho_states where block<? order by token,owner,block,tx,log',(start,)):
            self.gho[owner]=(previous,discount)
        for kind,key,data in db.execute("select kind,key,data from snapshots where block<? and kind in ('mode','gho_borrowing') order by block",(start,)):
            self.update_snapshot(kind,key,json.loads(data))
        assets,_=self.prices(start)
        for asset in assets:
            row=db.execute("select 1 from snapshots where kind='reserve' and key=? and block<? limit 1",(asset,start)).fetchone()
            if row:self.reserves[asset]=dict(self.reserve(asset,start-1))
        self.change_events=iter(db.execute('select block,token,owner,scaled from changes where block>=? and block<? order by block,tx,log',(start,stop)))
        self.pool_events=iter(db.execute("select block,name,args from pool_events where block>=? and block<? and name in (?,?,?,'ReserveDataUpdated') order by block,log",(start,stop,*FLAGS)))
        self.gho_events=iter(db.execute('select block,owner,previous_index,discount_percent from gho_states where block>=? and block<? order by block,tx,log',(start,stop)))
        self.snapshot_events=iter(db.execute('select block,kind,key,data from snapshots where block>=? and block<? order by block',(start,stop)))
        self.pending={};self.start,self.stop=start,stop
    def close(self):
        self.header.cache_clear();self.reserve.cache_clear()
    def update_position(self,token,owner,scaled):
        if int(scaled):self.positions[owner][token]=scaled
        elif owner in self.positions:
            self.positions[owner].pop(token,None)
            if not self.positions[owner]:del self.positions[owner]
    def update_user(self,name,args):
        owner=args['user'].lower()
        if name=='UserEModeSet':self.emodes[owner]=int(args['categoryId'])
        elif name=='ReserveUsedAsCollateralEnabled':self.collateral[owner].add(args['reserve'].lower())
        else:self.collateral[owner].discard(args['reserve'].lower())
    def update_snapshot(self,kind,key,data):
        if kind=='mode':self.modes[int(key)]=data['mode']
        elif kind=='gho_borrowing':self.ghoborrow[key]=data['borrowing']
        elif kind=='reserve':
            state=data['reserve'];cfg=state['configuration'];state['configuration']=cfg['data'] if isinstance(cfg,dict) else cfg[0]
            self.reserves[key]=state
    def consume(self,name,events,b,apply):
        row=self.pending.pop(name,None)
        if row is None:row=next(events,None)
        while row is not None and row[0]<=b:
            apply(row);row=next(events,None)
        if row is not None:self.pending[name]=row
    def advance(self,b):
        if not self.start<=b<self.stop or (self.block is not None and b!=self.block+1) or (self.block is None and b!=self.start):raise ValueError('Batch blocks must be contiguous')
        def pool(row):
            block,name,args=row;args=json.loads(args)
            if name=='ReserveDataUpdated':
                asset=args['reserve'].lower()
                if asset not in self.reserves:
                    # A first-block seed is already the authoritative end state.
                    self.reserves[asset]=dict(self.reserve(asset,block));return
                self.reserves[asset].update(liquidityIndex=args['liquidityIndex'],currentLiquidityRate=args['liquidityRate'],variableBorrowIndex=args['variableBorrowIndex'],currentVariableBorrowRate=args['variableBorrowRate'],lastUpdateTimestamp=self.header(block)[0])
            else:self.update_user(name,args)
        self.consume('changes',self.change_events,b,lambda r:self.update_position(*r[1:]))
        self.consume('pool',self.pool_events,b,pool)
        self.consume('gho',self.gho_events,b,lambda r:self.gho.__setitem__(r[1],r[2:]))
        self.consume('snapshots',self.snapshot_events,b,lambda r:self.update_snapshot(r[1],r[2],json.loads(r[3])))
        self.block=b;self.timestamp,self.block_hash=self.header(b);self.profile=profile_at(self.profs,b)['key']
        self.assets,self.asset_prices=self.prices(b);self.active=set(self.assets);self.templates={};self.normalized={}
        for asset in self.assets:
            state=self.reserves[asset]
            norm=normalize(b,dict(li=state['liquidityIndex'],lr=state['currentLiquidityRate'],vi=state['variableBorrowIndex'],vr=state['currentVariableBorrowRate'],updated=state['lastUpdateTimestamp'],timestamp=self.timestamp))
            self.normalized[asset]=norm
            self.templates[asset]=dict(asset=asset,id=int(state['id']),configuration=str(state['configuration']),liquidityIndex=str(norm['income']),variableBorrowIndex=str(norm['debt']),liquidityRate='0',variableBorrowRate='0',updated=self.timestamp,stablePrincipal='0',stableRate='0',stableUpdated=0)
        self.available={}
        for j in self.plan['jobs']:
            if j['start_block']<=b:self.available.setdefault(j['asset'].lower(),{})[j['kind']]=j
    def nominal(self,token,owner,scaled):
        job=self.jobs[token];asset=job['asset'].lower()
        version=max((v for v in self.token_profiles[token] if v['block']<=self.block),key=lambda v:(v['block'],v['log_index']))
        index=self.normalized[asset]['debt' if job['kind']=='variableDebtToken' else 'income']
        if job['kind']=='stableDebtToken':raise ValueError('Unverified stable debt activity')
        if token==GHO and version['implementation'].lower() in self.gho_legacy:
            if int(scaled) and owner not in self.gho:raise ValueError('Missing GHO discount history')
            previous,discount=self.gho.get(owner,('0','0'));return discounted_balance(scaled,index,previous,discount)
        if token==GHO and version['implementation'].lower() not in STANDARD_IMPLEMENTATIONS:raise ValueError('Unverified GHO version')
        return balance(scaled,index,version['version'] in self.modern[token],job['kind']=='variableDebtToken')
    def account(self,owner,include_balances=False):
        positions=self.positions.get(owner,{})
        relevant=({self.jobs[t]['asset'].lower() for t in positions}|self.collateral.get(owner,set()))&self.active
        if self.ghoborrow.get(owner):
            relevant.update(j['asset'].lower() for t,j in self.jobs.items() if t==GHO and j['asset'].lower() in self.active)
        rows=[];balances=[];config=0;emode=self.emodes.get(owner,0)
        for asset in sorted(relevant):
            jobs=self.available[asset];supply=positions.get(jobs['aToken']['address'],'0');debt=positions.get(jobs['variableDebtToken']['address'],'0')
            if 'stableDebtToken' in jobs and int(positions.get(jobs['stableDebtToken']['address'],'0')):raise ValueError('Unverified stable debt activity')
            row=dict(self.templates[asset],scaledSupply=supply,scaledDebt=debt);rid=row['id']
            borrowing=int(debt)>0;token=jobs['variableDebtToken']['address']
            if token==GHO:
                if borrowing and owner not in self.ghoborrow:raise ValueError('Missing GHO borrowing flag')
                borrowing=self.ghoborrow.get(owner,False)
                if int(debt):row.update(hasDebtBalanceOverride=True,debtBalanceOverride=str(self.nominal(token,owner,debt)))
            if borrowing:config|=1<<(rid*2)
            if asset in self.collateral.get(owner,set()):config|=1<<(rid*2+1)
            rows.append(row)
            if include_balances:
                for kind in ['aToken','variableDebtToken']:
                    token=jobs[kind]['address'];scaled=positions.get(token,'0')
                    if int(scaled):balances.append(dict(token=token,asset=asset,kind=kind,scaled=scaled,balance=str(self.nominal(token,owner,scaled))))
        mode=self.modes.get(emode)
        if emode and mode is None:raise ValueError('Missing historical eMode')
        request=dict(rows=rows,userConfiguration=str(config),emode=emode,modes=[mode] if emode else [],priceAssets=self.assets,assetPrices=self.asset_prices)
        result=[str(v) for v in self.kernel.evaluate(int(self.profile),self.timestamp,request)]
        return dict(user=owner,block=self.block,block_hash=self.block_hash,timestamp=self.timestamp,profile=self.profile,emode=emode,user_configuration=str(config),account=result,balances=balances)
