"""Integer account arithmetic adapted from the retained Aave historical sources.

Source of truth: artifacts/aave-account-kernels-20261115/*.sources.json
(GenericLogic, MathUtils, TokenMath, ValidationLogic; original license headers
remain in those artifacts). This adapter has no EVM, RPC, floats or file writes.
It is validated against original bytecode, not a replacement source of rules.
"""
import hashlib,json
from pathlib import Path

RAY=10**27
WAD=10**18
YEAR=365*24*3600
MAX=2**256-1
ZERO='0x'+'0'*40

def uint(n):
    n=int(n)
    if not 0<=n<=MAX:raise ValueError('Aave uint256 overflow/underflow')
    return n

def muldiv(a,b,d,rounding='half'):
    if d<=0:raise ValueError('Aave division by zero')
    product=uint(a*b)
    if rounding=='half':return uint(product+d//2)//d
    q,r=divmod(product,d)
    return q+int(rounding=='ceil' and r!=0)

def normalize(block,row):
    li,lr,vi,vr,updated=[uint(row[k]) for k in ['li','lr','vi','vr','updated']]
    stamp=uint(row['timestamp']);elapsed=stamp-updated
    if elapsed<0:raise ValueError('Future reserve timestamp')
    if max(li,lr,vi,vr)>=2**128 or max(updated,stamp)>=2**40:raise ValueError('Reserve field out of range')
    if not elapsed:return dict(income=str(li),debt=str(vi))
    linear=uint(RAY+uint(lr*elapsed)//YEAR)
    if block<22839362:
        square=muldiv(vr,vr,RAY)//(YEAR*YEAR)
        cube=muldiv(square,vr,RAY)//YEAR
        compound=uint(RAY+uint(vr*elapsed)//YEAR+uint(elapsed*(elapsed-1)*square)//2+uint(elapsed*(elapsed-1)*max(elapsed-2,0)*cube)//6)
    else:
        x=vr*elapsed//YEAR
        compound=(RAY+x+muldiv(x,x//2+muldiv(x,x//6,RAY),RAY)) & MAX
    return dict(income=str(muldiv(linear,li,RAY)),debt=str(muldiv(compound,vi,RAY)))

def balance(shares,index,modern=False,debt=False):
    return muldiv(uint(shares),uint(index),RAY,('ceil' if debt else 'floor') if modern else 'half')

def discounted_balance(shares,index,previous,discount):
    shares,index,previous,discount=map(uint,(shares,index,previous,discount))
    if shares>=2**128 or previous>=2**128 or discount>=2**16:raise ValueError('GHO state out of range')
    if not shares:return 0
    value=balance(shares,index)
    if index!=previous and discount:value=uint(value-muldiv(uint(value-balance(shares,previous)),discount,10000))
    return value


from functools import lru_cache

@lru_cache(maxsize=1)
def _prepared_prices(assets, raw_prices):
    prices={k.lower():uint(v) for k,v in zip(assets,raw_prices)}
    if len(assets)!=len(raw_prices):raise ValueError('Incomplete prices')
    if any(v<=0 for v in prices.values()):raise ValueError('Nonpositive Oracle price')
    return prices

@lru_cache(maxsize=128)
def _prepared_norm(version,timestamp,li,lr,vi,vr,updated):
    return normalize(version,dict(li=li,lr=lr,vi=vi,vr=vr,updated=updated,timestamp=timestamp))

class AccountKernel:
    def __init__(self,artifacts):
        self.artifacts=Path(artifacts)
        raw=(self.artifacts/'manifest.json').read_bytes()
        if hashlib.sha256(raw).hexdigest()!='f647dc7b95118f34a4d6384d15ecc481b9c57343fdf5cdca642713084c0823a5':raise ValueError('Unreviewed native historical rules manifest')
        manifest=json.loads(raw)
        self.profiles={p['profile']:p for p in manifest['profiles']}
    def close(self):pass
    def call(self,p):
        if p['operation']!='evaluate':raise ValueError('Unsupported native operation')
        key=str(p['profile']);profile=self.profiles[key];block=int(p['block'])
        if not profile['from_block']<=block<profile['to_block_exclusive']:raise ValueError('Block outside reviewed profile')
        results=[]
        for a in p['accounts']:
            results.append(dict(id=a['id'],account=[str(v) for v in self.evaluate(int(key),int(p['timestamp']),a)]))
        return dict(results=results)
    def evaluate(self,version,timestamp,a):
        config=uint(a['userConfiguration']);emode=int(a['emode'])
        if not config:return [0,0,0,0,0,MAX]
        prices=_prepared_prices(tuple(a['priceAssets']),tuple(a['assetPrices']))
        rows={int(r['id']):r for r in a['rows']}
        present=sum(3<<(rid*2) for rid in rows if 0<=rid<128)
        if config & ~present:raise ValueError('missing reserve id')
        mode=next((m for m in a.get('modes',[]) if int(m['id'])==emode),None)
        if emode and mode is None:raise ValueError('Missing selected eMode')
        if emode and version>=20920979 and 'collateralBitmap' not in mode:raise ValueError('Missing historical eMode field')
        collateral=debt=weighted_ltv=weighted_threshold=0
        for rid,r in rows.items():
            flags=(config>>(rid*2))&3
            if not flags:continue
            cfg=uint(r['configuration']);ltv=cfg&65535;threshold=(cfg>>16)&65535;unit=10**((cfg>>48)&255)
            if not unit or unit>MAX:raise ValueError('Invalid asset decimals')
            in_mode=bool(emode and (((cfg>>168)&255)==emode if version<20920979 else (int(mode['collateralBitmap'])>>rid)&1))
            price=prices[r['asset'].lower()]
            if version<20920979 and in_mode and mode.get('priceSource',ZERO).lower()!=ZERO:
                price=prices[mode['priceSource'].lower()]
            norm=_prepared_norm(version,timestamp,r['liquidityIndex'],r['liquidityRate'],r['variableBorrowIndex'],r['variableBorrowRate'],r['updated'])
            if int(r.get('stablePrincipal',0)):raise ValueError('Unverified stable debt activity')
            if flags&2 and (threshold or version>=24247927):
                supplied=balance(r['scaledSupply'],norm['income'],version>=23088584)
                base=uint(supplied*price)//unit
                collateral=uint(collateral+base)
                effective_ltv=int(mode['ltv']) if in_mode else ltv
                if version<24247927:
                    if not ltv:effective_ltv=0
                elif in_mode and (int(mode.get('ltvzeroBitmap',0))>>rid)&1:effective_ltv=0
                elif emode and version>=25199939 and not in_mode and mode.get('isolated',False):effective_ltv=0
                weighted_ltv=uint(weighted_ltv+uint(base*effective_ltv))
                weighted_threshold=uint(weighted_threshold+uint(base*(int(mode['threshold']) if in_mode else threshold)))
            if flags&1:
                nominal=balance(r['scaledDebt'],norm['debt'],version>=23088584,True)
                if version==21917056 and not (cfg>>252)&1 and r.get('hasDebtBalanceOverride'):nominal=uint(r['debtBalanceOverride'])
                base=muldiv(nominal,price,unit,'ceil' if version>=23088584 else 'floor')
                debt=uint(debt+base)
        avg_ltv=weighted_ltv//collateral if collateral else 0
        avg_threshold=weighted_threshold//collateral if collateral else 0
        hf=MAX if not debt else (muldiv(weighted_threshold,WAD,debt)//10000 if version>=23088584 else muldiv(muldiv(collateral,avg_threshold,10000),WAD,debt))
        available=max(0,muldiv(collateral,avg_ltv,10000,'floor' if version>=23088584 else 'half')-debt)
        return [collateral,debt,available,avg_threshold,avg_ltv,hf]
