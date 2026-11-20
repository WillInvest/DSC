"""Float64 sparse-edge HF model; existing replay owns historical state semantics.

NumPy performs account reductions. Exact arithmetic remains a reference and a
narrow fallback for dust/ambiguous basis-point boundaries, not the main loop.
"""
import numpy as np
from native_batch import GHO
from native_hf import MAX, ZERO


def exact_debts(ev, owner, reserve_ids):
    """Reference debt attribution through the already verified integer kernel."""
    positions=ev.positions.get(owner,{});mode_id=ev.emodes.get(owner,0);mode=ev.modes.get(mode_id)
    result=[]
    for asset,jobs in ev.available.items():
        if asset not in ev.active or 'variableDebtToken' not in jobs:continue
        token=jobs['variableDebtToken']['address'];scaled=positions.get(token,'0')
        if not int(scaled):continue
        if token==GHO and not ev.ghoborrow.get(owner,False):continue
        row=dict(ev.templates[asset],scaledSupply='0',scaledDebt=scaled)
        if token==GHO:row.update(hasDebtBalanceOverride=True,debtBalanceOverride=str(ev.nominal(token,owner,scaled)))
        request=dict(rows=[row],userConfiguration=str(1<<(2*row['id'])),emode=mode_id,modes=[mode] if mode_id else [],priceAssets=ev.assets,assetPrices=ev.asset_prices)
        value=ev.kernel.evaluate(int(ev.profile),ev.timestamp,request)[1]
        if value:result.append((reserve_ids[asset],float(value)))
    return result


class FloatBatch:
    def __init__(self, owners, jobs):
        self.owners=owners;self.firsts=np.array([b for _,b in owners],dtype=np.int64)
        self.user_ids={a:i for i,(a,_) in enumerate(owners)}
        self.reserves=sorted({j['asset'].lower() for j in jobs})
        self.reserve_ids={a:i for i,a in enumerate(self.reserves)}
        self.tokens={j['address']:j for j in jobs}

    def evaluate(self,ev,hf_override=None,dust_cutoff=0):
        if dust_cutoff not in [0,1_000_000]:raise ValueError('Unreviewed debt cutoff')
        n=int(np.searchsorted(self.firsts,ev.block,side='right'));version=int(ev.profile)
        ids=[];assets=[];shares=[];is_debt=[];collateral=[];modes=[];tokens=[];owners=[]
        # Only nonzero sparse positions are expanded, including supply-only users.
        for owner,positions in ev.positions.items():
            if owner==ZERO:continue
            uid=self.user_ids[owner]
            if uid>=n:raise ValueError('Position precedes owner first appearance')
            for token,scaled in positions.items():
                job=self.tokens[token];asset=job['asset'].lower()
                if asset not in ev.active:continue
                if job['kind']=='stableDebtToken':raise ValueError('Unverified stable debt activity')
                debt=job['kind']=='variableDebtToken'
                if hf_override is not None and not debt:continue
                if token==GHO and debt:
                    if owner not in ev.ghoborrow:raise ValueError('Missing GHO borrowing flag')
                    if not ev.ghoborrow[owner]:continue
                ids.append(uid);assets.append(self.reserve_ids[asset]);shares.append(float(scaled));is_debt.append(debt)
                collateral.append(not debt and asset in ev.collateral.get(owner,set()));modes.append(ev.emodes.get(owner,0));tokens.append(token);owners.append(owner)
        uid=np.asarray(ids,dtype=np.int64);rid=np.asarray(assets,dtype=np.int64);scaled=np.asarray(shares,dtype=np.float64)
        debt_mask=np.asarray(is_debt,dtype=bool);coll_mask=np.asarray(collateral,dtype=bool);mode_ids=np.asarray(modes,dtype=np.int64)
        count=len(uid);price=np.empty(count);index=np.empty(count);unit=np.empty(count);threshold=np.empty(count);native_threshold=np.empty(count)
        for asset in ev.assets:
            if asset not in self.reserve_ids:continue
            select=rid==self.reserve_ids[asset]
            if not np.any(select):continue
            state=ev.reserves[asset];cfg=int(state['configuration']);base_threshold=(cfg>>16)&65535
            unit[select]=float(10**((cfg>>48)&255));threshold[select]=base_threshold;native_threshold[select]=base_threshold
            price[select]=float(ev.asset_prices[ev.assets.index(asset)])
            index[select & debt_mask]=float(ev.normalized[asset]['debt'])/1e27
            index[select & ~debt_mask]=float(ev.normalized[asset]['income'])/1e27
            for mid in np.unique(mode_ids[select]):
                if not mid:continue
                if int(mid) not in ev.modes:raise ValueError('Missing historical eMode')
                mode=ev.modes[int(mid)]
                in_mode=((cfg>>168)&255)==mid if version<20920979 else bool((int(mode['collateralBitmap'])>>int(state['id']))&1)
                if not in_mode:continue
                chosen=select & (mode_ids==mid);threshold[chosen]=int(mode['threshold'])
                if version<20920979 and mode.get('priceSource',ZERO).lower()!=ZERO:
                    source=mode['priceSource'].lower();price[chosen]=float(ev.asset_prices[ev.assets.index(source)])
        if np.any(price<=0) or not np.all(np.isfinite(price)):raise ValueError('Nonpositive/nonfinite historical price')
        nominal=scaled*index
        if version>=23088584:nominal=np.where(debt_mask,np.ceil(nominal),np.floor(nominal))
        else:nominal=np.floor(nominal+0.5)
        # The historical GHO override is explicitly restricted to the same profile
        # and configuration bit as the retained reference GenericLogic adapter.
        if version==21917056:
            for k,token in enumerate(tokens):
                if token==GHO and debt_mask[k] and not (int(ev.reserves[self.reserves[rid[k]]]['configuration'])>>252)&1:
                    nominal[k]=float(ev.nominal(token,owners[k],ev.positions[owners[k]][token]))
        raw=(nominal/unit)*price
        base=np.where(debt_mask & (version>=23088584),np.ceil(raw),np.floor(raw))
        if np.any(base<0) or not np.all(np.isfinite(base)):raise ValueError('Invalid position valuation')
        coll_mask &= (native_threshold>0) | (version>=24247927)
        cb=np.where(coll_mask,base,0.0);db=np.where(debt_mask,base,0.0)
        C=np.bincount(uid,weights=cb,minlength=n);D=np.bincount(uid,weights=db,minlength=n)
        weighted=np.bincount(uid,weights=cb*threshold,minlength=n)
        H=np.full(n,np.inf,dtype=np.float64);borrowers=D>0
        ambiguous=np.zeros(n,dtype=bool)
        if hf_override is None and version<23088584:
            avg=np.divide(weighted,C,out=np.zeros(n),where=C>0)
            lo=np.full(n,np.inf);hi=np.full(n,-np.inf);positive=cb>0
            np.minimum.at(lo,uid[positive],threshold[positive]);np.maximum.at(hi,uid[positive],threshold[positive])
            same=(lo==hi)&np.isfinite(lo);avg[same]=lo[same]
            ambiguous=borrowers & ~same & (C>0) & (np.abs(avg-np.rint(avg))<1e-8)
            adjusted=np.floor((C*np.floor(avg)+5000.0)/10000.0)
        else:adjusted=weighted/10000.0
        if hf_override is None:np.divide(adjusted,D,out=H,where=borrowers)
        else:
            H=np.asarray(hf_override,dtype=np.float64).copy()
            if H.shape!=(n,):raise ValueError('Reused HF user coverage differs')
        # Preserve exact sentinel/quantization semantics where atomic rounding can
        # materially change the research result. This is measured and reported.
        candidate_debt=np.bincount(uid[debt_mask],minlength=n)>0
        if dust_cutoff:
            fallback=np.flatnonzero((ambiguous & (D>=dust_cutoff)) | (candidate_debt & (np.abs(D-dust_cutoff)<=2)) | ((D>=dust_cutoff) & ~np.isfinite(H)))
        else:fallback=np.flatnonzero(ambiguous | (candidate_debt & (D<1e6)) | (borrowers & ~np.isfinite(H)))
        keep=debt_mask & (db>0);duid=uid[keep];drid=rid[keep];values=db[keep]
        replace=np.zeros(n,dtype=bool);replace[fallback]=True
        keep=~replace[duid];duid=duid[keep];drid=drid[keep];values=values[keep]
        extra_users=[];extra_reserves=[];extra_values=[]
        for user in fallback:
            owner=self.owners[int(user)][0]
            if hf_override is None or not np.isfinite(H[user]):
                a=ev.account(owner)['account'];h=int(a[5]);H[user]=np.inf if h==MAX else float(h)/1e18
            exact=exact_debts(ev,owner,self.reserve_ids)
            if exact:
                extra_users.extend([user]*len(exact));extra_reserves.extend(r for r,_ in exact);extra_values.extend(v for _,v in exact)
        if extra_users:
            duid=np.concatenate((duid,np.asarray(extra_users,dtype=np.int64)));drid=np.concatenate((drid,np.asarray(extra_reserves,dtype=np.int64)));values=np.concatenate((values,np.asarray(extra_values,dtype=np.float64)))
        D=np.bincount(duid,weights=values,minlength=n)
        excluded=candidate_debt & (D<dust_cutoff) if dust_cutoff else np.zeros(n,dtype=bool)
        excluded_debt=float(D[excluded].sum());excluded_count=int(np.count_nonzero(excluded))
        if dust_cutoff:
            keep=~excluded[duid];duid=duid[keep];drid=drid[keep];values=values[keep]
            D[excluded]=0.;H[excluded]=np.inf
        if np.any(np.isnan(H)) or np.any(H<0) or np.any(~np.isfinite(H[D>0])) or np.any(~np.isposinf(H[D==0])):raise ValueError('Invalid borrower HF/sentinel')
        return dict(hf=H,debt=D,collateral=C,debt_user_ids=duid,debt_reserve_ids=drid,debt_values=values,exact_fallback_users=len(fallback),excluded_users=excluded_count,excluded_debt_base=excluded_debt)
