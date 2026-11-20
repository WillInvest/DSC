"""Debt-weighted user HF research indicators; not official Aave market getters."""
import numpy as np

def aggregate(hf,user_ids,reserve_ids,debt_values,reserve_count):
    hf=np.asarray(hf,dtype=np.float64);u=np.asarray(user_ids,dtype=np.int64);r=np.asarray(reserve_ids,dtype=np.int64);d=np.asarray(debt_values,dtype=np.float64)
    if not (len(u)==len(r)==len(d)) or np.any(d<0) or not np.all(np.isfinite(d)):raise ValueError('Invalid debt edges')
    if np.any(u<0) or np.any(u>=len(hf)) or np.any(r<0) or np.any(r>=reserve_count):raise ValueError('Debt attribution out of range')
    mask=d>0;u=u[mask];r=r[mask];d=d[mask]
    if not np.all(np.isfinite(hf[u])) or np.any(hf[u]<0):raise ValueError('Positive debt with nonfinite/negative HF')
    numerator=np.bincount(r,weights=d*hf[u],minlength=reserve_count);denominator=np.bincount(r,weights=d,minlength=reserve_count)
    valid=denominator>0;result=np.divide(numerator,denominator,out=np.zeros(reserve_count),where=valid)
    pn=float(np.sum(numerator,dtype=np.float64));pd=float(np.sum(denominator,dtype=np.float64))
    # Separate direct-user reduction is a cross-check, not the returned sum again.
    user_debt=np.bincount(u,weights=d,minlength=len(hf));borrowers=user_debt>0
    direct_n=float(np.sum(user_debt[borrowers]*hf[borrowers],dtype=np.float64));direct_d=float(np.sum(user_debt,dtype=np.float64))
    if not np.allclose([pn,pd],[direct_n,direct_d],rtol=1e-10,atol=1e-10):raise ValueError('Reserve/protocol attribution differs')
    return dict(numerator=numerator,denominator=denominator,hf=result,valid=valid,protocol_numerator=pn,protocol_denominator=pd,protocol_hf=pn/pd if pd else None)
