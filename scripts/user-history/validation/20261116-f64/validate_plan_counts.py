from pathlib import Path
import numpy as np
from shared.artifacts import digest,write_json
from vector_compute import universe
from hf_f64_queue import validate
import materialize_accounts as m
B=Path('/var/lib/dsc-aave-hf-f64/20261116-v2');p=m.load(B/'f64-plan.json');validate(p);owners=universe()
if digest(owners)!=p['identity']['contract']['registry_digest']:raise ValueError('Registry changed')
first=np.array([x[1] for x in owners],dtype=np.int64);prefix=np.r_[0,np.cumsum(first)];starts=np.array([j['start'] for j in p['jobs']],dtype=np.int64);stops=np.array([j['stop'] for j in p['jobs']],dtype=np.int64);a=np.searchsorted(first,starts,side='left');b=np.searchsorted(first,stops,side='left');actual=b*stops-prefix[b]-(a*starts-prefix[a]);expected=np.array([j['expected_rows'] for j in p['jobs']],dtype=np.int64)
if not np.array_equal(actual,expected):raise ValueError('Manifest per-job population differs')
mask=np.array([j['id'] in p['sources'] for j in p['jobs']]);maximum=int(np.max(((stops-starts)*b)[mask],initial=0))
if maximum>32_000_000:raise ValueError('Retained matrix exceeds budget')
r=dict(generation=p['generation'],wallets=len(owners),jobs=len(starts),every_job_population_exact=True,all_logical_values=int(actual.sum()),maximum_retained_source_matrix_values=maximum,max_matrix_budget_values=32_000_000,production_coverage=False)
write_json(B/'evidence/manifest-population-and-memory.json',r);print(r)
