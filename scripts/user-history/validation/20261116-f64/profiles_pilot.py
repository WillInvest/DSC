import json,multiprocessing,time
from pathlib import Path
from shared.artifacts import write_json
from benchmark_hf_f64 import probe
BASE=Path('/var/lib/dsc-aave-hf-f64/20261116-v2')
if __name__=='__main__':
 manifest=json.loads(Path('/home/dsc-data/artifacts/aave-account-kernels-20261115/manifest.json').read_text());reports=[]
 for p in manifest['profiles']:
  block=max(16496792,int(p['from_block']))
  with multiprocessing.get_context('spawn').Pool(1,maxtasksperchild=1) as pool:r=pool.apply(probe,(block,))
  write_json(BASE/('evidence/profile-'+str(p['profile'])+'.json'),r);reports.append(r)
  passed=all(r[k] for k in ['all_aggregate_within_budget','all_denominators_within_budget','all_no_debt_classifications_exact','all_user_hf_within_budget'])
  print(json.dumps(dict(profile=p['profile'],block=block,users=r['users'],passed=passed,seconds=r['wall_seconds'])),flush=True)
  if not passed:raise ValueError('Historical profile numerical budget failed')
 write_json(BASE/'evidence/all-profiles.json',dict(profiles=len(reports),all_passed=True,new_node_rpc=0,reference_user_blocks=sum(r['users'] for r in reports),reports=reports))
