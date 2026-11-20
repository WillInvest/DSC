import json,sys
from pathlib import Path
import materialize_accounts as m
from hf_replay_checkpoint import build
root=Path('/home/dsc-data/runs/aave-user-history-v3/26407881');db=m.readonly(root/'accounting.sqlite')
try:r=build(root,db,m.load(root/'plan.json'),25199900,Path(sys.argv[1]));print(json.dumps(r),flush=True)
finally:db.close()
