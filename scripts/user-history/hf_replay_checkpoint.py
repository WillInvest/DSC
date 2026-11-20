"""Immutable replay checkpoints; existing BatchEvaluator still applies all events.

A checkpoint seeds its four prefix queries. Tail queries use SQLite's established
ordering. Other queries, including historical reserve configuration, are intact.
"""
import gzip
import json
import os
import time
from pathlib import Path
from shared.artifacts import checksum,digest,write_json
from native_batch import BatchEvaluator,FLAGS
from hf_f64_contract import FACTOR,IMAGE,ORIGIN

class SeededDatabase:
    def __init__(self,db,state,block):self.db=db;self.state=state;self.block=block;self.seeded_calls=0
    def execute(self,sql,args=()):
        if sql=='select token,owner,scaled from changes where block<? order by token,owner,block,tx,log':
            rows=[(t,o,s) for o,positions in self.state['positions'].items() for t,s in positions.items()]
            tail=self.db.execute('select token,owner,scaled from changes where block>=? and block<? order by token,owner,block,tx,log',(self.block,args[0]))
        elif sql=="select name,args from pool_events where block<? and name in (?,?,?) order by block,log":
            rows=[('UserEModeSet',json.dumps(dict(user=o,categoryId=v))) for o,v in self.state['emodes'].items()]
            rows += [('ReserveUsedAsCollateralEnabled',json.dumps(dict(user=o,reserve=a))) for o,assets in self.state['collateral'].items() for a in assets]
            tail=self.db.execute('select name,args from pool_events where block>=? and block<? and name in (?,?,?) order by block,log',(self.block,args[0],*args[1:]))
        elif sql=='select owner,previous_index,discount_percent from gho_states where block<? order by token,owner,block,tx,log':
            rows=[(o,*v) for o,v in self.state['gho'].items()]
            tail=self.db.execute('select owner,previous_index,discount_percent from gho_states where block>=? and block<? order by token,owner,block,tx,log',(self.block,args[0]))
        elif sql=="select kind,key,data from snapshots where block<? and kind in ('mode','gho_borrowing') order by block":
            rows=[('mode',k,json.dumps({'mode':v})) for k,v in self.state['modes'].items()]
            rows += [('gho_borrowing',k,json.dumps({'borrowing':v})) for k,v in self.state['ghoborrow'].items()]
            tail=self.db.execute("select kind,key,data from snapshots where block>=? and block<? and kind in ('mode','gho_borrowing') order by block",(self.block,args[0]))
        else:return self.db.execute(sql,args)
        self.seeded_calls+=1
        import itertools
        return itertools.chain(rows,tail)


def state_of(ev):
    return dict(positions={o:dict(v) for o,v in ev.positions.items() if v},collateral={o:sorted(v) for o,v in ev.collateral.items() if v},emodes=ev.emodes,gho=ev.gho,modes=ev.modes,ghoborrow=ev.ghoborrow)


def build(root,db,plan,block,path):
    path=Path(path);started=time.monotonic();ev=BatchEvaluator(root,db,plan,block,block+1)
    try:state=state_of(ev)
    finally:ev.close()
    record=dict(factor_generation=FACTOR,snapshot_manifest_sha256=IMAGE,exclusive_before_block=block,state=state)
    encoded=json.dumps(record,sort_keys=True).encode();compressed=gzip.compress(encoded,compresslevel=6,mtime=0)
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():raise ValueError('Refuse overwrite checkpoint')
    with path.open('xb') as h:h.write(compressed);h.flush();os.fsync(h.fileno())
    side=dict(path=str(path),sha256=checksum(path),state_sha256=digest(state),block=block,build_seconds=time.monotonic()-started,bytes=len(compressed))
    write_json(path.with_suffix('.receipt.json'),side);return side


def restore(root,db,plan,start,stop,path,expected_sha256):
    if checksum(path)!=expected_sha256:raise ValueError('Checkpoint hash differs')
    record=json.loads(gzip.decompress(Path(path).read_bytes()))
    block=record['exclusive_before_block']
    if record['factor_generation']!=FACTOR or record['snapshot_manifest_sha256']!=IMAGE or not ORIGIN<=block<=start:raise ValueError('Checkpoint input identity/range differs')
    proxy=SeededDatabase(db,record['state'],block);ev=BatchEvaluator(root,proxy,plan,start,stop)
    if proxy.seeded_calls!=4:raise ValueError('Replay prefix API changed')
    return ev
