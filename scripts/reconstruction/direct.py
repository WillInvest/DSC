"""Local event-derived state, with no node comparisons or block-header downloads."""
from copy import deepcopy
from itertools import groupby
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from reconstruction.core import apply
def decimal_strings(state):
    """Match the existing decoded-data convention for exact JSON integers."""
    return {k:decimal_strings(v) if isinstance(v,dict) else str(v) for k,v in state.items()}


def replay_table(protocol, address, seed, events, start, stop, bootstraps=None):
    bootstraps=bootstraps or {}
    state=deepcopy(seed)
    ordered=sorted(events,key=lambda e:(e['block_number'],e['transaction_index'],e['log_index']))
    blocks=[];values=[];cursor=start
    previous=None
    for event in ordered:
        key=(event['block_number'],event['log_index'])
        if not start<=key[0]<stop or key==previous:
            raise ValueError('Duplicate or out-of-range decoded event')
        previous=key
    groups={n:list(group) for n,group in groupby(ordered,key=lambda e:e['block_number'])}
    # Serialize only when state changes, then forward-fill unchanged blocks.
    text=json.dumps(decimal_strings(state),sort_keys=True)
    for n in sorted(set(groups)|{int(n) for n in bootstraps if start<=int(n)<stop}):
        blocks.extend(range(cursor,n));values.extend([text]*(n-cursor))
        if str(n) in bootstraps:
            state=deepcopy(bootstraps[str(n)]) # end-of-block seed: don't apply boundary logs twice
        else:
            for event in groups.get(n,[]): apply(protocol,state,event)
        text=json.dumps(decimal_strings(state),sort_keys=True)
        cursor=n
    blocks.extend(range(cursor,stop));values.extend([text]*(stop-cursor))
    return pa.table({'protocol':[protocol]*len(blocks),'address':[address]*len(blocks),
                     'block_number':pa.array(blocks,type=pa.uint64()),'state_json':values}),state


def read_chunk(decoded, name, handlers):
    folder=Path(decoded)/Path(name).stem
    receipt=json.loads((folder/'receipt.json').read_text())
    events=[]
    for group in receipt['tables']:
        directory=folder/group['directory']
        manifest=json.loads((directory/'manifest.json').read_text())
        for item in manifest['outputs']:
            if Path(item['file']).stem in handlers:
                events.extend(pq.read_table(directory/item['file']).to_pylist())
    return events
