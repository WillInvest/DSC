"""Export a catalog-pinned continuous sample; copy and verify on Drive, never retire."""
import csv
import datetime
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from shared.artifacts import checksum
from hf_flow_json import write_json
from hf_flow_retire import job_locks
from hf_flow_cloud import _common, _copy_and_check

ROOT = Path('/home/node/hf-colleague-samples/svb-20230303-20230315')
OUT = ROOT / 'release'
REMOTE = 'dsc_hf:01_RESEARCH_READY/AaveV3_SVB_2023-03-03_to_2023-03-15_v1'
ORIGIN = 16291127


def status(phase, **extra):
    write_json(ROOT / 'progress.json', dict(phase=phase, at=time.time(), **extra))
    print(phase, flush=True)


def copy_verified(source, target, expected):
    source, target = Path(source), Path(target)
    if source.is_symlink() or checksum(source) != expected:
        raise ValueError('Source file differs from accepted receipt: ' + str(source))
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if checksum(target) != expected:
            raise ValueError('Existing immutable sample file differs: ' + str(target))
    else:
        temporary = target.with_suffix(target.suffix + '.copying')
        shutil.copyfile(source, temporary)
        if checksum(temporary) != expected:
            raise ValueError('Sample copy checksum mismatch')
        temporary.replace(target)


def guides(manifest):
    first, last = manifest['first_timestamp_utc'], manifest['last_timestamp_utc']
    readme = f'''# Aave V3 Ethereum — continuous SVB research sample

## Start here

This is an analysis-ready, continuous subset of an unfinished larger project.
Period: **{first} through {last}**, UTC, inclusive block endpoints.
Blocks: **{manifest['start_block']:,}–{manifest['end_block']:,}**, every block end,
**{manifest['blocks']:,} blocks**, no block downsampling or gaps.
Population: **{manifest['wallets']:,} historical addresses**, from each address's
first appearance within this window; **{manifest['logical_user_values']:,} HF values**.
An address is not necessarily a person, an active borrower, or a current user.

## Folder map

| Folder/file | What to use it for |
|---|---|
| 01_quick_start/protocol_by_block.csv | First chart: protocol research indicator and debt value, one row per block |
| 01_quick_start/reserves_with_debt_by_block.csv.gz | Reserve charts; only reserve/block pairs with positive included debt |
| 01_quick_start/example_wallet_1.csv | One illustrative wallet's complete series; not a representative borrower sample |
| 02_complete_data/user_hf_vectors/ | All included wallets' complete HF vectors, original accepted Parquet bytes |
| 02_complete_data/aggregate_vectors/ | Every reserve and protocol scope, including zero-debt/null-HF scopes |
| 03_dictionaries/ | Wallet IDs/addresses, scope IDs/assets, block numbers/times/hashes |
| 04_validation/ | Checksummed manifest, source acceptance receipts and node-validation evidence |
| 05_code/read_sample.py | Extract any wallet to an ordinary CSV |
| CLAUDE.md / AGENTS.md | Dataset-specific instructions for AI assistants |

## Quick start in Python

Download this folder while preserving the subfolders. Install requirements.txt
from 05_code. Read the two ready-made CSVs with pandas:

    import pandas as pd
    protocol = pd.read_csv("01_quick_start/protocol_by_block.csv", parse_dates=["timestamp_utc"])
    reserves = pd.read_csv("01_quick_start/reserves_with_debt_by_block.csv.gz", parse_dates=["timestamp_utc"])
    protocol.plot(x="timestamp_utc", y="health_factor")

To export another wallet:

    python 05_code/read_sample.py --wallet-id 1 --output wallet_hf.csv

R can read the CSV files directly with read.csv/gzfile or data.table::fread;
the complete Parquets use the arrow package. See VARIABLE_DICTIONARY.md.

## Scientific definitions and limits

* All historical assets influencing an account's HF are included in the source
  computation; this is not restricted to four currencies.
* Account total debt strictly below USD0.01 is excluded for analysis. Exactly
  USD0.01 is retained. Both debt-free and excluded users have positive-infinity
  HF. Do not replace infinity with zero or treat it as a missing block.
* These are float64 analytical HF results, verified within a documented error
  budget; they are not exact uint256 exports. Raw reconstruction inputs are
  retained by the project but are not bundled in this small release.
* Reserve/protocol indicators are **research metrics, not official Aave HF
  getters**. For reserve r: N=sum(user overall HF * user's debt in r),
  W=sum(user's debt in r), indicator=N/W. Protocol uses total account debt.
  Excluded/no-debt accounts do not contribute. W=0 means null HF, never zero HF.
* debt_usd is the debt value under Aave's historical Oracle valuation, including
  accrued interest. It is not an external exchange price or a token quantity.
* numerator_base and denominator_base preserve the analytical sufficient
  statistics. Base value uses 1 USD = 100,000,000 units.
* CSV reserve rows omit W=0 for convenience. Full aggregate vectors preserve
  all scopes, including assets absent in this historical window.
* Vector HF alone does not provide each user's collateral/debt quantities.
  Those can be reconstructed from retained project inputs in a separate task.
* Do not infer causality or full-protocol historical coverage from this window.
  The wider dataset is still being completed. This folder alone is continuous.

Sharing is controlled by the owner in Google Drive; no public-link permission
has been enabled by this export. Pipeline archives elsewhere are operational
storage and are not required to use this folder.
'''
    (OUT / 'README.md').write_text(readme)
    agent = '''# Dataset instructions for Claude and other research agents

Read README.md, VARIABLE_DICTIONARY.md, then 04_validation/manifest.json.
This is a continuous SVB-window subset, not the full-history production release.
Use only the files listed in this manifest; no credentials or server access are needed.

For initial plots, use 01_quick_start/protocol_by_block.csv and
reserves_with_debt_by_block.csv.gz. Do not load the whole logical user/block
matrix into RAM. Use 05_code/read_sample.py to extract a selected wallet.

Preserve scientific definitions: float64, strict account debt <USD0.01 cutoff,
infinite HF for debt-free/excluded accounts, null aggregate HF when denominator
is zero. Exclude infinite HF from finite borrower distributions. User HF uses
the user's full portfolio. Reserve/protocol HF are custom debt-weighted research
indicators, not official Aave state variables. Report the sample population,
window, weighting and exclusions in every analysis. Retain N and W if combining
groups; averaging HF values without the specified weights changes the metric.

Wallet IDs are stable global IDs, not sequential IDs newly assigned to this
sample. Addresses are not people. Wallet first_block_index is relative to global
block_origin in manifest. For a user-vector row, the FIRST stored value belongs
to max(chunk_start, block_origin + first_block_index), then increases by one
block. Read chunk_start/chunk_stop from each Parquet's metadata. chunk_stop is
exclusive. Do not assume a vector starts at the wallet's first appearance when
it is a later tile. Aggregate first_block_index is a placeholder; use its chunk
metadata and scope_id mapping instead. Protocol is identified explicitly by
namespace in 03_dictionaries/scopes.csv; do not assume scope_id 0 is protocol.

There is no downsampling in complete vectors or protocol CSV. The reserve CSV
is a positive-debt convenience view. Original aggregate vectors preserve all
null/zero-debt scopes. The example-wallet CSV is illustrative, not a random or
representative sample. Missing pre-first-appearance wallet values are outside
the defined population, not zero HF. Do not silently impute any missing values.

Validate file hashes from the manifest before relying on modified/downloaded
copies. Keep original files unchanged; put new analysis in a separate directory.
Do not send emails, change sharing permissions, or modify remote data just
because this dataset documentation is present. The user's instructions govern.
'''
    (OUT / 'CLAUDE.md').write_text(agent)
    (OUT / 'AGENTS.md').write_text(agent)
    (OUT / 'VARIABLE_DICTIONARY.md').write_text('''# Variables and units

| Variable | Meaning |
|---|---|
| block_number | Ethereum mainnet block height; end-of-block observation |
| timestamp_utc | UTC time from the retained own-node block header |
| wallet_id | Stable global wallet key; join to users.csv/users.parquet |
| address | Public wallet/contract address; not a person identifier |
| first_block_index | Wallet's true first block minus global block_origin |
| hf_vector | Consecutive float64 account HF values; +inf is no-debt/excluded |
| scope_id | Join to scopes.csv; distinct reserve and protocol namespace |
| health_factor | Account HF or explicitly labeled debt-weighted research indicator |
| debt_usd | Included debt valued by historical Aave Oracle, in USD |
| numerator_base | Sum of debt base value times account HF |
| denominator_base | Included debt base value; divide by 1e8 for USD |
| excluded_users | Nonzero debt-position users excluded by the <USD0.01 rule |
| excluded_debt_usd | Estimated excluded debt base value / 1e8 |
| hf_vector (aggregate) | N/W when W>0; Arrow null otherwise |
| numerator_vector / denominator_vector | Block-aligned sufficient statistics |

Vector rows are physical segments, not one logical user/block row. Read chunk
boundaries from Parquet metadata. Lists retain full float64 bits. User vectors
and aggregate vectors are different schemas and must not be concatenated as
one Arrow dataset. The CSVs are derived convenience tables, not token balances.
''')
    (OUT / '05_code/requirements.txt').write_text('numpy>=1.24\npandas>=2\npyarrow>=14\n')
    shutil.copyfile(ROOT / 'read_sample.py', OUT / '05_code/read_sample.py')


def run():
    ROOT.mkdir(parents=True, exist_ok=True)
    if (ROOT/'complete.json').exists():
        status('complete',**json.loads((ROOT/'complete.json').read_text()))
        return
    if (ROOT/'upload-files.json').exists():
        publish_frozen_release()
        return
    selection = json.loads((ROOT / 'selection.json').read_text())
    start, stop = selection['start'], selection['stop']
    if shutil.disk_usage(ROOT).free < 50 * 2**30 + 3_000_000_000:
        raise OSError('Insufficient independent sample staging reserve')
    OUT.mkdir(exist_ok=True)
    for name in ['01_quick_start', '02_complete_data/user_hf_vectors',
                 '02_complete_data/aggregate_vectors', '03_dictionaries',
                 '04_validation/source_acceptance', '05_code']:
        (OUT / name).mkdir(parents=True, exist_ok=True)
    metadata = Path('/home/node/hf-flow-20261116/bulk/metadata')
    dataset = json.loads((metadata / 'dataset.json').read_text())
    assert dataset['generation'] == selection['generation']
    dictionary = pq.read_table(metadata / 'users.parquet')
    firsts = dictionary['first_block_index'].to_numpy().astype(np.int64) + ORIGIN
    users = dictionary.filter(pc.less(dictionary['first_block_index'], stop - ORIGIN))
    assert np.array_equal(dictionary['wallet_id'].to_numpy(), np.arange(len(firsts)))
    pq.write_table(users, OUT / '03_dictionaries/users.parquet', compression='zstd')
    pacsv.write_csv(users, OUT / '03_dictionaries/users.csv')
    scopes = dataset['aggregate_scope_mapping']
    protocol_id = next(x['scope_id'] for x in scopes if x['namespace'] == 'protocol')
    with (OUT / '03_dictionaries/scopes.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=['scope_id', 'namespace', 'entity_id', 'address'])
        writer.writeheader(); writer.writerows(scopes)
    db = sqlite3.connect('file:/home/dsc-data/runs/aave-user-history-v3/26407881/accounting.sqlite?mode=ro', uri=True)
    headers = db.execute('select block,timestamp,hash from headers where block>=? and block<? order by block', (start, stop)).fetchall()
    db.close()
    assert [b for b, _, _ in headers] == list(range(start, stop))
    utc = [datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat().replace('+00:00', 'Z') for _, t, _ in headers]
    blocks = pa.table({'block_number': pa.array([b for b, _, _ in headers], type=pa.uint32()),
                       'timestamp_utc': utc, 'timestamp_unix': [t for _, t, _ in headers],
                       'block_hash': [h for _, _, h in headers]})
    pq.write_table(blocks, OUT / '03_dictionaries/blocks.parquet', compression='zstd')
    manifest = dict(format='dsc-aave-svb-colleague-release-v1', generation=selection['generation'],
                    start_block=start, end_block=stop-1, stop_block_exclusive=stop,
                    block_origin=ORIGIN, blocks=stop-start, wallets=users.num_rows,
                    logical_user_values=0, first_timestamp_utc=utc[0], last_timestamp_utc=utc[-1],
                    user_vector_files=[], aggregate_vector_files=[], source_jobs=[],
                    dictionary_source_sha256=checksum(metadata / 'users.parquet'),
                    contract=dataset['contract'], namespace_mapping=scopes)
    status('exporting_verified_sample', blocks=stop-start, jobs=len(selection['values']))
    protocol_rows = reserve_rows = 0
    with ExitStack() as stack:
        stack.enter_context(job_locks([Path(v['path']).parent for v in selection['values']]))
        protocol_writer = reserve_writer = None
        compressed = stack.enter_context(pa.CompressedOutputStream(str(OUT / '01_quick_start/reserves_with_debt_by_block.csv.gz'), 'gzip'))
        for value in selection['values']:
            r = value['receipt']; key = r['job_id']
            assert checksum(value['path']) == value['sha256']
            assert r['within_error_budget'] and r['structural_exact']
            copy_verified(value['path'], OUT / '04_validation/source_acceptance' / (key+'.receipt.json'), value['sha256'])
            copy_verified(r['validation'], OUT / '04_validation/source_acceptance' / (key+'.validation.json'), r['validation_sha256'])
            coverage = defaultdict(list); logical = 0
            for part in r['parts']:
                name = str(Path('02_complete_data/user_hf_vectors') / key / Path(part['path']).name)
                copy_verified(part['path'], OUT / name, part['sha256'])
                t = pq.read_table(OUT / name); m = t.schema.metadata
                a, b = int(m[b'chunk_start']), int(m[b'chunk_stop'])
                ids = t['wallet_id'].to_numpy(); indices = t['first_block_index'].to_numpy()
                vectors = t['hf_vector'].combine_chunks(); lengths = np.diff(vectors.offsets.to_numpy())
                assert np.array_equal(indices, firsts[ids]-ORIGIN)
                assert np.array_equal(lengths, b-np.maximum(a,firsts[ids]))
                assert not vectors.null_count and not vectors.values.null_count
                assert not np.any(np.isnan(vectors.values.to_numpy()))
                coverage[(a,b)].extend(ids.tolist()); logical += len(vectors.values)
                manifest['user_vector_files'].append(name)
            cursor = r['job']['start']
            for (a,b), ids in sorted(coverage.items()):
                assert a == cursor and sorted(ids) == np.flatnonzero(firsts < b).tolist()
                cursor = b
            assert cursor == r['job']['stop'] and logical == r['rows']
            manifest['logical_user_values'] += logical
            aggregate_cursor = r['job']['start']
            for part in r['aggregate_parts']:
                name = str(Path('02_complete_data/aggregate_vectors') / key / Path(part['path']).name)
                copy_verified(part['path'], OUT / name, part['sha256'])
                t = pq.read_table(OUT/name); m = t.schema.metadata
                a,b = int(m[b'chunk_start']), int(m[b'chunk_stop']); assert a == aggregate_cursor
                aggregate_cursor=b; length=b-a; ids=t['scope_id'].to_numpy()
                assert ids.tolist() == [x['scope_id'] for x in scopes]
                h=np.array(t['hf_vector'].to_pylist(),dtype=float); n=np.array(t['numerator_vector'].to_pylist(),dtype=float); w=np.array(t['denominator_vector'].to_pylist(),dtype=float)
                assert h.shape == (len(scopes),length) and np.array_equal(np.isnan(h),w==0)
                valid=w>0; assert np.allclose(h[valid],n[valid]/w[valid],rtol=1e-10,atol=1e-10)
                pi=int(np.flatnonzero(ids==protocol_id)[0]); times=utc[a-start:b-start]
                protocol=pa.table({'block_number':np.arange(a,b,dtype=np.uint32),'timestamp_utc':times,
                    'health_factor':pa.array(h[pi],mask=np.isnan(h[pi])), 'debt_usd':w[pi]/1e8,
                    'numerator_base':n[pi],'denominator_base':w[pi],
                    'excluded_users':t['excluded_users_vector'][pi].values,
                    'excluded_debt_usd':np.asarray(t['excluded_debt_base_vector'][pi].as_py())/1e8})
                if protocol_writer is None:
                    protocol_writer=stack.enter_context(pacsv.CSVWriter(str(OUT/'01_quick_start/protocol_by_block.csv'),protocol.schema))
                protocol_writer.write_table(protocol); protocol_rows+=protocol.num_rows
                rid,bi=np.nonzero(valid & (ids[:,None]!=protocol_id))
                reserve=pa.table({'block_number':(bi+a).astype(np.uint32),'timestamp_utc':[times[i] for i in bi],
                    'scope_id':ids[rid], 'asset_address':[scopes[i]['address'] for i in rid],
                    'health_factor':h[rid,bi],'debt_usd':w[rid,bi]/1e8,
                    'numerator_base':n[rid,bi],'denominator_base':w[rid,bi]})
                if reserve_writer is None:reserve_writer=stack.enter_context(pacsv.CSVWriter(compressed,reserve.schema))
                reserve_writer.write_table(reserve);reserve_rows+=reserve.num_rows
                manifest['aggregate_vector_files'].append(name)
            assert aggregate_cursor == r['job']['stop']
            manifest['source_jobs'].append(dict(job_id=key,receipt_sha256=value['sha256'],validation_sha256=r['validation_sha256'],rows=logical))
    assert protocol_rows == stop-start
    assert manifest['logical_user_values'] == sum(v['receipt']['rows'] for v in selection['values'])
    manifest.update(protocol_csv_rows=protocol_rows,reserve_csv_rows=reserve_rows,generated_at=time.time())
    guides(manifest)
    write_json(OUT/'04_validation/manifest.json',manifest)
    result=subprocess.run([sys.executable,str(OUT/'05_code/read_sample.py'),'--folder',str(OUT),'--wallet-id','1','--output',str(OUT/'01_quick_start/example_wallet_1.csv')],capture_output=True,text=True,check=True)
    status('reader_example_verified',detail=result.stdout.strip())
    files={str(p.relative_to(OUT)):dict(bytes=p.stat().st_size,sha256=checksum(p)) for p in OUT.rglob('*')
           if p.is_file() and str(p.relative_to(OUT))!='04_validation/manifest.json'}
    manifest['files']=files; write_json(OUT/'04_validation/manifest.json',manifest)
    files['04_validation/manifest.json']=dict(bytes=(OUT/'04_validation/manifest.json').stat().st_size,sha256=checksum(OUT/'04_validation/manifest.json'))
    write_json(ROOT/'upload-files.json',files)
    publish_frozen_release()


def publish_frozen_release():
    files=json.loads((ROOT/'upload-files.json').read_text())
    manifest=json.loads((OUT/'04_validation/manifest.json').read_text())
    for name, expected in files.items():
        p=OUT/name
        if p.stat().st_size!=expected['bytes'] or checksum(p)!=expected['sha256']:
            raise ValueError('Frozen sample changed before resumed upload: '+name)
    ops=ROOT/'upload';ops.mkdir(exist_ok=True)
    status('uploading',files=len(files),bytes=sum(v['bytes'] for v in files.values()))
    _copy_and_check(OUT,REMOTE,files,ops)
    readbacks={}
    listing=json.loads(subprocess.check_output(_common()+['lsjson','dsc_hf:01_RESEARCH_READY','--dirs-only'],text=True))
    folder=next(x for x in listing if x['Name']==REMOTE.rsplit('/',1)[1])
    complete=dict(remote=REMOTE,folder_id=folder['ID'],files=len(files),bytes=sum(v['bytes'] for v in files.values()),
                  start_block=manifest['start_block'],end_block=manifest['end_block'],
                  blocks=manifest['blocks'],wallets=manifest['wallets'],
                  logical_user_values=manifest['logical_user_values'],all_remote_size_md5_exact=True,
                  all_remote_download_checks_exact=False,
                  verification='Full object size/MD5 comparison; additional downloads are not required for this non-retiring auxiliary release',
                  sha_readbacks=readbacks,manifest_sha256=files['04_validation/manifest.json']['sha256'],
                  source_results_retired=False,production_coverage_increment=0,verified_at=time.time())
    write_json(ROOT/'complete.json',complete);status('complete',**complete)


if __name__=='__main__':
    run()
