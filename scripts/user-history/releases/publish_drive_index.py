"""Publish a readable research entrypoint without moving active archive paths."""
import csv
import json
from pathlib import Path

from shared.artifacts import checksum
from hf_flow_json import write_json
from hf_flow_cloud import _copy_and_check

ROOT = Path('/home/node/hf-colleague-samples/svb-20230303-20230315')
SAMPLE_URL = 'https://drive.google.com/drive/folders/1bVKHLFJVBMxDIe_CJw_5ioBnrQpVB-_Q'


def run():
    completed = json.loads((ROOT / 'complete.json').read_text())
    assert completed['all_remote_size_md5_exact']
    assert completed['folder_id'] == '1bVKHLFJVBMxDIe_CJw_5ioBnrQpVB-_Q'
    manifest = json.loads((ROOT / 'release/04_validation/manifest.json').read_text())
    out = ROOT / 'drive_index';out.mkdir(exist_ok=True)
    start = f'''# DSC Aave research data — start here / 从这里开始

## For researchers and colleagues

Open the verified continuous SVB sample:

**[Aave V3 Ethereum: 3–15 March 2023]({SAMPLE_URL})**

This folder contains {completed['blocks']:,} continuous end-of-block observations,
{completed['wallets']:,} historical addresses and
{completed['logical_user_values']:,} user/block HF values. It is approximately
{completed['bytes']/1e6:.1f}MB, with the original accepted vectors and ready-made
plotting CSVs. The first and last dates are partial days; exact UTC boundaries
are in its README and manifest.

### Read in this order

1. README.md — scope, definitions, directory map and quick start.
2. 01_quick_start/ — protocol/reserve tables, a borrower example and a debt-free example.
3. VARIABLE_DICTIONARY.md and 03_dictionaries/ — units, wallet/asset IDs and dates.
4. 05_code/ — tested reader for any wallet.
5. 02_complete_data/ — complete vectors when deeper analysis is needed.
6. 04_validation/ — checksums, exact source scope and acceptance evidence.

For Claude, Codex and other assistants: open CLAUDE.md or AGENTS.md in the
sample folder before analysis. These explain vector indexing, infinity/null
semantics, exclusions and the custom aggregate definitions.

## Storage layout

* **01_RESEARCH_READY/**: verified self-contained releases for analysis and sharing.
* **DATA_CATALOG.csv**: index of published research releases and their exact scope.
* **26407881/**: internal pipeline archive/recovery area, named for the finalized
  cutoff block. It contains operational/test/batch objects, not an analysis-ready
  claim of complete history. See its internal README.

The larger all-history project is still running. Data-server gaps in that wider
project do not create gaps inside the verified continuous SVB sample above.
Reserve/protocol HF are debt-weighted research indicators, not official Aave HF
getters. Account debt strictly below USD0.01 is excluded; exact raw reconstruction
inputs are retained separately by the project.

## Sharing

Share the **sample folder link above**, with viewer access, using Google Drive's
Share dialog. The owner will manage colleague permissions. No public access has
been enabled by this publication. Sharing this root is unnecessary for using
the self-contained sample.
'''
    (out / '00_START_HERE.md').write_text(start)
    agent = '''# DSC Aave research directory: agent entrypoint

Read 00_START_HERE.md and DATA_CATALOG.csv. Use a verified research release under
01_RESEARCH_READY, then read its README.md, VARIABLE_DICTIONARY.md,
CLAUDE.md/AGENTS.md and 04_validation/manifest.json. Start with its plotting CSVs.

The numeric 26407881 directory is internal pipeline/archive storage. Do not infer
research coverage from directory names, TAR counts, or the presence of files.
Use only the explicit validated release scope. The continuous SVB sample is not
the whole project's complete history. Full user vectors and custom reserve/
protocol indicators have distinct schemas; preserve their definitions, debt
weights, strict USD0.01 cutoff and infinity/null semantics. Complete instructions
and a tested wallet reader are bundled with each release. Keep new analysis
separate from the original release files.
'''
    (out / 'CLAUDE.md').write_text(agent)
    (out / 'AGENTS.md').write_text(agent)
    with (out / 'DATA_CATALOG.csv').open('w') as f:
        fields = ['dataset','status','protocol','chain','start_block','end_block',
                  'first_timestamp_utc','last_timestamp_utc','blocks','wallets',
                  'logical_user_hf_values','bytes','files','continuous',
                  'full_history','folder_url','manifest_sha256']
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        writer.writerow(dict(dataset='Aave V3 SVB continuous sample v1',status='uploaded_and_checksum_verified',
            protocol='Aave V3 Ethereum main market',chain='Ethereum mainnet',
            start_block=completed['start_block'],end_block=completed['end_block'],
            first_timestamp_utc=manifest['first_timestamp_utc'],last_timestamp_utc=manifest['last_timestamp_utc'],
            blocks=completed['blocks'],wallets=completed['wallets'],logical_user_hf_values=completed['logical_user_values'],
            bytes=completed['bytes'],files=completed['files'],continuous=True,full_history=False,
            folder_url=SAMPLE_URL,manifest_sha256=completed['manifest_sha256']))
    internal=out/'26407881';internal.mkdir(exist_ok=True)
    (internal/'00_INTERNAL_ARCHIVE_README.md').write_text('''# Internal pipeline archives — not the research entrypoint

26407881 is the finalized Ethereum cutoff block used by the larger project.
This directory contains pipeline/test batches, packed objects, manifests and
restore indices. Archive objects support programmatic verification and recovery;
their presence alone does not prove full-history/user completeness. Existing
paths are stable recovery references used by active jobs.

For human analysis and colleague sharing, use the root 00_START_HERE.md,
DATA_CATALOG.csv and 01_RESEARCH_READY/. Each published research release includes
an explicit continuous scope, plotting CSVs, dictionaries, checksums and agent
instructions. Operational archive verification is tracked separately from these
auxiliary research releases.
''')
    files={str(p.relative_to(out)):dict(bytes=p.stat().st_size,sha256=checksum(p))
           for p in out.rglob('*') if p.is_file()}
    ops=ROOT/'index-upload';ops.mkdir(exist_ok=True)
    _copy_and_check(out,'dsc_hf:',files,ops)
    write_json(ROOT/'index-complete.json',dict(files=files,all_remote_size_md5_exact=True,
                                              sample_folder_id=completed['folder_id'],sharing_changed=False))
    print('Readable Drive entrypoint and agent instructions uploaded and checksum-verified',flush=True)


if __name__=='__main__':
    run()
