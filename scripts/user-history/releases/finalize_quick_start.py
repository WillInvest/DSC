"""Add a finite borrower example to the private draft, preserving base verification."""
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from shared.artifacts import checksum
from hf_flow_json import write_json
from hf_flow_cloud import _common, _copy_and_check

ROOT = Path('/home/node/hf-colleague-samples/svb-20230303-20230315')


def run():
    done=ROOT/'quick-start-complete.json'
    if done.exists():
        print('Quick start already finalized; preserve immutable published draft',flush=True)
        return
    complete=json.loads((ROOT/'complete.json').read_text())
    assert complete['all_remote_size_md5_exact']
    backup=ROOT/'initial-complete.json'
    if not backup.exists():shutil.copy2(ROOT/'complete.json',backup)
    out=ROOT/'release';relative='01_quick_start/example_borrower_wallet_0.csv'
    subprocess.run([sys.executable,str(out/'05_code/read_sample.py'),'--folder',str(out),
                    '--wallet-id','0','--output',str(out/relative)],check=True)
    readme='01_quick_start/README.md'
    (out/readme).write_text('''# First visualizations

1. protocol_by_block.csv: plot timestamp_utc against health_factor.
2. reserves_with_debt_by_block.csv.gz: filter asset_address or scope_id, then
   plot each reserve separately. Join scope_id to ../03_dictionaries/scopes.csv.
3. example_borrower_wallet_0.csv: an illustrative account with borrowing in this
   historical window; use this for a first finite user-HF plot.
4. example_wallet_1.csv: a debt-free-account illustration in this window. Its HF
   is infinity throughout; do not draw it as a finite borrower series or set it
   to zero. It is useful for understanding the sample's no-debt semantics.

Neither example wallet is a random or representative user sample. The complete
all-wallet vectors are in ../02_complete_data/. Extract any wallet using the
tested ../05_code/read_sample.py. Protocol/reserve HF are custom debt-weighted
research indicators, not official Aave getters. See the root README and
VARIABLE_DICTIONARY for definitions and the strict USD0.01 debt exclusion rule.
''')
    changed=[relative,readme]
    path=out/'04_validation/manifest.json';manifest=json.loads(path.read_text())
    for n in changed:manifest['files'][n]=dict(bytes=(out/n).stat().st_size,sha256=checksum(out/n))
    manifest['quick_start_examples']=dict(borrower_wallet_id=0,debt_free_wallet_id=1,
                                        illustrative_only=True)
    write_json(path,manifest)
    changed.append('04_validation/manifest.json')
    files=json.loads((ROOT/'upload-files.json').read_text())
    for n in changed:files[n]=dict(bytes=(out/n).stat().st_size,sha256=checksum(out/n))
    remote=complete['remote'];log=['--log-file',str(ROOT/'quick-start-rclone.log'),'--log-level','INFO']
    for n in changed:
        subprocess.run(_common()+['copyto',str(out/n),remote+'/'+n,'--checksum']+log,
                       capture_output=True,check=True,timeout=1800)
    ops=ROOT/'final-verification';ops.mkdir(exist_ok=True)
    # Keep the accepted base proof; only these three objects were added/changed.
    _copy_and_check(out,remote,{n:files[n] for n in changed},ops)
    proofs={n:dict(bytes=files[n]['bytes'],sha256=files[n]['sha256'],remote_size_md5_exact=True) for n in changed}
    complete.update(files=len(files),bytes=sum(v['bytes'] for v in files.values()),
                    manifest_sha256=files['04_validation/manifest.json']['sha256'],
                    supplemental_checksum_checks=proofs,verified_at=time.time(),
                    verification='Unchanged objects retain the base size/MD5 proof; amended manifest and two new files passed size/MD5 comparison. SHA256 values describe local frozen bytes, not remote SHA readbacks. No full download pass or source retirement claimed')
    write_json(ROOT/'upload-files.json',files)
    write_json(ROOT/'complete.json',complete)
    write_json(done,dict(verified_at=time.time(),files=changed,checksum_checks=proofs,
                        initial_verification=str(backup)))
    print('Borrower and debt-free examples documented, uploaded and verified',flush=True)


if __name__=='__main__':
    run()
