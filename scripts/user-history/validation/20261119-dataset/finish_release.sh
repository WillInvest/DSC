#!/usr/bin/env bash
# Operator-side continuation for the fixed, accepted 2026-11-19 release.
# Prepares and publishes release metadata only. No retirement.
set -euo pipefail
umask 077
work=$(mktemp -d /private/tmp/dsc-hf-final-release.XXXXXXXX)
node=node@100.108.171.14
compute=fao@100.119.128.31
authority=/home/fao/hf-flow-control-20261117
entrypoint=/home/node/aave-research-dataset
staging="$entrypoint/.final-release-20261119"
deadline=$((SECONDS + 14400))

printf 'Waiting for the canonical Compute acceptance receipt\n'
until ssh -o ConnectTimeout=10 "$compute" 'test -f ~/hf-flow-control-20261117/complete.json'; do
    if (( SECONDS >= deadline )); then
        printf 'Canonical acceptance wait expired; release files unchanged\n' >&2
        exit 1
    fi
    sleep 30
done

ssh "$compute" 'set -eu
    r="$HOME/hf-flow-control-20261117"
    frozen="$HOME/aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d"
    cd "$r"
    PYTHONPATH="$r/code:$frozen/scripts" python3 "$r/code/export_job_catalog.py" \
        --ledger "$r/flow.sqlite" --destination "$r/final-delivery-20261119"'
scp "$compute:$authority/final-delivery-20261119/CANONICAL_JOBS.csv" \
    "$compute:$authority/final-delivery-20261119/job-catalog-snapshot.json" "$work/"
ssh "$node" 'umask 077
    mkdir -p ~/aave-research-dataset/.final-release-20261119/{01_HF_Vectors,04_Validation_and_Provenance}'
scp "$work/CANONICAL_JOBS.csv" "$node:$staging/01_HF_Vectors/"
scp "$work/job-catalog-snapshot.json" "$node:$staging/04_Validation_and_Provenance/"
scp docs/data-collection/aave-v3/events.jsonl "$node:$staging/04_Validation_and_Provenance/collection-events.jsonl"

ssh "$node" 'set -eu
    root="$HOME/aave-research-dataset"
    stage="$root/.final-release-20261119"
    flow="$HOME/hf-flow-20261116"
    frozen="$HOME/aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d"
    export PYTHONPATH="$flow/code-v5:$frozen/scripts"
    python3 "$flow/code-v5/audit_job_archives.py" \
        --root "$stage" --primary "$flow/storage/primary-archives" \
        --output "$stage/04_Validation_and_Provenance/canonical-archive-audit.json"'
printf 'Full canonical archive audit passed\n'

unit=dsc-aave-v3-hf-readable-metadata.service
until [ "$(ssh -o ConnectTimeout=10 "$node" 'systemctl show dsc-aave-v3-hf-readable-metadata.service -p ActiveState --value')" = inactive ]; do
    if (( SECONDS >= deadline )); then
        printf 'Initial metadata publisher is still running; final audit is staged on Node\n' >&2
        exit 1
    fi
    sleep 30
done

ssh "$node" 'set -eu
    root="$HOME/aave-research-dataset"
    stage="$root/.final-release-20261119"
    flow="$HOME/hf-flow-20261116"
    frozen="$HOME/aave-hf-distributed/5197dae89b140ccb/images/1d1a9be72b345989a4a8667573e929c66320535824f480517e6e440633d0e32d"
    export PYTHONPATH="$flow/code-v5:$frozen/scripts"
    for f in 01_HF_Vectors/CANONICAL_JOBS.csv \
        04_Validation_and_Provenance/job-catalog-snapshot.json \
        04_Validation_and_Provenance/collection-events.jsonl \
        04_Validation_and_Provenance/canonical-archive-audit.json; do
        install -m 600 "$stage/$f" "$root/$f.final-incoming"
        mv -f "$root/$f.final-incoming" "$root/$f"
    done
    install -m 644 "$flow/code-v5/dataset-reader-20261119.md" "$root/README.md.final-incoming"
    mv -f "$root/README.md.final-incoming" "$root/README.md"
    python3 "$flow/code-v5/build-dataset-entrypoint-20261119.py"
    python3 -c "import json; from pathlib import Path; d=json.loads((Path.home()/\"aave-research-dataset/04_Validation_and_Provenance/dataset-status.json\").read_text()); assert d[\"all_catalog_components_accepted\"] and d[\"components\"] == 12"
    install -m 644 "$flow/code-v5/publish-dataset-metadata-final-20261119.py" "$flow/code-v5/publish-dataset-metadata-20261119.py.final-incoming"
    mv -f "$flow/code-v5/publish-dataset-metadata-20261119.py.final-incoming" "$flow/code-v5/publish-dataset-metadata-20261119.py"
    sudo systemctl start --no-block dsc-aave-v3-hf-readable-metadata.service'
printf 'Final authenticated catalog publication started on Node\n'
