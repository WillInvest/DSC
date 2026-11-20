#!/usr/bin/env bash
# One-shot continuation of the already authorized, manifest-scoped retirement.
# The audit and published metadata must pass read_dataset_gate before any unlink.
set -euo pipefail
umask 077
work=$(mktemp -d /private/tmp/dsc-hf-final-retirement.XXXXXXXX)
node=node@100.108.171.14
compute=fao@100.119.128.31
data=dsc-data@100.79.189.8
root=/home/node/aave-research-dataset
final="$root/.data-retirement-requests-20261119"
data_requests=/var/lib/dsc-aave-hf-flow/20261116/local/transfers/final-dataset-retirement-requests-20261119
deadline=$((SECONDS + 14400))
printf 'Waiting for final published Node and Drive receipt\n'
until ssh -o ConnectTimeout=10 "$node" 'python3 - <<'"'"'PY'"'"'
import json
from pathlib import Path
p=Path.home()/"aave-research-dataset/.metadata-publication/complete.json"
if not p.is_file():
    raise SystemExit(1)
r=json.loads(p.read_text())
if not r.get("all_remote_sha256_exact") or "04_Validation_and_Provenance/canonical-archive-audit.json" not in r["files"]:
    raise SystemExit(1)
PY'; do
    if (( SECONDS >= deadline )); then
        printf 'Final acceptance wait expired. No cleanup started.\n' >&2
        exit 1
    fi
    sleep 30
done

# This resolves the final gate afresh, rechecks the matched archive indices,
# and performs the exact Drive range readbacks for the planned Data groups.
ssh "$node" 'set -eu
    root="$HOME/aave-research-dataset"
    code="$HOME/hf-flow-20261116/code-v5"
    frozen="$root/.replay-validation/frozen"
    sudo systemd-run --quiet --wait --pipe --collect --unit=dsc-hf-final-data-retirement-proofs-20261119 --uid=node \
        --property="BindReadOnlyPaths=$frozen:/home/dsc-data" \
        --property=WorkingDirectory=/home/dsc-data/scripts \
        --setenv="PYTHONPATH=$code:$HOME/hf-f64-v2-20261116/code:/home/dsc-data/scripts/user-history:/home/dsc-data/scripts" \
        --setenv=PYTHONDONTWRITEBYTECODE=1 \
        /bin/bash -c '"'"'exec /home/dsc-data/scripts/.venv-abi/bin/python "$@"'"'"' proof \
        "$code/prepare_data_retirement.py" --root "$root" \
        --groups "$root/.data-cleanup-groups-20261119" \
        --members "$root/.data-cleanup-member-proof-20261119" \
        --output "$root/.data-retirement-requests-20261119"'
scp -r "$node:$final/." "$work/"
prepared_sha=$(shasum -a 256 "$work/prepared.json" | cut -d ' ' -f 1)
acceptance_sha=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["dataset_acceptance"]["publication_sha256"])' "$work/prepared.json")
[[ "$acceptance_sha" =~ ^[0-9a-f]{64}$ ]]
current_sha=$(ssh "$node" 'sha256sum ~/aave-research-dataset/.metadata-publication/complete.json | cut -d " " -f 1')
test "$acceptance_sha" = "$current_sha"

ssh "$data" "umask 077; mkdir -p '$data_requests'"
scp "$work/prepared.json" "$work/"data-final-20261119-*.json "$data:$data_requests/"
data_cli="cd /var/lib/dsc-aave-hf-flow/20261116; PYTHONPATH=code-v5:/home/dsc-data/scripts python3 code-v5/apply_data_retirement.py --requests '$data_requests' --prepared-sha256 '$prepared_sha' --acceptance-sha256 '$acceptance_sha'"
ssh "$data" "$data_cli"
printf 'Final publication and Data preflight accepted\n'

# Preserve the previous service code and activate only the retirement unit.
ssh "$compute" 'set -eu
    unit=dsc-aave-v3-hf-legacy-helper-retirement.service
    r="$HOME/hf-flow-control-20261117"
    test "$(systemctl show "$unit" -p ActiveState --value)" = inactive
    if [ ! -e "$r/retire_legacy_helpers.before-final-dataset.py" ]; then
        cp -p "$r/retire_legacy_helpers.py" "$r/retire_legacy_helpers.before-final-dataset.py"
    fi
    install -m 644 "$r/retire_legacy_helpers.final-dataset-staged.py" "$r/retire_legacy_helpers.py.final-incoming"
    mv -f "$r/retire_legacy_helpers.py.final-incoming" "$r/retire_legacy_helpers.py"
    sudo systemctl start --no-block "$unit"'

ssh "$data" "$data_cli --apply"
scp "$data:$data_requests/complete.json" "$work/data-complete.json"
printf 'Data retirement complete; helper retirement unit active or terminal. Evidence at %s\n' "$work"
