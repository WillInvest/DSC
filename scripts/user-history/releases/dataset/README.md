# Aave V3 Ethereum — HF vectors and reconstruction inputs

Start with `DATA_CATALOG.csv`. It records the exact locations and verification
status of each component. A running upload is not an accepted backup. The
dataset is delivered only when all required components and the final acceptance
record are complete.

## Research scope

- Ethereum Aave V3 main market, end-of-block states.
- Blocks **16,291,127–26,407,881**, inclusive: **10,116,755 blocks**.
- **258,131 wallet addresses**, from each address's first appearance onward.
- **910,310,934,885 logical wallet/block HF values**, including exited wallets.
- All historical reserves and the protocol-level research indicator.

The wallet universe includes recipients of aTokens who did not directly call
the Pool. Wallet addresses are not a count of people or current borrowers.

## Folder guide

| Folder | Contents |
|---|---|
| `01_HF_Vectors` | Wallet, reserve and protocol vectors; dictionaries and archive catalog |
| `02_Reconstruction_Inputs/token_events` | Complete planned aToken and debt-token raw events |
| `02_Reconstruction_Inputs/pool_events` | Pool raw events, retaining historical deployment-anchored chunks and explicit tail supplements |
| `02_Reconstruction_Inputs/oracle_prices` | Per-block prices, ordered asset lists, quote units and routing/boundary evidence |
| `02_Reconstruction_Inputs/configuration` | Provider/configurator raw logs, configuration changes and initialization evidence |
| `02_Reconstruction_Inputs/replay_bundle` | Compact historical positions/configuration/interest/header database and the frozen replay runtime |
| `03_Code_and_Definitions` | Pinned scientific code, calculation definitions and reading/reconstruction examples |
| `04_Validation_and_Provenance` | Coverage manifests, checksums, node comparisons and collection logs |

Some archive folders use links to immutable storage locations. Follow the
catalog rather than inferring completeness from a directory listing. Historical
operational path names are compatibility identifiers, not different scientific
datasets. Existing colleague sample links remain valid.

## Read a small amount first

Most data uses Parquet/Zstd inside indexed TAR packages. A few early canonical
segments retain their original raw-file archive format; the catalog identifies
these explicitly. For raw-file archives, read the manifest-listed Parquets
directly and verify their SHA256 against that manifest. TAR packaging
does not change the Parquet bytes or numerical precision. You need not unpack
the whole dataset to read one file.

1. Find the relevant block interval in `01_HF_Vectors/CANONICAL_JOBS.csv` and
   use the matching archive's `drive_url` or `node_path` in `DATA_CATALOG.csv`.
2. Obtain its `pack-index.json` and the required `packs/pack-XXXX.tar`. The index
   maps original member names to their package, offset, size and SHA256.
3. Use `03_Code_and_Definitions/read_archive.py` to list or recover a member:

```sh
python read_archive.py --folder /path/to/archive --pattern '*aggregate_parts*'
python read_archive.py --folder /path/to/archive \
  --member 'exact/member/name/from/index.parquet' \
  --index-sha256 'HASH_FROM_CATALOG' --output account_part.parquet
```

Restoration verifies the pinned index and the recovered member's SHA256 and
refuses to overwrite an existing output. The script needs Python 3.10 or newer;
reading Parquet afterward needs PyArrow. Restore indices and manifests are part
of the data, not optional bookkeeping.

For a ready-to-plot, continuous SVB example, use the separately documented
[SVB sample](https://drive.google.com/drive/folders/1bVKHLFJVBMxDIe_CJw_5ioBnrQpVB-_Q).
It is a subset, not another contribution to full-history coverage.

## Important definitions

Wallet vectors are `float64` outputs of the validated historical model. They
are not stored uint256 account getter results. Total account debt **strictly
below USD0.01** is excluded for analysis; exactly USD0.01 remains included.
Debt-free and excluded wallets retain positive-infinity HF. Excluded wallets
do not enter debt-weighted aggregates; raw reconstruction inputs are retained.

Reserve/protocol HF values are **research indicators**, not official Aave
getters. They are debt-weighted averages of included wallets' whole-account HF.
The numerator and denominator vectors are retained. A zero denominator yields
null HF, not zero. Read `VARIABLE_DICTIONARY.md` and `dataset.json`; do not
assume the protocol has scope ID zero.

## Rebuild balances and omitted intermediate data

The replay bundle preserves sparse position changes, user collateral/eMode
settings, complete block times/hashes, historical reserve configuration,
interest factors, GHO adjustments, historical ABIs and the runtime. Original
Pool/token events and Oracle inputs are also retained separately. The compact
database supplements the original-event archives; it does not replace them.

`rebuild_account.py` delegates to the existing historical `BatchEvaluator`.
It returns actual token balances including interest, raw account values, prices,
normalized interest indices and configuration words. It does not query a node
unless `--verify-node` is explicitly requested. Run it in the documented replay
environment mounted at `/home/dsc-data`; its Python runtime is Linux x86-64
Python 3.10 and the Node executable and Python/JavaScript dependencies are
included in the frozen environment. Scientific source hashes and NumPy identity
are recorded under `03_Code_and_Definitions`.

For a local replay on Linux x86-64 with systemd and Python 3.10, download
`hf-frozen-inputs.tar.gz` from the replay bundle catalog link and
`packs/pack-0000.tar` from the pinned scientific code link. Verify both
archives against their published SHA-256 values. In a fresh working directory,
unpack and run the same account sample included in the validation folder:

```sh
mkdir frozen pinned
tar -xzf hf-frozen-inputs.tar.gz -C frozen
tar -xf pack-0000.tar -C pinned
code="$PWD/pinned/hf-f64-v2-20261116/code"
sudo systemd-run --quiet --wait --pipe --collect --uid="$(id -u)" \
  --property="BindReadOnlyPaths=$PWD/frozen:/home/dsc-data" \
  --property=WorkingDirectory=/home/dsc-data/scripts \
  --setenv="PYTHONPATH=$code:/home/dsc-data/scripts/user-history:/home/dsc-data/scripts" \
  --setenv=PYTHONDONTWRITEBYTECODE=1 \
  /bin/bash -c 'exec /home/dsc-data/scripts/.venv-abi/bin/python "$@"' replay \
  "$PWD/rebuild_account.py" \
  --user 0xc7b42f99c63126b22858f4eed636f805cfe82c91 --block 22434005 \
  --output "$PWD/rebuilt-account.json"
```

Place `03_Code_and_Definitions/rebuild_account.py` in the working directory
before running the command. Compare the output with
`04_Validation_and_Provenance/rebuilt-account-22434005.json`, which also
contains the earlier own-node verification results.

Full per-wallet/per-asset intermediate tables were intentionally not expanded
for storage. They can be regenerated from these inputs; none should be inferred
from HF alone. Preserve the original archives when producing new analyses.
