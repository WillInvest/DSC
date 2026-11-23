# DSC data file naming

Data files under `/mnt/storage/dsc-data` start with `START-END`.
Both block numbers are inclusive and describe the file itself. File lengths
can vary. Use `__` between filename fields.

| Folder | Filename |
| --- | --- |
| `dex/events/`, `lending/events/` | `START-END__PROTOCOL__ADDRESS__EVENT__SCHEMA.parquet` |
| `dex/block-state/`, `lending/block-state/` | `START-END__PROTOCOL__ADDRESS.parquet` |
| `dex/reverted-tx/`, `lending/reverted-tx/` | `START-END__PROTOCOL.jsonl.gz` |
| `revert/receipts/`, `revert/traces/`, `revert/selected/` | `START-END.jsonl.gz` |
| `lending/health-factor/wallets/` | `START-END__wallet-ids-FIRST-LAST.parquet` |
| `lending/health-factor/reserves/`, `lending/health-factor/protocol/` | `START-END.parquet` |

`PROTOCOL` is `uniswap-v2`, `uniswap-v3`, `aave-v2`, or `aave-v3`.
`ADDRESS` is the lowercase contract address. `EVENT` is the ABI event name
and is case-sensitive. `SCHEMA` identifies the decoder profile.

## Health factor

`26478269-26478300__wallet-ids-30000-59999.parquet` covers blocks
26,478,269 through 26,478,300 and wallet IDs 30,000 through 59,999.
Both ranges are inclusive in the filename. In the Parquet footer,
`chunk_stop` and `wallet_stop` are exclusive.

Use the accepted wallet dictionary for the queried interval to look up the
address. The `wallet_id` selects a row and `hf_vector` contains the values.
Find historical interval-to-dictionary assignments in
`metadata/query-layout/wallet-jobs.csv`. Wallet dictionaries are under
`metadata/query-layout/dictionaries/`, keyed by batch ID for historical files
or by the full accepted interval for incremental files.

Reserve and protocol files contain `all`, `emode`, and `non_emode` rows.
Filter the `cohort` column. Resolve reserve `scope_id` using
`metadata/query-layout/historical-scopes.json` or the applicable
`metadata/query-layout/dictionaries/<window>.scopes.json`.

## Reading accepted data

Check `metadata/query-publication.json` for `state = complete`,
`naming = range-first-v1`, `market_through`, and `hf_through`.
Select filenames that overlap the query range. For HF, use the HF watermark.
For the global Revert archive, use `metadata/global-revert-publication.json`.
