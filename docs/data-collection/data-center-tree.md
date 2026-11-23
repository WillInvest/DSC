# DSC data center · file tree

Direct query files on Node `vdb1`.

```text
/mnt/storage/dsc-data/
├── dex/
│   ├── events
│   ├── block-state
│   └── reverted-tx
├── lending/
│   ├── events
│   ├── block-state
│   ├── reverted-tx
│   └── health-factor
│       ├── wallets
│       ├── reserves
│       └── protocol
└── revert/
    ├── receipts
    ├── traces
    └── selected
```

Each leaf contains data files directly. Protocol and address are filename fields for filtering where applicable. HF has one continuous series on either side of the historical cutoff. Wallet HF covers all wallets. Reserve and protocol HF support `all`, `emode`, and `non_emode`.

Read `metadata/query-publication.json` for the accepted watermarks. File ranges are inclusive at both ends. The initial publication is ready when its state is `complete`.

See the [file naming README](file-naming/README.md) for exact patterns and examples.
