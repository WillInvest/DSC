# Production historical run

`dsc-extraction.service` runs on dsc-data as `dsc-data` and starts at boot.
Systemd restarts a failed coordinator after 60 seconds. It reuses the same local
tracker, fixed cutoff, profiles and registry; destination-accepted coverage is
not discarded. A successful bounded run exits without restarting indefinitely.
This supervises process failure and reboot recovery, not arbitrary application
hangs. Network/worker errors remain visible in the tracker and monitor.

The initial run covers all 102 addresses from deployment through finalized block
26,259,811 inclusive, with state at every block and events/reverted checks for
empty blocks too. The service command pins that cutoff to the 15 reviewed Aave
profiles. Do not remove the cutoff before extending/reviewing profile coverage.

The coordinator uses private LAN SSH, not Tailscale, between workers. The existing
node watchdog keeps RETH/Lighthouse running. Tailscale is used for phone monitor
access only: https://dsc-web.tail5aad6c.ts.net/ .

```bash
sudo systemctl status dsc-extraction.service
journalctl -u dsc-extraction.service -n 50 --no-pager
sudo systemctl stop dsc-extraction.service
sudo systemctl start dsc-extraction.service
```

`--cleanup` removes only acknowledged node staging; it retains the dsc-revert
archive and dsc-data accepted files. No protected datasets or Savva transfer
paths are involved. The current runner retains incoming raw files on dsc-data.
Disk usage must be monitored during this first full backfill; initial free space
was approximately 854 GiB on data, 782 GiB on revert and 11 TiB on node.

Profiles and proof: `contracts/ethereum/aave-profiles/README.md` and its evidence
files. Review receipt: `/tmp/compound-engineering-501/ce-code-review/aave-profiles-20261023`
on the operator workspace (temporary review record, not a runtime dependency).
