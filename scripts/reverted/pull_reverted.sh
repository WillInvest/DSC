#!/usr/bin/env bash
# Run on dsc-revert. Data still flows directly from node to archive.
set -euo pipefail
if [[ $# -ne 2 || ! "$1" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ ]]; then
  echo "Usage: $0 ARCHIVE_NAME START:END_EXCLUSIVE" >&2
  exit 2
fi
ARCHIVE_ROOT="${ARCHIVE_ROOT:-/home/dsc-revert/data/archive/reverted}"
mkdir -p "$ARCHIVE_ROOT/$1"
rsync -rtc --checksum --partial-dir=.rsync-partial \
  -e 'ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=3' \
  "node@100.108.171.14:/home/node/dsc-reverted/output/$1/blocks" "$ARCHIVE_ROOT/$1/"
python3 "$(dirname "$0")/select_reverted.py" --archive "$ARCHIVE_ROOT/$1" --blocks "$2"
