#!/usr/bin/env bash
# Run on dsc-node. Cryo block ranges exclude the end block.
set -euo pipefail
if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
  echo "Usage: bash $0 PROTOCOL ADDRESS START:END_EXCLUSIVE [--deployment PROFILE.json]"
  exit 0
fi
if [[ ( $# -ne 3 && $# -ne 5 ) || ! "$3" =~ ^[0-9]+:[0-9]+$ ]]; then
  echo "Usage: bash $0 PROTOCOL ADDRESS START:END_EXCLUSIVE [--deployment PROFILE.json]" >&2
  exit 2
fi
PROTOCOL="$1"; POOL_ADDRESS="$2"; BLOCKS="$3"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/pool_common.sh"
if [[ $# -eq 5 && "$4" != "--deployment" ]]; then
  echo 'Expected --deployment PROFILE.json' >&2; exit 2
fi
DEPLOYMENT="${5:-}"
"$PYTHON_BIN" -c 'import sys; a,b=map(int,sys.argv[1].split(":")); sys.exit(0 if a < b else 2)' "$BLOCKS"
OUTPUT_DIR="${OUTPUT_DIR:-/home/node/dsc-extraction/data/$PROTOCOL/$POOL_ADDRESS/events-raw}"
if [[ "$REQUIRES_DEPLOYMENT" == "1" ]]; then
  [[ -n "$DEPLOYMENT" ]] || { echo 'A reviewed --deployment profile is required' >&2; exit 2; }
  "$PYTHON_BIN" "$SCRIPT_DIR/check_deployment.py" "$PROTOCOL" "$POOL_ADDRESS" "$BLOCKS" "$OUTPUT_DIR" "$DEPLOYMENT"
fi
"${CRYO_BIN:-/home/node/.cargo/bin/cryo}" logs \
  --rpc "${ETH_RPC_URL:-http://127.0.0.1:8545}" \
  --contract "$POOL_ADDRESS" \
  --blocks "$BLOCKS" \
  --output-dir "$OUTPUT_DIR" \
  --chunk-size "$CHUNK_SIZE" \
  --inner-request-size "${BLOCKS_PER_REQUEST:-1000}" \
  --max-concurrent-requests "$MAX_CONCURRENT_REQUESTS" \
  --max-concurrent-chunks "$MAX_CONCURRENT_CHUNKS" \
  --include-columns block_hash \
  --hex \
  --label "${PROTOCOL}_${POOL_ADDRESS}_raw_events_v1"

if [[ "$REQUIRES_DEPLOYMENT" == "1" ]]; then
  "$PYTHON_BIN" "$SCRIPT_DIR/check_deployment.py" "$PROTOCOL" "$POOL_ADDRESS" "$BLOCKS" "$OUTPUT_DIR" "$DEPLOYMENT"
fi
