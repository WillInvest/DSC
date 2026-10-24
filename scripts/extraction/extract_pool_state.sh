#!/usr/bin/env bash
# Run on dsc-node. Each requested height is an end-of-block snapshot.
set -euo pipefail
if [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
  echo "Usage: bash $0 PROTOCOL ADDRESS BLOCK|START:END_EXCLUSIVE[:STEP] [--deployment PROFILE.json]"
  exit 0
fi
if [[ ( $# -ne 3 && $# -ne 5 ) || ! "$3" =~ ^[0-9]+(:[0-9]+(:[0-9]+)?)?$ ]]; then
  echo "Usage: bash $0 PROTOCOL ADDRESS BLOCK|START:END_EXCLUSIVE[:STEP] [--deployment PROFILE.json]" >&2
  exit 2
fi
PROTOCOL="$1"; POOL_ADDRESS="$2"; BLOCKS="$3"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/pool_common.sh"
if [[ $# -eq 5 && "$4" != "--deployment" ]]; then
  echo 'Expected --deployment PROFILE.json' >&2; exit 2
fi
DEPLOYMENT="${5:-}"
OUTPUT_DIR="${OUTPUT_DIR:-/home/node/dsc-extraction/data/$PROTOCOL/$POOL_ADDRESS/state-raw}"
if [[ "$STATE_MODE" == "reserves" ]]; then
  [[ -n "$DEPLOYMENT" ]] || { echo 'A reviewed --deployment profile is required' >&2; exit 2; }
  exec "$PYTHON_BIN" "$SCRIPT_DIR/extract_reserve_state.py" "$PROTOCOL" "$POOL_ADDRESS" "$BLOCKS" --deployment "$DEPLOYMENT"
fi
CATALOG="$("$PYTHON_BIN" "$SCRIPT_DIR/../protocols/protocol_config.py" "$PROTOCOL" catalog)"
"$PYTHON_BIN" "$SCRIPT_DIR/../validation/verify_pool_state.py" --blocks "$BLOCKS" \
  --address "$POOL_ADDRESS" --output "$OUTPUT_DIR" --catalog "$CATALOG" --preflight
CALL_DATA=()
while IFS=$'\t' read -r name selector bytes; do
  [[ -n "$name" ]] && CALL_DATA+=("$selector")
done < "$CATALOG"
"${CRYO_BIN:-/home/node/.cargo/bin/cryo}" eth_calls \
  --rpc "${ETH_RPC_URL:-http://127.0.0.1:8545}" \
  --contract "$POOL_ADDRESS" \
  --call-data "${CALL_DATA[@]}" \
  --blocks "$BLOCKS" \
  --output-dir "$OUTPUT_DIR" \
  --chunk-size "$CHUNK_SIZE" \
  --max-concurrent-requests "$MAX_CONCURRENT_REQUESTS" \
  --max-concurrent-chunks "$MAX_CONCURRENT_CHUNKS" \
  --hex \
  --label "${PROTOCOL}_${POOL_ADDRESS}_state_v1"
"$PYTHON_BIN" "$SCRIPT_DIR/../validation/verify_pool_state.py" --blocks "$BLOCKS" \
  --address "$POOL_ADDRESS" --output "$OUTPUT_DIR" --catalog "$CATALOG"
