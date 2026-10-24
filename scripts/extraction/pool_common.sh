#!/usr/bin/env bash
# Sourced by event/state launchers after reading PROTOCOL, POOL_ADDRESS and BLOCKS.
[[ "$POOL_ADDRESS" =~ ^0x[0-9a-fA-F]{40}$ ]] || { echo 'Invalid contract address' >&2; exit 2; }
POOL_ADDRESS="$(printf '%s' "$POOL_ADDRESS" | tr '[:upper:]' '[:lower:]')"
PYTHON_BIN="${PYTHON_BIN:-python3}"
STATE_MODE="$("$PYTHON_BIN" "$SCRIPT_DIR/../protocols/protocol_config.py" "$PROTOCOL" state_mode)"
MAX_CONCURRENT_REQUESTS="${MAX_CONCURRENT_REQUESTS:-8}"
MAX_CONCURRENT_CHUNKS="${MAX_CONCURRENT_CHUNKS:-8}"
CHUNK_SIZE="${CHUNK_SIZE:-1000}"

REQUIRES_DEPLOYMENT="$("$PYTHON_BIN" "$SCRIPT_DIR/../protocols/protocol_config.py" "$PROTOCOL" requires_deployment)"
