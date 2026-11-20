"""Producer startup must match the evidence-approved immutable generation."""
import sys
from pathlib import Path
from shared.artifacts import checksum,digest
from materialize_accounts import load

base=Path(sys.argv[1]);plan=load(base/'f64-plan.json');gate=load(base/'release-gates.json')
if gate.get('approved') is not True or gate['generation']!=plan['generation'] or gate['manifest_sha256']!=checksum(base/'f64-plan.json') or digest(plan['identity'])!=plan['generation']:raise SystemExit('No matching approved HF release')
