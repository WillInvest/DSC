#!/usr/bin/env python3
"""Fixed report command, suitable for an SSH forced-command key."""
import json
from pathlib import Path
from report import export

if __name__ == '__main__':
    config = json.loads(Path('/home/dsc-data/.config/dsc-monitor/sources.json').read_text())
    print(json.dumps(export(config)))
