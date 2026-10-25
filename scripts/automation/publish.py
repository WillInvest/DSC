#!/usr/bin/env python3
"""Mirror the web monitor counters into Cryo's second tmux pane."""
import importlib.util
from pathlib import Path
import subprocess
import time

path=Path(__file__).resolve().parents[1]/'event-cryo-extraction/export.py'
spec=importlib.util.spec_from_file_location('pipeline_report',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
from storage import collect
collect()
data=module.report()
lines=['DSC PIPELINE | '+time.strftime('%Y-%m-%d %H:%M:%S UTC',time.gmtime())]
lines.append(f"EXTRACTION: {data['complete_addresses']}/{data['total_addresses']} addresses received")
for key,label,count,total in [('decoding','DECODE','decoded_rows','total_rows'),
                               ('reconstruction','RECONSTRUCT','completed_blocks','total_blocks')]:
    jobs=data[key]
    lines.append(f"{label}: {sum(j['state']=='complete' for j in jobs)}/{data['total_addresses']} addresses complete")
    active=[j for j in jobs if j['state']!='complete']
    for job in (active or jobs[-1:])[:3]:
        done=job.get(count,0);size=job.get(total,0)
        lines.append(f"  {job['protocol']} {job['name']}: {100*done/size if size else 0:.2f}% ({done:,}/{size:,}) {job['state']}")
lines.append('SCHEDULER: '+data['pipeline']['state']+' | Decode first; event-derived reconstruction, unchecked')
for address,state in data['pipeline_jobs'].items():
    if state.get('error'):
        lines.append('RETRY '+address+': '+state['error'])
content='\n'.join(lines)+'\n'
# Only the known progress pane is created. Never replace the native Cryo pane.
command="""cat > /home/node/dsc-extraction/sequential-events/decode-status.txt.tmp && mv /home/node/dsc-extraction/sequential-events/decode-status.txt.tmp /home/node/dsc-extraction/sequential-events/decode-status.txt
if tmux has-session -t cryo-events 2>/dev/null; then
 if ! tmux list-panes -t cryo-events:0 -F '#{pane_index}' | grep -qx 1; then
  tmux split-window -d -v -t cryo-events:0.0 -l 12 'watch -t -n 10 cat /home/node/dsc-extraction/sequential-events/decode-status.txt'
 fi
fi"""
subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','node@10.246.103.160',command],
               input=content,text=True,check=True,timeout=20)
