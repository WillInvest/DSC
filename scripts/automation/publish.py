#!/usr/bin/env python3
"""Mirror the web monitor counters into Cryo's second tmux pane."""
from concurrent.futures import ThreadPoolExecutor
import json
import importlib.util
from pathlib import Path
import subprocess
import time

path=Path(__file__).resolve().parents[1]/'event-cryo-extraction/export.py'
spec=importlib.util.spec_from_file_location('pipeline_report',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
from storage import collect
collect()
# Poll preparation independently of the scheduler, including manual recovery jobs.
cache=Path('/home/dsc-data/runs/event-pipeline/preparation.json')
try:
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',
        'node@10.246.103.160','python3 -'],
        input=Path(__file__).with_name('preparation_status.py').read_text(),
        capture_output=True,text=True,check=True,timeout=15)
    snapshot=json.loads(result.stdout)
    temp=cache.with_suffix('.tmp');temp.write_text(json.dumps(snapshot));temp.replace(cache)
except (OSError,ValueError,subprocess.SubprocessError):
    pass  # Preserve the last snapshot; clients label its timestamp as stale.
receipt_cache=Path('/home/dsc-data/runs/event-pipeline/receipt-scan.json')
try:
    current_path=Path('/home/dsc-data/runs/pipeline/current.json')
    current=json.loads(current_path.read_text()) if current_path.exists() else None
    cycle=current['cycle']['id'] if current else None
    node_root='/home/node/dsc-extraction/cycles/'+cycle+'/receipts' if cycle else '/home/node/dsc-reverted/receipts/historical-v1'
    revert_root='/home/dsc-revert/data/cycles/'+cycle if cycle else None
    receipt_root=revert_root+'/receipts' if cycle else '/home/dsc-revert/data/receipts/historical-v1'
    trace_root=revert_root+'/traces' if cycle else '/home/dsc-revert/data/archive/reverted/drpc-v1'
    previous=json.loads(receipt_cache.read_text()) if receipt_cache.exists() else {}
    def receipt_status(item):
        key,host,path=item
        try:
            result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',host,'cat '+path],capture_output=True,text=True,check=True,timeout=10)
            return key,json.loads(result.stdout)
        except (OSError,ValueError,subprocess.SubprocessError):
            return key,{'connection_error':True}
    with ThreadPoolExecutor(max_workers=3) as pool:
        receipts=dict(pool.map(receipt_status,[('node','node@10.246.103.160',node_root+'/progress.json'),('archive','dsc-revert@10.246.103.26',receipt_root+'/progress.json'),('traces','dsc-revert@10.246.103.26',trace_root+'/progress.json')]))
    temp=receipt_cache.with_suffix('.tmp');temp.write_text(json.dumps(receipts));temp.replace(receipt_cache)
except (OSError,ValueError): pass
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
for job in data.get('preparation',{}).get('jobs',[]):
    age=max(0,int(time.time()-job['updated_at']))
    lines.append(f"PREPARE {job['protocol']}: seeds {job['seeds_completed']}/{job['seeds_total']} | heartbeat {age}s ago")
    for source in job['sources']:
        age=int(time.time()-source['last_output_at']) if source['last_output_at'] else '?'
        lines.append(f"  {source['address']}: {source['completed_chunks']}/{source['total_chunks']} chunks | {source['bytes']/1e6:.1f} MB | last file {age}s ago")
receipt=data.get('receipt_scan',{});node=receipt.get('node',{});archive=receipt.get('archive',{})
if node.get('total_blocks'):
    count=node.get('completed_blocks',0);total=node['total_blocks'];rate=node.get('blocks_per_second',0)
    lines.append(f"RECEIPT SCAN: {count:,}/{total:,} blocks ({100*count/total:.2f}%) | {rate:.1f} blocks/s | {node.get('state')} | heartbeat {int(time.time()-node.get('updated_at',0))}s ago")
    lines.append(f"  Failed txs: {node.get('failed_transactions',0):,} | current chunk: {node.get('inflight_completed',0)}/1000 | workers: {node.get('workers')}")
    lines.append(f"  DSC-REVERT accepted: {archive.get('completed_blocks',0):,} blocks | {archive.get('compressed_bytes',0)/1e9:.2f} GB | {archive.get('state','waiting')}")
    if node.get('error') or archive.get('error'):lines.append('RECEIPT ERROR: '+str(node.get('error') or archive.get('error')))
traces=receipt.get('traces',{});delivery=data.get('trace_delivery',{})
if traces.get('total_blocks'):
    count=traces.get('completed_blocks',0);total=traces['total_blocks'];accounts=traces.get('accounts',[])
    lines.append(f"TRACING: {count:,}/{total:,} blocks ({100*count/total:.2f}%) | {traces.get('blocks_per_second',0):.2f} blocks/s | {traces.get('state')} | heartbeat {int(time.time()-traces.get('updated_at',0))}s ago")
    lines.append(f"  {traces.get('failed_transactions',0):,} failed txs archived/checkpointed | {traces.get('selected_transactions',0):,} match tracked contracts | {traces.get('active_requests',0)}/{len(accounts)} workers busy")
    for kind,group in traces.get('providers',{}).items():lines.append(f"  {kind.upper()}: {group['active']}/{group['workers']} workers busy")
    lines.append(f"  DSC-DATA accepted: {delivery.get('accepted_blocks',0):,} blocks | {delivery.get('selected_transactions',0):,} selected txs")
    estimate=traces.get('estimate',{})
    lines.append(f"  FIXED-RANGE ETA: {estimate.get('full_duration','unavailable')} | {estimate.get('remaining_blocks',0):,} blocks remaining")
    for account in accounts:
        if account.get('error'):lines.append(f"  KEY {account['name']}: {'disabled' if account['disabled'] else 'cooldown'} | {account['error']}")
    for error in traces.get('errors',[])[:2]:lines.append(f"  TRACE RETRY block {error['number']}: {error['error']}")
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

subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','dsc-revert@10.246.103.26',
    'mkdir -p /home/dsc-revert/runs; cat > /home/dsc-revert/runs/trace-status.txt.tmp && mv /home/dsc-revert/runs/trace-status.txt.tmp /home/dsc-revert/runs/trace-status.txt'],input=content,text=True,check=True,timeout=15)
