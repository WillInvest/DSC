#!/usr/bin/env python3
"""View-only fixed SSH report. Collect only small progress JSON files."""
from pathlib import Path
import json

ROOT = Path('/home/dsc-data/runs/sequential-events')
AUTO = Path('/home/dsc-data/runs/event-pipeline')


def optional(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def report():
    data = json.loads((ROOT/'progress.json').read_text())
    data['decoding'],data['reconstruction']=[],[]
    for job in json.loads((ROOT/'plan.json').read_text())['jobs']:
        encoded=Path(job['destination']);base=encoded.parent.parent
        for key,folder,file in [('decoding','decoded','decode-progress.json'),
                                 ('reconstruction','reconstructed','reconstruction-progress.json')]:
            progress=optional(base/folder/encoded.name/file)
            if progress:
                data[key].append({**progress,'name':job['name'],'address':job['address']})
    data['storage']=optional(AUTO/'storage.json',[])
    data['pipeline']=optional(AUTO/'progress.json',{'state':'waiting'})
    for job in data['decoding']:
        pipeline=data['pipeline']
        if pipeline.get('state')=='decode' and pipeline.get('current',{}).get('address')==job['address']:
            fresh=max(0,job.get('decoded_rows',0)-pipeline.get('reused_rows',0))
            job['rows_per_second']=fresh/max(1,job.get('elapsed_seconds',1))
    data['pipeline_jobs']=optional(AUTO/'jobs.json',{})
    return data


if __name__=='__main__':
    print(json.dumps(report()))
