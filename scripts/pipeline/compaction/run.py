"""Coordinate canonical storage independently of extraction completion checkpoints."""
from pathlib import Path
import json
import time
from pipeline.runtime import NODE,REVERT,NODE_PYTHON,NODE_SCRIPTS,REVERT_PYTHON,REVERT_SCRIPTS,PYTHON,SCRIPTS,ssh,run,transfer,remote_json
from shared.artifacts import write_json
from pipeline.compaction.collect import read
from pipeline.compaction.worker import ROOTS


def compact_completed(tracker,baseline_plan):
    latest=tracker.db.execute("select * from cycles where state='complete' order by end desc limit 1").fetchone()
    if latest is None:return
    current=dict(latest);through=current['end'];status_path=tracker.root/'compaction.json'
    committed=tracker.db.execute('select * from compactions order by through desc limit 1').fetchone()
    baseline=read(baseline_plan);accepted=committed['through'] if committed else baseline['end_block']
    previous_roles=json.loads(committed['roles']) if committed else {}
    if accepted==through:
        try:previous=read(status_path)
        except (OSError,ValueError):previous={}
        if previous.get('state')=='complete' and previous.get('through')==accepted:return
        write_json(status_path,dict(state='complete',through=accepted,target=through,roles=previous_roles,
                                   current_role=None,updated_at=committed['committed']));return
    cycles=[dict(r) for r in tracker.db.execute("select * from cycles where state='complete' and end>? order by start",(accepted,))]
    request=dict(baseline=baseline,baseline_events_root=str(Path(baseline_plan).parent),
                 pipeline_root=str(tracker.root),cycles=[])
    for cycle in cycles:
        plan=read(tracker.root/'cycles'/cycle['id']/'events/plan.json')
        request['cycles'].append({**cycle,'jobs':plan['jobs']})
    path=tracker.root/'compaction-request.json';write_json(path,request)
    status=dict(state='compacting',through=accepted,target=through,roles=previous_roles,updated_at=time.time())
    def heartbeat():
        tracker.publish(current,'compaction','compacting')
        status['updated_at']=time.time();write_json(status_path,status)
    log=tracker.root/'compaction.log'
    try:
        for role,host,python,scripts,remote in [
            ('node',NODE,NODE_PYTHON,NODE_SCRIPTS,'/home/node/dsc-extraction/compaction-request.json'),
            ('revert',REVERT,REVERT_PYTHON,REVERT_SCRIPTS,'/home/dsc-revert/compaction-request.json'),
            ('data',None,PYTHON,str(SCRIPTS),str(path))]:
            status['current_role']=role;heartbeat()
            if host:transfer(str(path),host+':'+remote,log,heartbeat)
            command=[python,scripts+'/pipeline/compaction/worker.py','--role',role,'--request',remote]
            def progress():
                item=remote_json(host,str(ROOTS[role]/'progress.json')) if host else read(ROOTS[role]/'progress.json')
                status['roles'][role]=item
                return (item.get('through'),item.get('current_stream'),item.get('current_start'),item.get('retired_files'))
            run(ssh(host,command,True) if host else command,log,heartbeat,progress=progress)
            item=remote_json(host,str(ROOTS[role]/'progress.json')) if host else read(ROOTS[role]/'progress.json')
            if item['state']!='complete' or item['through']!=through:raise ValueError('Canonical store incomplete')
            status['roles'][role]=item
        with tracker.db:
            tracker.db.execute('insert into compactions values(?,?,?)',(through,json.dumps(status['roles']),time.time()))
        status.update(state='complete',through=through,current_role=None);heartbeat()
    except Exception as error:
        status.update(state='error',error=str(error));write_json(status_path,status)
        tracker.publish(current,'compaction','error',error=str(error));raise
