"""Keep raw receipts and complete call trees unchanged while regrouping block rows."""
import gzip
import json
from reverted.trace_received import pack


def block(row):return row['block_number'] if 'block_number' in row else row['receipt_summary']['block_number']


def records(fragments,start,stop):
    cursor=start
    for f in fragments:
        previous=None
        with gzip.open(f.path,'rt') as stream:
            for line in stream:
                row=json.loads(line);number=block(row)
                if not f.start<=number<f.stop or previous is not None and number!=previous+1:
                    raise ValueError('JSONL fragment coverage mismatch')
                previous=number
                if start<=number<stop:
                    if number!=cursor:raise ValueError('JSONL gap, duplicate or overlap')
                    yield row;cursor+=1
    if cursor!=stop:raise ValueError('JSONL interval incomplete')


def combine(fragments,target,start,stop,dense):
    pack(target,records(fragments,start,stop))
    with gzip.open(target,'rt') as stream:
        count=0
        for expected in records(fragments,start,stop):
            line=next(stream,None)
            if line is None or json.loads(line)!=expected:raise ValueError('Compacted JSONL values differ')
            count+=1
        if next(stream,None) is not None:raise ValueError('Compacted JSONL has extra records')
    return count
