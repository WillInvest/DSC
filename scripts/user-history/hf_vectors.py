"""Exact HF vector codec. Arrow owns grouping, lists, Parquet and IPC encoding."""
import hashlib
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
ORIGIN=16291127
STOP=26407882
SPAN=4096
MAX=2**256-1
KEYS=['entity_id','block_number','hf_values']

def raw_hf(value):
    n=int(value)
    if str(n)!=str(value) or not 0<=n<=MAX:raise ValueError('Noncanonical uint256 HF')
    return n.to_bytes(max(1,(n.bit_length()+7)//8),'big')

def validate_values(values):
    if values.null_count:raise ValueError('Missing user HF')
    lengths=pc.binary_length(values)
    if pc.any(pc.or_(pc.less(lengths,1),pc.greater(lengths,32))).as_py():raise ValueError('Invalid uint256 byte width')
    zeros=pc.equal(pc.binary_slice(values,0,1),pa.scalar(b'\0',pa.binary()))
    if pc.any(pc.and_(pc.greater(lengths,1),zeros)).as_py():raise ValueError('Nonminimal uint256')

def canonical(table):return table.select(KEYS).combine_chunks().sort_by([('entity_id','ascending'),('block_number','ascending')])

def logical_sha(table):
    table=canonical(table);sink=pa.BufferOutputStream()
    with pa.ipc.new_stream(sink,table.schema) as writer:writer.write_table(table)
    return hashlib.sha256(sink.getvalue()).hexdigest()

def pack(table):
    table=canonical(table);n=table.num_rows
    if not n:raise ValueError('Empty vector buffer')
    validate_values(table['hf_values']);ids=table['entity_id'];blocks=table['block_number']
    if table.schema.field('entity_id').type!=pa.uint32() or table.schema.field('block_number').type!=pa.uint32():raise ValueError('Invalid key types')
    if ids.null_count or blocks.null_count or pc.min(blocks).as_py()<ORIGIN or pc.max(blocks).as_py()>=STOP:raise ValueError('Invalid key scope')
    same=pc.equal(ids.slice(1),ids.slice(0,n-1));consecutive=pc.equal(blocks.slice(1),pc.add(blocks.slice(0,n-1),1))
    if pc.any(pc.and_(same,pc.invert(consecutive))).as_py():raise ValueError('Duplicate/gap in buffered user history')
    bucket=pc.divide(pc.subtract(blocks,pa.scalar(ORIGIN,pa.uint32())),pa.scalar(SPAN,pa.uint32())).cast(pa.uint32())
    groups=table.append_column('bucket',bucket).group_by(['entity_id','bucket'],use_threads=False).aggregate([('block_number','min'),('block_number','max'),('hf_values','list')])
    lens=pc.list_value_length(groups['hf_values_list'])
    if not pc.all(pc.equal(lens,pc.add(pc.subtract(groups['block_number_max'],groups['block_number_min']),1))).as_py():raise ValueError('Noncontiguous vector')
    if pc.max(lens).as_py()>SPAN:raise ValueError('Oversized vector')
    return pa.table({'entity_id':groups['entity_id'],'start_offset':pc.subtract(groups['block_number_min'],pa.scalar(ORIGIN,pa.uint32())),'hf_values':groups['hf_values_list']}).sort_by([('entity_id','ascending'),('start_offset','ascending')])

def unpack(vectors):
    if vectors.column_names!=['entity_id','start_offset','hf_values']:raise ValueError('Wrong vector schema')
    if vectors.schema.field('entity_id').type!=pa.uint32() or vectors.schema.field('start_offset').type!=pa.uint32():raise ValueError('Wrong ID/offset type')
    values=vectors['hf_values'].combine_chunks();lens=pc.list_value_length(values).cast(pa.int64())
    if values.null_count or pc.any(pc.or_(pc.less(lens,1),pc.greater(lens,SPAN))).as_py():raise ValueError('Missing/invalid vector')
    flat=pc.list_flatten(values);validate_values(flat)
    if vectors['entity_id'].null_count or vectors['start_offset'].null_count:raise ValueError('Missing vector key')
    if pc.any(pc.greater(pc.add(vectors['start_offset'].cast(pa.int64()),lens),STOP-ORIGIN)).as_py():raise ValueError('Vector beyond cutoff')
    parent=pc.list_parent_indices(values);prefix=pc.subtract(pc.cumulative_sum(lens),lens)
    count=pc.sum(lens).as_py();within=pc.subtract(pa.array(range(count),type=pa.int64()),pc.take(prefix,parent))
    blocks=pc.add(pc.add(pc.take(vectors['start_offset'],parent),ORIGIN),within).cast(pa.uint32())
    return pa.table({'entity_id':pc.take(vectors['entity_id'],parent),'block_number':blocks,'hf_values':flat})

def dumps(vectors):
    sink=pa.BufferOutputStream();pq.write_table(vectors,sink,compression='zstd',compression_level=9,row_group_size=100000)
    return sink.getvalue().to_pybytes()

def loads(data):return pq.read_table(pa.BufferReader(data))
