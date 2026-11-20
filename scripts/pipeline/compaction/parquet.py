"""Use Arrow's dataset filter and writer; compare every output value before publication."""
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def combine(fragments,target,start,stop,dense):
    paths=[str(f.path) for f in fragments]
    schema=pq.read_schema(paths[0])
    for f in fragments:
        if not pq.read_schema(f.path).equals(schema):raise ValueError('Cannot combine different Arrow schemas')
        blocks=pq.read_table(f.path,columns=['block_number']).column(0)
        bounds=pc.min_max(blocks).as_py()
        if blocks.length() and (bounds['min']<f.start or bounds['max']>=f.stop):
            raise ValueError('Source rows outside recorded coverage')
    table=ds.dataset(paths,format='parquet',schema=schema).to_table(
        filter=(ds.field('block_number')>=start)&(ds.field('block_number')<stop),use_threads=False)
    if dense:
        expected=pa.array(range(start,stop),type=table.schema.field('block_number').type)
        if not table.column('block_number').combine_chunks().equals(expected):
            raise ValueError('Every-block state has gaps, duplicates or wrong ordering')
    pq.write_table(table,target,compression='zstd',row_group_size=10000)
    if not pq.read_table(target).equals(table):raise ValueError('Compacted Parquet values differ')
    return table.num_rows
