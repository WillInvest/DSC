"""Stream decoded Parquet batches and publish checksummed output atomically."""
from collections import defaultdict
from contextlib import ExitStack
import hashlib
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq


def checksum(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def inventory(source):
    files = sorted(source.rglob('*.parquet'))
    if not files:
        raise ValueError('no input Parquet files')
    return [{'file': p.relative_to(source).as_posix(), 'sha256': checksum(p), 'bytes': p.stat().st_size} for p in files]


def verify_existing(output, config):
    if not output.exists():
        return False
    manifest = json.loads((output / 'manifest.json').read_text())
    if manifest['config'] != config:
        raise ValueError('existing output is for different inputs/configuration; use a new output directory')
    for entry in manifest['outputs']:
        if checksum(output / entry['file']) != entry['sha256']:
            raise ValueError('decoded output checksum mismatch')
    print('Already decoded; source and output checksums verified.')
    return True


class BatchWriter:
    def __init__(self, directory, stack, kind, protocol):
        self.directory = directory
        self.stack = stack
        self.kind = kind
        self.protocol = protocol
        self.writers = {}
        self.counts = defaultdict(int)
        self.seen = set()

    def write_batch(self, batch, decoder, source_file):
        groups = defaultdict(list)
        for row in batch.to_pylist():
            record = decoder.decode_row(row, source_file)
            if record.key in self.seen:
                raise ValueError(f'duplicate {self.kind} record: {record.key}')
            self.seen.add(record.key)
            if record.name not in self.writers:
                self.writers[record.name] = self.stack.enter_context(pq.ParquetWriter(
                    self.directory / (record.name + '.parquet'), record.schema, compression='zstd'))
            groups[record.name].append(dict(protocol=self.protocol, **record.values))
        for name, rows in groups.items():
            writer = self.writers[name]
            writer.write_table(pa.Table.from_pylist(rows, schema=writer.schema))
            self.counts[name] += len(rows)


def write_batches(source, temporary, config, decoder):
    with ExitStack() as stack:
        writer = BatchWriter(temporary, stack, config['kind'], config['protocol'])
        for entry in config['inputs']:
            path = source / entry['file']
            for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
                writer.write_batch(batch, decoder, entry['file'])
    return writer.counts


def fsync_path(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish(temporary, output, counts, config):
    outputs = []
    for name, count in sorted(counts.items()):
        path = temporary / (name + '.parquet')
        if pq.ParquetFile(path).metadata.num_rows != count:
            raise ValueError('written row count mismatch')
        fsync_path(path)
        outputs.append(dict(file=path.name, rows=count, sha256=checksum(path)))
    manifest = dict(config=config, outputs=outputs, total_rows=sum(counts.values()),
                    note='Decoding validated; source coverage, delivery and completion bits are separate.')
    manifest_path = temporary / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    fsync_path(manifest_path)
    fsync_path(temporary)
    os.rename(temporary, output)
    fsync_path(output.parent)
