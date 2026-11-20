"""Authenticated HF control and file transfer through existing Dask TLS identities."""
import json
import os
import shutil
from pathlib import Path

from .dispatch import dispatch
from .ledger import JOB_ID


def connect(config):
    """Use the same mutually authenticated TLS API as the established HF flow."""
    from distributed import Client, Security

    private = Path(config['tls_private'])
    security = Security(require_encryption=True,
                        tls_ca_file=str(private / 'ca.pem'),
                        tls_client_cert=str(private / 'host-cert.pem'),
                        tls_client_key=str(private / 'host-key.pem'))
    return Client(config['scheduler'], security=security, set_as_default=True,
                  timeout=config.get('connect_timeout_seconds', 15))


def declared_parts(receipt):
    """Limit transport to closed receipt-declared aggregate files."""
    names = [part['name'] for part in receipt['parts']]
    if not names or len(names) != len(set(names)):
        raise ValueError('Missing or duplicate aggregate filenames')
    if any(Path(name).name != name or not name.endswith('.parquet') for name in names):
        raise ValueError('Invalid aggregate filename in closed receipt')
    return names


def sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class TLSTransport:
    """Control host-local queues and stream only their declared closed results.

    The scheduler handles IO only. Numerical workers retain their independent
    local queues, and node remains the owner of the acceptance ledger.
    """
    def __init__(self, config):
        self.client = connect(config) if any(not host.get('local') for host in config['hosts']) else None
        self.filesystem = None
        self.transfer_timeout = config.get('transfer_timeout_seconds', 30)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.client is not None:
            self.client.close()

    def worker(self, host):
        try:
            workers = self.client.scheduler_info(n_workers=-1)['workers']
        except Exception as exc:
            raise OSError('Authenticated HF scheduler unavailable: ' + str(exc)) from exc
        matches = [address for address, worker in workers.items()
                   if worker.get('name') == host['worker_name']
                   and worker.get('host') == host['worker_host']]
        if not matches:
            raise OSError('HF IO worker unavailable: ' + host['role'])
        if len(matches) != 1:
            raise ValueError('Ambiguous HF IO worker identity: ' + host['role'])
        return matches[0]

    def request(self, host, operation, payload=None, after=0):
        if host.get('local'):
            return dispatch(host['root'], operation, after, assignment=payload)
        address = self.worker(host)
        future = None
        try:
            future = self.client.submit(dispatch, host['root'], operation, after,
                                        assignment=payload, workers=[address],
                                        allow_other_workers=False, pure=False)
            return future.result(timeout=host.get('timeout_seconds', 30))
        except (ValueError, KeyError, TypeError):
            # Invalid assignments and source records require operator attention.
            raise
        except Exception as exc:
            raise OSError('HF worker control unavailable: ' + host['role'] + ': ' + str(exc)) from exc
        finally:
            if future is not None:
                future.release()

    def receive(self, host, key, destination):
        if not JOB_ID.fullmatch(key):
            raise ValueError('Invalid completed job ID')
        source = Path(host['root']) / 'results' / key
        destination = Path(destination)
        if destination.is_symlink():
            raise ValueError('Symlink result destination')
        destination.parent.mkdir(parents=True, exist_ok=True)
        if host.get('local'):
            if source.resolve() != destination.resolve():
                shutil.copytree(source, destination, dirs_exist_ok=True)
            receipt = json.loads((destination / 'receipt.json').read_text())
            names = ['receipt.json', *declared_parts(receipt)]
            for name in names:
                with (destination / name).open('rb') as stream:
                    os.fsync(stream.fileno())
            sync_directory(destination)
            return
        address = self.worker(host)
        destination.mkdir(exist_ok=True)
        import dask
        import fsspec

        if self.filesystem is None:
            # A default TLS Client keeps live credentials out of serialized graphs.
            self.filesystem = fsspec.filesystem('dask', target_protocol='file', client=None,
                                               skip_instance_cache=True)
        with dask.annotate(workers=[address], allow_other_workers=False), \
                dask.config.set(scheduler=self._bounded_get):
            self._copy(source / 'receipt.json', destination / 'receipt.json')
            receipt = json.loads((destination / 'receipt.json').read_text())
            for name in declared_parts(receipt):
                self._copy(source / name, destination / name)
        sync_directory(destination)

    def _bounded_get(self, graph, keys, **kwargs):
        """Bound fsspec's delayed reads using the existing Dask future APIs.

        DaskWorkerFileSystem calls delayed.compute without a timeout. A lost
        constrained worker would otherwise prevent unrelated roles progressing.
        """
        from distributed import wait
        from distributed.client import futures_of

        futures = self.client.get(graph, keys, sync=False, **kwargs)
        try:
            wait(futures, timeout=self.transfer_timeout)
            return self.client.gather(futures)
        except TimeoutError:
            self.client.cancel(futures)
            raise
        finally:
            for future in futures_of(futures):
                future.release()

    def _copy(self, source, target):
        if target.is_symlink():
            raise ValueError('Symlink result member destination')
        incoming = target.with_name(target.name + '.receiving')
        if incoming.is_symlink():
            raise ValueError('Symlink partial result destination')
        try:
            self.filesystem.get_file(str(source), str(incoming))
        except (ValueError, KeyError, TypeError):
            raise
        except Exception as exc:
            raise OSError('HF result transfer unavailable: ' + str(source) + ': ' + str(exc)) from exc
        with incoming.open('rb') as stream:
            os.fsync(stream.fileno())
        os.replace(incoming, target)
