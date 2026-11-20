"""Active RPC and checksummed trace artifact helpers shared by receipt/trace workers."""
import gzip,hashlib,json,os,tempfile
from pathlib import Path

OPTIONS = {'tracer': 'callTracer', 'tracerConfig': {'onlyTopCall': False, 'withLog': False}, 'timeout': '120s'}

def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()

def save(path, payload, compresslevel=9):
    """One atomic checksummed gzip artifact; interrupted writes never look complete."""
    path.parent.mkdir(parents=True, exist_ok=True)
    envelope = {'sha256': hashlib.sha256(encoded(payload)).hexdigest(), 'payload': payload}
    fd, temp = tempfile.mkstemp(prefix='.partial-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(gzip.compress(encoded(envelope), compresslevel=compresslevel, mtime=0))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)

def load(path):
    envelope = json.loads(gzip.decompress(path.read_bytes()))
    payload = envelope['payload']
    if hashlib.sha256(encoded(payload)).hexdigest() != envelope['sha256']:
        raise ValueError(f'Checksum mismatch: {path}')
    return payload

def make_rpc(url):
    # Lazy imports keep offline archive selection free of an RPC dependency.
    from requests.exceptions import ConnectionError, HTTPError, Timeout
    from web3 import HTTPProvider
    from web3.providers.rpc.utils import ExceptionRetryConfiguration

    methods = ['eth_chainId', 'web3_clientVersion', 'eth_getBlockByNumber',
               'eth_getBlockReceipts', 'debug_traceTransaction', 'debug_traceBlockByNumber']
    provider = HTTPProvider(url, request_kwargs={'timeout': (10, 150)},
                            exception_retry_configuration=ExceptionRetryConfiguration(
                                errors=(ConnectionError, HTTPError, Timeout),
                                retries=3, backoff_factor=1.0, method_allowlist=methods))

    def call(method, params):
        # Provider-level API preserves raw RPC hex strings and all result fields.
        response = provider.make_request(method, params)
        if response.get('error') is not None or response.get('result') is None:
            raise RuntimeError(f'{method}: RPC error or missing result')
        return response['result']

    return call

def validate_trace(trace):
    if not isinstance(trace, dict) or not all(k in trace for k in ('type', 'from', 'gas', 'gasUsed', 'input')):
        raise ValueError('Missing or invalid callTracer result')
    for child in trace.get('calls', []):
        validate_trace(child)

def called_addresses(trace):
    # Match every destination, including successful child calls in a failed tx.
    found = {trace['to'].lower()} if trace.get('to') else set()
    for child in trace.get('calls', []):
        found.update(called_addresses(child))
    return found
