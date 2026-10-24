"""Exercise the real Web3 HTTP provider against a local server, without RETH."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
if ROOT.name == 'scripts':
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT / 'scripts'))
from reverted.extract_reverted import make_rpc


class RpcTests(unittest.TestCase):
    def setUp(self):
        self.responses = []
        self.requests = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                outer.requests.append(request)
                code, body = outer.responses.pop(0)
                payload = json.dumps(dict(jsonrpc='2.0', id=request['id'], **body)).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.call = make_rpc(f'http://127.0.0.1:{self.server.server_port}')
        self.addCleanup(self.close)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_debug_transport_retry_preserves_raw_result(self):
        trace = {'gas': '0x123', 'error': 'execution reverted', 'calls': [], 'extension': {'x': 1}}
        self.responses = [(503, {}), (200, {'result': trace})]
        self.assertEqual(self.call('debug_traceTransaction', ['0xabc', {'tracer': 'callTracer'}]), trace)
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.requests[0]['params'], self.requests[1]['params'])

    def test_rpc_error_fails_without_retrying_execution(self):
        self.responses = [(200, {'error': {'code': -32000, 'message': 'execution timeout'}})]
        with self.assertRaises(RuntimeError):
            self.call('debug_traceBlockByNumber', ['0x123', {}])
        self.assertEqual(len(self.requests), 1)

    def test_null_receipts_are_not_empty_coverage(self):
        self.responses = [(200, {'result': None})]
        with self.assertRaises(RuntimeError):
            self.call('eth_getBlockReceipts', ['0x123'])

    def test_receipts_retry_and_empty_list_preserved(self):
        self.responses = [(503, {}), (200, {'result': []})]
        self.assertEqual(self.call('eth_getBlockReceipts', ['0x123']), [])
        self.assertEqual(len(self.requests), 2)


if __name__ == '__main__':
    unittest.main()
