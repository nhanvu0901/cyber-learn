import contextlib
import io
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import main


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503 if self.path == '/fail' else 200)
        self.send_header('Content-Length', '4' if self.path == '/truncated' else '2')
        self.end_headers()
        if self.path == '/slow':
            time.sleep(0.15)
        try:
            self.wfile.write(b'ok')
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


class LoadTestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def run_load(self, path='/', **kwargs):
        self.assertTrue(callable(getattr(main, 'run_load_test', None)),
                        'Missing bounded staged load-test runner')
        with contextlib.redirect_stdout(io.StringIO()):
            return main.run_load_test(self.url + path, **kwargs)

    def test_rate_is_paced_and_counts_successful_responses(self):
        start = time.perf_counter()
        result = self.run_load(rates=(10,), stage_seconds=0.45)
        self.assertGreaterEqual(time.perf_counter() - start, 0.45)
        stage = result[0]
        self.assertGreaterEqual(stage['requests'], 4)
        self.assertLessEqual(stage['requests'], 5)
        self.assertEqual(stage['statuses'], {200: stage['requests']})
        self.assertEqual(stage['errors'], 0)
        self.assertTrue(stage['passed'])

    def test_slow_body_measured_and_pending_work_is_bounded(self):
        stage = self.run_load('/slow', rates=(20,), stage_seconds=0.3,
                              max_workers=1)[0]
        self.assertGreaterEqual(stage['p95_seconds'], 0.14)
        self.assertGreater(stage['skipped'], 0)
        self.assertLessEqual(stage['requests'], 2)
        self.assertFalse(stage['passed'])

    def test_http_errors_stop_before_next_stage(self):
        results = self.run_load('/fail', rates=(5, 10), stage_seconds=0.25)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['errors'], results[0]['requests'])
        self.assertFalse(results[0]['passed'])

    def test_timeout_is_counted_as_error(self):
        stage = self.run_load('/slow', rates=(1,), stage_seconds=0.1,
                              timeout_seconds=0.03)[0]
        self.assertEqual(stage['errors'], 1)
        self.assertFalse(stage['passed'])

    def test_truncated_body_is_not_reported_as_success(self):
        stage = self.run_load('/truncated', rates=(1,), stage_seconds=0.1)[0]
        self.assertEqual(stage['errors'], 1)
        self.assertFalse(stage['passed'])

    def test_latency_threshold_stops_ramp(self):
        results = self.run_load('/slow', rates=(1, 5), stage_seconds=0.2,
                                max_p95_seconds=0.05)
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0]['passed'])

    def test_invalid_inputs_fail_before_requests(self):
        self.assertTrue(callable(getattr(main, 'run_load_test', None)),
                        'Missing bounded staged load-test runner')
        for url in ('', 'ftp://example.com', 'https://',
                    'http://user:password@localhost', 'http://localhost:invalid'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                main.run_load_test(url)
        for options in ({'rates': ()}, {'rates': (0,)}, {'rates': (float('nan'),)},
                        {'stage_seconds': -1}, {'stage_seconds': float('inf')},
                        {'max_workers': 0}, {'timeout_seconds': 0},
                        {'max_error_rate': 2}, {'max_p95_seconds': 0}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                main.run_load_test(self.url, **options)


if __name__ == '__main__':
    unittest.main()
