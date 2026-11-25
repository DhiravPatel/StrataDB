"""Conformance tests for the Python SDK against a real server.

Run by apps/stratadb-server/tests/python_sdk.rs with STRATADB_URL and
STRATADB_TOKEN set, or manually against any server.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from stratadb_client import Client, ServerError, RetriesExhausted  # noqa: E402

URL, TOKEN = os.environ.get("STRATADB_URL"), os.environ.get("STRATADB_TOKEN")


@unittest.skipUnless(URL and TOKEN, "STRATADB_URL/STRATADB_TOKEN not set")
class Conformance(unittest.TestCase):
    def setUp(self):
        self.c = Client(URL, TOKEN)

    def test_ingest_dedup_query(self):
        self.c.health()
        events = [{"service": "py", "severity": "ERROR", "body": "boom"}, {"service": "py", "body": "ok"},
                  {"ingest_time": 1}]
        r = self.c.ingest(events)
        self.assertEqual((r.accepted, r.rejected, r.duplicate), (2, 1, False))
        self.assertEqual(r.errors[0]["index"], 2)
        again = self.c.ingest(events, idempotency_key=r.idempotency_key)
        self.assertTrue(again.duplicate)
        self.assertEqual(again.commit_seq, r.commit_seq)
        big = [{"service": "py", "body": "x" * 100} for _ in range(300)]  # gzip path
        self.assertEqual(self.c.ingest(big).accepted, 300)
        q = self.c.query("SELECT count(*) AS n FROM logs WHERE service = 'py'")
        self.assertEqual(q.rows, [{"n": 302}])
        self.assertTrue(q.query_id)

    def test_pagination(self):
        self.c.ingest([{"service": "pages", "body": f"p{i:03}"} for i in range(95)])
        sql = "SELECT body FROM logs WHERE service = 'pages' ORDER BY body"
        pages = list(self.c.query_pages(sql, 20))
        self.assertEqual([len(p.rows) for p in pages], [20, 20, 20, 20, 15])
        self.assertEqual([r["body"] for p in pages for r in p.rows], [f"p{i:03}" for i in range(95)])
        self.assertIsNone(pages[-1].next_cursor)
        with self.assertRaises(ServerError) as e:
            self.c.query("SELECT service FROM logs", page_size=5, cursor=pages[0].next_cursor)
        self.assertEqual(e.exception.status, 400)

    def test_errors(self):
        with self.assertRaises(ServerError) as e:
            self.c.query("SELECT * FROM nowhere")
        self.assertEqual((e.exception.status, e.exception.kind, e.exception.retryable), (400, "invalid_input", False))
        with self.assertRaises(ServerError) as e:
            Client(URL, "sdb_x_y").query("SELECT 1 FROM logs")
        self.assertEqual(e.exception.status, 401)
        self.assertNotIn(TOKEN, repr(self.c))
        self.assertFalse(self.c.cancel_query("not-running"))

    def test_transport_retries_are_bounded(self):
        c = Client("http://127.0.0.1:9", "t", max_retries=2, base_backoff=0.001, max_backoff=0.002, timeout=1)
        with self.assertRaises(RetriesExhausted) as e:
            c.query("SELECT 1 FROM logs")
        self.assertEqual(e.exception.attempts, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
