"""StrataDB Python client (spec §23.1). Standard library only.

    from stratadb_client import Client
    c = Client("http://127.0.0.1:7070", token)
    r = c.ingest([{"service": "api", "severity": "ERROR", "body": "boom"}])
    print(r.commit_seq, r.durability, r.duplicate)
    print(c.query("SELECT count(*) AS n FROM logs").rows)

Every ingest call carries an idempotency key that is reused across its
retries, so retrying after a timeout cannot duplicate data (within the
server's dedup window, see docs/api/http.md).
"""

from __future__ import annotations

import gzip
import json
import os
import random
import ssl
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional

__all__ = ["Client", "StrataError", "ServerError", "TransportError", "Cancelled",
           "RetriesExhausted", "IngestReceipt", "QueryResult"]


class StrataError(Exception):
    """Base class for client errors."""

    retryable = False


class ServerError(StrataError):
    def __init__(self, status: int, kind: str, message: str, retryable: bool):
        super().__init__(f"server error {status} ({kind}): {message}")
        self.status, self.kind, self.message, self.retryable = status, kind, message, retryable


class TransportError(StrataError):
    """Network/TLS/timeout failure: the request may or may not have arrived."""

    retryable = True


class Cancelled(StrataError):
    pass


class RetriesExhausted(StrataError):
    def __init__(self, attempts: int, last: StrataError):
        super().__init__(f"gave up after {attempts} attempts: {last}")
        self.attempts, self.last = attempts, last


@dataclass
class IngestReceipt:
    accepted: int
    rejected: int
    errors: list
    commit_seq: int
    durability: str
    duplicate: bool
    idempotency_key: str
    attempts: int


@dataclass
class QueryResult:
    columns: list
    rows: list  # dicts; missing values are absent, null is None
    explain: Optional[str] = None
    stats: dict = field(default_factory=dict)
    query_id: Optional[str] = None
    next_cursor: Optional[str] = None  # set while more pages remain


class Client:
    def __init__(self, endpoint: str, token: str, *, timeout: float = 30.0, max_retries: int = 5,
                 base_backoff: float = 0.1, max_backoff: float = 10.0, gzip_min_bytes: int = 1024,
                 ca_file: Optional[str] = None, cancel: Optional[threading.Event] = None,
                 on_attempt: Optional[Callable[[str, int, Optional[int], float], None]] = None):
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("endpoint must start with http:// or https://")
        if not token:
            raise ValueError("token is empty")
        self.endpoint = endpoint.rstrip("/")
        self._token = token
        self.timeout, self.max_retries = timeout, max_retries
        self.base_backoff, self.max_backoff = base_backoff, max_backoff
        self.gzip_min_bytes = gzip_min_bytes
        self.cancel, self.on_attempt = cancel, on_attempt
        self._ctx = ssl.create_default_context(cafile=ca_file) if ca_file else None
        self._seq = 0
        self._lock = threading.Lock()

    def __repr__(self) -> str:  # never print the token
        return f"Client(endpoint={self.endpoint!r})"

    def new_idempotency_key(self) -> str:
        with self._lock:
            self._seq += 1
            n = self._seq
        return f"py-{os.urandom(12).hex()}-{n}"

    def _backoff(self, attempt: int, retry_after: Optional[float]) -> float:
        cap = min(self.max_backoff, self.base_backoff * (2 ** min(attempt, 16)))
        jitter = random.uniform(0, cap)  # full jitter
        return max(jitter, min(retry_after, self.max_backoff)) if retry_after is not None else jitter

    def _send(self, method: str, path: str, body: Optional[bytes], ctype: str,
              key: Optional[str]) -> tuple[int, Optional[float], bytes]:
        headers = {"Authorization": f"Bearer {self._token}", "Content-Type": ctype}
        if key:
            headers["Idempotency-Key"] = key
        if body is not None and len(body) >= self.gzip_min_bytes:
            body = gzip.compress(body, compresslevel=1)
            headers["Content-Encoding"] = "gzip"
        req = urllib.request.Request(self.endpoint + path, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self._ctx) as r:
                return r.status, None, r.read()
        except urllib.error.HTTPError as e:
            ra = e.headers.get("Retry-After")
            return e.code, float(ra) if ra and ra.strip().isdigit() else None, e.read()
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            raise TransportError(str(e)) from e

    def _with_retries(self, op: str, send: Callable[[], tuple[int, Optional[float], bytes]]) -> tuple[bytes, int]:
        attempt = 0
        while True:
            if self.cancel is not None and self.cancel.is_set():
                raise Cancelled("cancelled")
            start = time.monotonic()
            status = None
            try:
                status, retry_after, body = send()
            except TransportError as e:
                err: StrataError = e
                retry_after = None
            else:
                if 200 <= status < 300:
                    if self.on_attempt:
                        self.on_attempt(op, attempt, status, time.monotonic() - start)
                    return body, attempt + 1
                try:
                    v = json.loads(body or b"{}")
                except ValueError:
                    v = {}
                err = ServerError(status, v.get("error", "unknown"), v.get("message", body.decode(errors="replace")),
                                  bool(v.get("retryable", status in (429, 502, 503, 504))))
            if self.on_attempt:
                self.on_attempt(op, attempt, status, time.monotonic() - start)
            if not err.retryable:
                raise err
            if attempt >= self.max_retries:
                raise RetriesExhausted(attempt + 1, err)
            time.sleep(self._backoff(attempt, retry_after))
            attempt += 1

    def ingest(self, events: list[dict], idempotency_key: Optional[str] = None) -> IngestReceipt:
        """Ingest events as one atomic batch (retries reuse the same key)."""
        if not events:
            raise ValueError("no events")
        key = idempotency_key or self.new_idempotency_key()
        body = json.dumps(events, separators=(",", ":")).encode()
        raw, attempts = self._with_retries(
            "ingest", lambda: self._send("POST", "/api/v1/ingest/batch", body, "application/json", key))
        v = json.loads(raw)
        return IngestReceipt(v.get("accepted", 0), v.get("rejected", 0), v.get("errors", []), v.get("commit_seq", 0),
                             v.get("durability", ""), bool(v.get("duplicate", False)), key, attempts)

    def query(self, sql: str, *, timeout_ms: Optional[int] = None, max_rows: Optional[int] = None,
              query_id: Optional[str] = None, page_size: Optional[int] = None,
              cursor: Optional[str] = None) -> QueryResult:
        """Run a query. With ``page_size`` (and later ``cursor`` = the previous
        result's ``next_cursor`` plus the *same* ``sql``) rows are returned page
        by page with keyset semantics: no row twice, rows committed after the
        first page excluded (ADR-0023)."""
        req: dict[str, Any] = {"query": sql}
        for k, val in (("timeout_ms", timeout_ms), ("max_rows", max_rows), ("query_id", query_id),
                       ("page_size", page_size), ("cursor", cursor)):
            if val is not None:
                req[k] = val
        body = json.dumps(req).encode()
        raw, _ = self._with_retries("query", lambda: self._send("POST", "/api/v1/query", body, "application/json", None))
        v = json.loads(raw)
        return QueryResult(v.get("columns", []), v.get("rows", []), v.get("explain"), v.get("stats", {}),
                           v.get("query_id"), v.get("next_cursor"))

    def query_pages(self, sql: str, page_size: int, **kw: Any) -> Iterator[QueryResult]:
        """Yield every page of a row query."""
        cursor: Optional[str] = None
        while True:
            page = self.query(sql, page_size=page_size, cursor=cursor, **kw)
            yield page
            if page.next_cursor is None:
                return
            cursor = page.next_cursor

    def cancel_query(self, query_id: str) -> bool:
        """Request cancellation of a running query; False if it is not running."""
        status, _, _ = self._send("DELETE", f"/api/v1/query/{query_id}", None, "application/json", None)
        return status == 202

    def health(self) -> None:
        status, _, body = self._send("GET", "/api/v1/health", None, "application/json", None)
        if status != 200:
            raise ServerError(status, "unknown", body.decode(errors="replace"), True)
