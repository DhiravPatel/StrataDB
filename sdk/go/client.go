// Package stratadb is the Go client for the StrataDB HTTP API (spec §23.1).
//
// Every Ingest call carries an idempotency key that is reused across its
// retries, so retrying after a timeout cannot duplicate data within the
// server's dedup window. Standard library only.
package stratadb

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Config configures a Client.
type Config struct {
	Endpoint     string        // e.g. https://stratadb.example:7070
	Token        string        // API token
	Timeout      time.Duration // per attempt; default 30s
	MaxRetries   int           // retries after the first attempt; default 5
	BaseBackoff  time.Duration // default 100ms
	MaxBackoff   time.Duration // default 10s
	GzipMinBytes int           // compress bodies at least this large; default 1024, <0 disables
	CAPEM        []byte        // extra trusted CA certificates (PEM)
	// OnAttempt is called once per HTTP attempt (status 0 = transport error).
	OnAttempt func(op string, attempt int, status int, elapsed time.Duration)
}

// ServerError is an error response from the server.
type ServerError struct {
	Status    int
	Kind      string
	Message   string
	Retryable bool
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error %d (%s): %s", e.Status, e.Kind, e.Message)
}

// TransportError is a network/TLS/timeout failure; the request may or may not have arrived.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "transport error: " + e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

// RetriesExhaustedError wraps the last retryable error.
type RetriesExhaustedError struct {
	Attempts int
	Last     error
}

func (e *RetriesExhaustedError) Error() string {
	return fmt.Sprintf("gave up after %d attempts: %v", e.Attempts, e.Last)
}
func (e *RetriesExhaustedError) Unwrap() error { return e.Last }

// RecordError describes one rejected record.
type RecordError struct {
	Index   int    `json:"index"`
	Kind    string `json:"error"`
	Message string `json:"message"`
}

// IngestReceipt is the commit receipt (see docs/api/http.md).
type IngestReceipt struct {
	Accepted       int           `json:"accepted"`
	Rejected       int           `json:"rejected"`
	Errors         []RecordError `json:"errors"`
	CommitSeq      uint64        `json:"commit_seq"`
	Durability     string        `json:"durability"`
	Duplicate      bool          `json:"duplicate"`
	IdempotencyKey string        `json:"-"`
	Attempts       int           `json:"-"`
}

// QueryResult holds query output; missing values are absent from row maps.
type QueryResult struct {
	QueryID string                   `json:"query_id"`
	Columns []string                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
	Explain *string                  `json:"explain"`
	Stats   map[string]interface{}   `json:"stats"`
}

// Client is safe for concurrent use; connections are pooled.
type Client struct {
	cfg  Config
	http *http.Client
	seq  atomic.Uint64
}

// New validates cfg and creates a client.
func New(cfg Config) (*Client, error) {
	if !strings.HasPrefix(cfg.Endpoint, "http://") && !strings.HasPrefix(cfg.Endpoint, "https://") {
		return nil, errors.New("endpoint must start with http:// or https://")
	}
	if cfg.Token == "" {
		return nil, errors.New("token is empty")
	}
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}
	if cfg.BaseBackoff == 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 10 * time.Second
	}
	if cfg.GzipMinBytes == 0 {
		cfg.GzipMinBytes = 1024
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if len(cfg.CAPEM) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(cfg.CAPEM) {
			return nil, errors.New("CAPEM contains no certificates")
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &Client{cfg: cfg, http: &http.Client{Transport: tr, Timeout: cfg.Timeout}}, nil
}

// String never includes the token.
func (c *Client) String() string { return "stratadb.Client{" + c.cfg.Endpoint + "}" }

// NewIdempotencyKey returns a random, unique key.
func (c *Client) NewIdempotencyKey() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return fmt.Sprintf("go-%s-%d", hex.EncodeToString(b), c.seq.Add(1))
}

func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	exp := c.cfg.BaseBackoff << min(attempt, 16)
	if exp > c.cfg.MaxBackoff || exp <= 0 {
		exp = c.cfg.MaxBackoff
	}
	d := time.Duration(mrand.Int64N(int64(exp) + 1)) // full jitter
	if retryAfter > 0 {
		retryAfter = min(retryAfter, c.cfg.MaxBackoff)
		d = max(d, retryAfter)
	}
	return d
}

func (c *Client) send(ctx context.Context, method, path string, body []byte, key string) (int, time.Duration, []byte, error) {
	var rd io.Reader
	gz := false
	if body != nil {
		if c.cfg.GzipMinBytes > 0 && len(body) >= c.cfg.GzipMinBytes {
			var buf bytes.Buffer
			w := gzip.NewWriter(&buf)
			_, _ = w.Write(body)
			_ = w.Close()
			body, gz = buf.Bytes(), true
		}
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.Endpoint+path, rd)
	if err != nil {
		return 0, 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, nil, &TransportError{err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return 0, 0, nil, &TransportError{err}
	}
	var ra time.Duration
	if s, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
		ra = time.Duration(s) * time.Second
	}
	return resp.StatusCode, ra, b, nil
}

func retryable(err error) bool {
	var se *ServerError
	var te *TransportError
	return (errors.As(err, &se) && se.Retryable) || errors.As(err, &te)
}

func (c *Client) withRetries(ctx context.Context, op string, send func() (int, time.Duration, []byte, error)) ([]byte, int, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, attempt, err
		}
		start := time.Now()
		status, ra, body, err := send()
		if c.cfg.OnAttempt != nil {
			c.cfg.OnAttempt(op, attempt, status, time.Since(start))
		}
		if err == nil && status >= 200 && status < 300 {
			return body, attempt + 1, nil
		}
		if err == nil {
			var v struct {
				Error     string `json:"error"`
				Message   string `json:"message"`
				Retryable *bool  `json:"retryable"`
			}
			_ = json.Unmarshal(body, &v)
			r := status == 429 || status == 502 || status == 503 || status == 504
			if v.Retryable != nil {
				r = *v.Retryable
			}
			if v.Error == "" {
				v.Error = "unknown"
			}
			err = &ServerError{Status: status, Kind: v.Error, Message: v.Message, Retryable: r}
		}
		if ctx.Err() != nil {
			return nil, attempt + 1, ctx.Err()
		}
		if !retryable(err) {
			return nil, attempt + 1, err
		}
		if attempt >= c.cfg.MaxRetries {
			return nil, attempt + 1, &RetriesExhaustedError{Attempts: attempt + 1, Last: err}
		}
		select {
		case <-ctx.Done():
			return nil, attempt + 1, ctx.Err()
		case <-time.After(c.backoff(attempt, ra)):
		}
	}
}

// Ingest commits events atomically with a fresh idempotency key.
func (c *Client) Ingest(ctx context.Context, events []map[string]interface{}) (*IngestReceipt, error) {
	return c.IngestWithKey(ctx, events, c.NewIdempotencyKey())
}

// IngestWithKey commits events with a caller-chosen idempotency key.
func (c *Client) IngestWithKey(ctx context.Context, events []map[string]interface{}, key string) (*IngestReceipt, error) {
	if len(events) == 0 {
		return nil, errors.New("no events")
	}
	body, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}
	raw, attempts, err := c.withRetries(ctx, "ingest", func() (int, time.Duration, []byte, error) {
		return c.send(ctx, http.MethodPost, "/api/v1/ingest/batch", body, key)
	})
	if err != nil {
		return nil, err
	}
	var r IngestReceipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, &TransportError{fmt.Errorf("bad response: %w", err)}
	}
	r.IdempotencyKey, r.Attempts = key, attempts
	return &r, nil
}

// Query runs a StrataQL query (read-only; retried on retryable errors).
func (c *Client) Query(ctx context.Context, sql string) (*QueryResult, error) {
	body, _ := json.Marshal(map[string]string{"query": sql})
	raw, _, err := c.withRetries(ctx, "query", func() (int, time.Duration, []byte, error) {
		return c.send(ctx, http.MethodPost, "/api/v1/query", body, "")
	})
	if err != nil {
		return nil, err
	}
	var q QueryResult
	if err := json.Unmarshal(raw, &q); err != nil {
		return nil, &TransportError{fmt.Errorf("bad response: %w", err)}
	}
	return &q, nil
}

// Health checks liveness.
func (c *Client) Health(ctx context.Context) error {
	status, _, body, err := c.send(ctx, http.MethodGet, "/api/v1/health", nil, "")
	if err != nil {
		return err
	}
	if status != 200 {
		return &ServerError{Status: status, Kind: "unknown", Message: string(body), Retryable: true}
	}
	return nil
}
