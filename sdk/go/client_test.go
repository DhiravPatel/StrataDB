package stratadb

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func fast(url string) Config {
	return Config{Endpoint: url, Token: "t", BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}
}

func TestRetriesReuseIdempotencyKey(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		n++
		if n < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"error":"unavailable","message":"busy","retryable":true}`))
			return
		}
		_, _ = w.Write([]byte(`{"accepted":1,"rejected":0,"errors":[],"commit_seq":9,"durability":"wal_synced","duplicate":false}`))
	}))
	defer srv.Close()
	c, err := New(fast(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.Ingest(context.Background(), []map[string]interface{}{{"body": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Attempts != 3 || r.CommitSeq != 9 {
		t.Fatalf("receipt %+v", r)
	}
	if len(keys) != 3 || keys[0] == "" || keys[0] != keys[1] || keys[1] != keys[2] {
		t.Fatalf("keys not reused: %v", keys)
	}
}

func TestNonRetryableAndBoundedAndCancel(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.Contains(r.URL.Path, "query") {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":"invalid_input","message":"bad","retryable":false}`))
			return
		}
		w.WriteHeader(503)
	}))
	defer srv.Close()
	c, _ := New(fast(srv.URL))
	_, err := c.Query(context.Background(), "x")
	var se *ServerError
	if !errors.As(err, &se) || se.Status != 400 || calls != 1 {
		t.Fatalf("want one non-retried 400, got %v after %d calls", err, calls)
	}
	cfg := fast(srv.URL)
	cfg.MaxRetries = 2
	c, _ = New(cfg)
	_, err = c.Ingest(context.Background(), []map[string]interface{}{{"body": "x"}})
	var re *RetriesExhaustedError
	if !errors.As(err, &re) || re.Attempts != 3 {
		t.Fatalf("want 3 attempts, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Ingest(ctx, []map[string]interface{}{{"body": "x"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if strings.Contains(c.String(), "t") && strings.Contains(c.String(), "Bearer") {
		t.Fatal("token leaked in String()")
	}
}

// Against a real server: run via apps/stratadb-server/tests/go_sdk.rs.
func TestConformanceAgainstServer(t *testing.T) {
	url, token := os.Getenv("STRATADB_URL"), os.Getenv("STRATADB_TOKEN")
	if url == "" || token == "" {
		t.Skip("STRATADB_URL/STRATADB_TOKEN not set")
	}
	ctx := context.Background()
	c, err := New(Config{Endpoint: url, Token: token})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
	ev := []map[string]interface{}{{"service": "go", "severity": "ERROR", "body": "boom"}, {"service": "go"}, {"ingest_time": 1}}
	r, err := c.Ingest(ctx, ev)
	if err != nil || r.Accepted != 2 || r.Rejected != 1 || r.Errors[0].Index != 2 || r.Durability != "wal_synced" {
		t.Fatalf("ingest %+v %v", r, err)
	}
	again, err := c.IngestWithKey(ctx, ev, r.IdempotencyKey)
	if err != nil || !again.Duplicate || again.CommitSeq != r.CommitSeq {
		t.Fatalf("dedup %+v %v", again, err)
	}
	big := make([]map[string]interface{}, 300)
	for i := range big {
		big[i] = map[string]interface{}{"service": "go", "body": strings.Repeat("x", 100)}
	}
	if r, err := c.Ingest(ctx, big); err != nil || r.Accepted != 300 {
		t.Fatalf("gzip ingest %+v %v", r, err)
	}
	q, err := c.Query(ctx, "SELECT count(*) AS n FROM logs WHERE service = 'go'")
	if err != nil || len(q.Rows) != 1 || q.Rows[0]["n"].(float64) != 302 || q.QueryID == "" {
		t.Fatalf("query %+v %v", q, err)
	}
	_, err = c.Query(ctx, "SELECT * FROM nowhere")
	var se *ServerError
	if !errors.As(err, &se) || se.Kind != "invalid_input" || se.Retryable {
		t.Fatalf("want invalid_input, got %v", err)
	}
	bad, _ := New(Config{Endpoint: url, Token: "sdb_x_y"})
	if _, err := bad.Query(ctx, "SELECT 1 FROM logs"); !errors.As(err, &se) || se.Status != 401 {
		t.Fatalf("want 401, got %v", err)
	}
}
