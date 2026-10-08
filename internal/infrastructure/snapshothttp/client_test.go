package snapshothttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestFetchRetriesTemporaryResponsesAndLogsAttempts(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<h1>snapshot</h1>")
	}))
	defer server.Close()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	client := New(&http.Transport{Proxy: nil}, logger)
	got, err := client.Fetch(context.Background(), server.URL+"/page?token=secret-marker")
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || string(got.Body) != "<h1>snapshot</h1>" || got.ContentType != "text/html" {
		t.Fatalf("calls = %d, snapshot = %+v", calls.Load(), got)
	}
	if !strings.Contains(logs.String(), `"attempt":1`) || !strings.Contains(logs.String(), `"attempt":2`) || strings.Contains(logs.String(), "secret-marker") {
		t.Fatalf("attempts were not logged safely: %s", logs.String())
	}
}

func TestFetchDoesNotRetryPermanentFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := New(nil, nil).Fetch(context.Background(), server.URL)
	if err == nil || !strings.Contains(err.Error(), "HTTP 404") || calls.Load() != 1 {
		t.Fatalf("error = %v, calls = %d", err, calls.Load())
	}
}

func TestFetchEnforcesBodyLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), maxBodyBytes+1))
	}))
	defer server.Close()

	_, err := New(nil, nil).Fetch(context.Background(), server.URL)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("error = %v, want ErrTooLarge", err)
	}
}
