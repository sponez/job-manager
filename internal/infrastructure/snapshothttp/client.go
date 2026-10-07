package snapshothttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/sponez/job-manager/internal/application/snapshot"
)

const (
	maxBodyBytes   = 5 << 20
	requestTimeout = 20 * time.Second
	maxAttempts    = 3
)

var ErrTooLarge = errors.New("snapshot exceeds 5 MiB")

type Client struct {
	http *http.Client
}

var _ snapshot.Fetcher = (*Client)(nil)

// New builds a snapshot client with retry and request logging middleware.
// A nil transport uses the default HTTP transport. The transport is not owned
// by this client and can be shared with other clients.
func New(transport http.RoundTripper, logger *slog.Logger) *Client {
	if transport == nil {
		transport = http.DefaultTransport
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{http: &http.Client{
		Transport: retryTransport{next: loggingTransport{next: transport, logger: logger}},
		Timeout:   requestTimeout,
	}}
}

func (c *Client) Fetch(ctx context.Context, rawURL string) (snapshot.Snapshot, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return snapshot.Snapshot{}, errors.New("snapshot URL must be an absolute HTTP or HTTPS URL without credentials")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return snapshot.Snapshot{}, fmt.Errorf("create snapshot request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return snapshot.Snapshot{}, ctx.Err()
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return snapshot.Snapshot{}, fmt.Errorf("fetch snapshot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return snapshot.Snapshot{}, fmt.Errorf("fetch snapshot: HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBodyBytes {
		return snapshot.Snapshot{}, ErrTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return snapshot.Snapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	if len(body) > maxBodyBytes {
		return snapshot.Snapshot{}, ErrTooLarge
	}
	return snapshot.Snapshot{SourceURL: resp.Request.URL.String(), ContentType: resp.Header.Get("Content-Type"), Body: body}, nil
}

type retryTransport struct{ next http.RoundTripper }

func (t retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptReq := req.WithContext(context.WithValue(req.Context(), attemptKey{}, attempt))
		resp, err := t.next.RoundTrip(attemptReq)
		if !retryable(req, resp, err) || attempt == maxAttempts {
			return resp, err
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		wait := time.NewTimer(time.Duration(1<<(attempt-1)) * 200 * time.Millisecond)
		select {
		case <-req.Context().Done():
			wait.Stop()
			return nil, req.Context().Err()
		case <-wait.C:
		}
	}
	panic("unreachable")
}

func retryable(req *http.Request, resp *http.Response, err error) bool {
	if req.Method != http.MethodGet || req.Context().Err() != nil {
		return false
	}
	if err != nil {
		return true
	}
	return resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError)
}

type attemptKey struct{}

type loggingTransport struct {
	next   http.RoundTripper
	logger *slog.Logger
}

func (t loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	level := slog.LevelInfo
	if err != nil || status >= http.StatusBadRequest {
		level = slog.LevelWarn
	}
	t.logger.LogAttrs(req.Context(), level, "snapshot HTTP attempt",
		slog.String("host", req.URL.Host),
		slog.Int("attempt", req.Context().Value(attemptKey{}).(int)),
		slog.Int("status", status),
		slog.Duration("duration", time.Since(start)),
		slog.Any("error", err),
	)
	return resp, err
}
