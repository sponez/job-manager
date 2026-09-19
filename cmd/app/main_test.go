package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"
)

// This is an integration test of application wiring: the handler, service and
// in-memory repository are all real. Requests still run without a TCP server.
func TestNewHandlerJobLifecycle(t *testing.T) {
	h := newHandler()
	request := func(method, path, body string, wantStatus int) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		if resp.Code != wantStatus {
			t.Fatalf("%s %s: status = %d, want %d; body = %s", method, path, resp.Code, wantStatus, resp.Body.String())
		}
		return resp
	}
	type jobResponse struct {
		ID     string `json:"id"`
		Kind   string `json:"kind"`
		Status string `json:"status"`
	}
	created := request(http.MethodPost, "/jobs", `{"kind":"Send email"}`, http.StatusCreated)
	var j jobResponse
	if err := json.Unmarshal(created.Body.Bytes(), &j); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	if _, err := uuid.Parse(j.ID); err != nil {
		t.Fatalf("created job ID = %q, want UUID: %v", j.ID, err)
	}
	if j.Kind != "Send email" || j.Status != "pending" {
		t.Fatalf("created job = %+v, want pending email", j)
	}
	path := "/jobs/" + j.ID
	if location := created.Header().Get("Location"); location != path {
		t.Errorf("Location = %q, want %q", location, path)
	}

	read := request(http.MethodGet, path, "", http.StatusOK)
	var stored jobResponse
	if err := json.Unmarshal(read.Body.Bytes(), &stored); err != nil {
		t.Fatalf("decode stored job: %v", err)
	}
	if stored != j {
		t.Errorf("stored job = %+v, want %+v", stored, j)
	}

	completed := request(http.MethodPost, path+"/complete", "", http.StatusNoContent)
	if completed.Body.Len() != 0 {
		t.Errorf("complete response body = %q, want empty", completed.Body.String())
	}
	read = request(http.MethodGet, path, "", http.StatusOK)
	if err := json.Unmarshal(read.Body.Bytes(), &stored); err != nil {
		t.Fatalf("decode completed job: %v", err)
	}
	j.Status = "done"
	if stored != j {
		t.Errorf("completed job = %+v, want %+v", stored, j)
	}

	listed := request(http.MethodGet, "/jobs", "", http.StatusOK)
	var list struct {
		Jobs     []jobResponse `json:"jobs"`
		Partial  bool          `json:"partial"`
		Warnings []string      `json:"warnings"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode job list: %v", err)
	}
	if len(list.Jobs) != 1 || list.Jobs[0] != j || list.Partial || len(list.Warnings) != 0 {
		t.Errorf("unexpected job list: %+v", list)
	}
}

func TestRunListenError(t *testing.T) {
	// An invalid address exercises startup failure without sending OS signals
	// or calling main(), which could terminate the test process with os.Exit.
	t.Setenv("HTTP_ADDR", "127.0.0.1:invalid:address")
	err := run()
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("run() error = %v, want wrapped network error", err)
	}
	if opErr.Op != "listen" {
		t.Errorf("network operation = %q, want listen", opErr.Op)
	}
}

func TestServeListenerError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("create listener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := &http.Server{Handler: http.NewServeMux()}
	if err := serve(ctx, server, listener); !errors.Is(err, net.ErrClosed) {
		t.Errorf("serve() error = %v, want wrapped net.ErrClosed", err)
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	// Port 0 asks the OS for an available port; this test uses real local HTTP.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("create listener: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	requestStarted := make(chan struct{})
	allowResponse := make(chan struct{})
	shutdownStarted := make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(allowResponse) }) }
	t.Cleanup(releaseRequest)

	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-allowResponse:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
			// If shutdown aborts the request, the client will not get "finished".
			return
		}
	})}
	server.RegisterOnShutdown(func() { close(shutdownStarted) })
	t.Cleanup(func() { server.Close() })
	serveDone := make(chan error, 1)
	go func() { serveDone <- serve(ctx, server, listener) }()

	// A dedicated transport avoids environment proxy settings and shared state.
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	requestDone := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + listener.Addr().String())
		if err != nil {
			requestDone <- err
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			requestDone <- err
			return
		}
		if resp.StatusCode != http.StatusOK || string(body) != "finished" {
			requestDone <- fmt.Errorf("response = %d %q, want 200 finished", resp.StatusCode, body)
			return
		}
		requestDone <- nil
	}()

	// Channels synchronize events; timeouts only prevent a broken test hanging.
	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the server")
	}
	cancel()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not begin shutdown")
	}
	select {
	case err := <-serveDone:
		t.Fatalf("serve returned before the active request finished: %v", err)
	default:
	}

	releaseRequest()
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("active request was interrupted: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("active request did not finish")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish shutdown")
	}
}
