package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunInvalidHTTPAddress(t *testing.T) {
	// Invalid HTTP configuration must fail before contacting Vault or PostgreSQL.
	t.Chdir(t.TempDir())
	t.Setenv("HTTP_ADDR", "127.0.0.1:invalid:address")
	err := run()
	if err == nil || !strings.Contains(err.Error(), "HTTP_ADDR") {
		t.Fatalf("run() error = %v, want HTTP_ADDR validation error", err)
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
