package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/infrastructure/apiserver"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
	"github.com/sponez/job-manager/internal/infrastructure/repository/memory"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           newHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	slog.Info("server listening", "address", listener.Addr().String())
	return serve(ctx, server, listener)
}

func newHandler() http.Handler {
	mux := http.NewServeMux()

	apiConfig := huma.DefaultConfig("Job Manager API", "1.0.0")
	apiConfig.DocsRenderer = huma.DocsRendererSwaggerUI

	api := humago.New(mux, apiConfig)
	server := apiserver.New(handlers())

	server.Register(api)
	return mux
}

func serve(ctx context.Context, server *http.Server, listener net.Listener) error {
	defer server.Close()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		slog.Info("shutting down server")
	}

	// Request contexts stay alive while in-flight requests finish.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}

func handlers() []apiserver.Handler {
	jobHandler := createJobHandler()

	return []apiserver.Handler{
		jobHandler,
	}
}

func createJobHandler() *handler.JobHandler {
	jobRepository := memory.New()
	jobService := job.New(jobRepository)
	jobHandler := handler.NewJobHandler(jobService)

	return jobHandler
}
