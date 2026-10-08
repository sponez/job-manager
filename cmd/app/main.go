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
	"github.com/sponez/job-manager/config"
	"github.com/sponez/job-manager/internal/application/worker"
	"github.com/sponez/job-manager/internal/application/workflow"
	"github.com/sponez/job-manager/internal/infrastructure/apiserver"
	"github.com/sponez/job-manager/internal/infrastructure/handler"
	"github.com/sponez/job-manager/internal/infrastructure/repository/postgres"
	"github.com/sponez/job-manager/internal/infrastructure/snapshothttp"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if err := config.LoadEnv(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadApp(ctx)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	dbPool, err := createDB(startupCtx, cfg.Database, cfg.Pool)
	cancel()
	if err != nil {
		return fmt.Errorf("initialize postgres: %w", err)
	}
	defer dbPool.Close()

	snapshotRepository := postgres.NewSnapshotRepository(dbPool)
	service, err := workflow.NewService(
		postgres.NewWorkflowRepository(dbPool), slog.Default(),
		workflow.NewPageDefinition(
			snapshothttp.New(nil, slog.Default()),
			snapshotRepository,
		),
	)
	if err != nil {
		return fmt.Errorf("create workflow service: %w", err)
	}
	pool, err := worker.New(4, 16)
	if err != nil {
		return fmt.Errorf("create worker pool: %w", err)
	}
	if err := pool.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("start worker pool: %w", err)
	}
	defer pool.Shutdown()

	h := newHandler(handler.NewSnapshotHandler(service, snapshotRepository))
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	schedulerCtx, stopScheduler := context.WithCancel(ctx)
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		service.Run(schedulerCtx, pool)
	}()
	defer func() {
		stopScheduler()
		<-schedulerDone
	}()
	slog.Info("server listening", "address", listener.Addr().String())
	return serve(ctx, server, listener)
}

func newHandler(handlers ...apiserver.Handler) http.Handler {
	mux := http.NewServeMux()

	apiConfig := huma.DefaultConfig("Job Manager API", "1.0.0")
	apiConfig.DocsRenderer = huma.DocsRendererSwaggerUI

	api := humago.New(mux, apiConfig)
	server := apiserver.New(handlers)

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
