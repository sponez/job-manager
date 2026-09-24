package main

import (
	"context"
	"fmt"

	"github.com/sponez/job-manager/config"
	"github.com/sponez/job-manager/internal/application/job"
	"github.com/sponez/job-manager/internal/infrastructure/repository/memory"
	"github.com/sponez/job-manager/internal/infrastructure/repository/postgres"
)

// createJobRepository returns the selected repository and its cleanup function.
func createJobRepository(ctx context.Context, cfg config.AppConfig) (job.JobRepository, func(), error) {
	if !cfg.UsePostgres {
		return memory.New(), func() {}, nil
	}
	pool, err := createDB(ctx, cfg.Database, cfg.Pool)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize postgres: %w", err)
	}
	return postgres.NewJobRepository(pool), pool.Close, nil
}
